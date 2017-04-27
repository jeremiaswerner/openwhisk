package whisk.core.logmgmt;

import java.io.ByteArrayOutputStream
import java.net.InetSocketAddress
import java.security.cert.X509Certificate
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions.mapAsScalaConcurrentMap
import scala.collection.mutable.Queue

import com.typesafe.sslconfig.akka.AkkaSSLConfig
import com.typesafe.sslconfig.ssl.ClientAuth

import akka.NotUsed
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.event.LoggingReceive
import akka.stream.ActorMaterializer
import akka.stream.Attributes
import akka.stream.BidiShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.OverflowStrategy
import akka.stream.TLSClientAuth
import akka.stream.TLSProtocol
import akka.stream.TLSRole
import akka.stream.scaladsl.BidiFlow
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.TLS
import akka.stream.scaladsl.Tcp
import akka.stream.stage.GraphStage
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import akka.stream.stage.TimerGraphStageLogic
import akka.util.ByteString
import javax.net.ssl.KeyManager
import javax.net.ssl.SSLContext
import javax.net.ssl.X509TrustManager
import whisk.core.entity.WhiskActivation

// Message formats sent to Logmet
sealed trait Message
case class UnknownMessage(msg: ByteString) extends Message
case class UnauthorizedMessage() extends Message
case class AcknowledgeMessage(sequence: Integer = 0) extends Message
case class IdentMessage(clientId: String) extends Message
case class AuthMessage(tenant: String, token: String) extends Message
case class WindowMessage(size: Integer) extends Message
case class DataMessage(tenantId: String, sequence: Integer, message: String, data: Map[String, String]) extends Message

// Interface to outside
case class LogEntry(message: String, data: Map[String, String])

class MessageBuffer {
    val sequence = new AtomicInteger(1)
    val synchronizedMessageMap = new ConcurrentHashMap[Integer, DataMessage]()

    def add(dataMessage: DataMessage) = {
        synchronizedMessageMap.put(dataMessage.sequence, dataMessage)
    }

    def remove(sequence: Integer) = {
       synchronizedMessageMap.remove(sequence)
       println(s"removed ${sequence}, buffer contains ${size()} messages")
    }

    def removeWindow(lastSequence: Integer, size: Integer) = {
       val firstSequence = lastSequence - (size)  + 1
       for (s <- firstSequence until lastSequence + 1) {
           remove(s)
       }
    }

    def nextSequence() = sequence.getAndIncrement

    def currentSequence() = sequence.get()

    def toSortedSeq() = synchronizedMessageMap.toSeq.sortBy(_._1)
    def size() = synchronizedMessageMap.size()

    def get(seq: Integer) = synchronizedMessageMap.get(seq)
}

// http://blog.kunicki.org/blog/2016/07/20/implementing-a-custom-akka-streams-graph-stage/
final class MessageBufferWithBackpressure(ident: IdentMessage, auth: AuthMessage)
  extends GraphStage[BidiShape[Message, Message, Message, Message]] {

  val inbound_in = Inlet[Message]("MessageBufferWithBackpressure.inbound_in")
  val inbound_out = Outlet[Message]("MessageBufferWithBackpressure.inbound_out")
  val outbound_in = Inlet[Message]("MessageBufferWithBackpressure.outbound_in")
  val outbound_out = Outlet[Message]("MessageBufferWithBackpressure.outbound_out")

  override def shape = BidiShape.of(inbound_in, inbound_out, outbound_in, outbound_out)

  override def createLogic(inheritedAttributes: Attributes) = new TimerGraphStageLogic(shape) {

    private val maxWindowSize = 10
    private val maxBufferSize = 100

    private var isAuthorized = false
    private var downstreamWaiting = false
    private var currentWindowSize = maxWindowSize

    private val messageBuffer = Queue[DataMessage]()
    def messageBufferFull = messageBuffer.length >= maxWindowSize

    private val waitAckBuffer = new MessageBuffer()
    def waitAckBufferFull = waitAckBuffer.size() >= 100

    def checkWindowAndSend() = {
        if (currentWindowSize == maxWindowSize) {
            push(outbound_out, WindowMessage(maxWindowSize))
            currentWindowSize = 0
        } else  {
            val elem = messageBuffer.dequeue()
            push(outbound_out, elem)
            waitAckBuffer.add(elem)
            currentWindowSize += 1
        }
    }

    // inbound, i.e. from logmet
    setHandlers(inbound_in, inbound_out, new InHandler with OutHandler {
      override def onPush(): Unit = {
        val nextMessage = grab(inbound_in)
        isAuthorized = nextMessage match {
            case AcknowledgeMessage(sequence) if sequence > 0  =>
                // free ack. sequence
                waitAckBuffer.removeWindow(sequence, maxWindowSize)
                true
            case AcknowledgeMessage(sequence) if sequence == 0 =>
                true
            case UnauthorizedMessage() =>
                false
            case UnknownMessage(error) =>
                println(error) // something went wrong
                false
        }
        push(inbound_out, nextMessage)
      }

      override def onPull(): Unit = {
        pull(inbound_in)
      }

      override def onUpstreamFinish(): Unit = {
        completeStage()
      }
    })

    // outbound, i.e. to logmet
    setHandlers(outbound_in, outbound_out, new InHandler with OutHandler {
      override def onPush(): Unit = {
        // get a datamessage message and send if authorized.
        // otherwise just buffer
        val nextMessage = grab(outbound_in)
        nextMessage match {
            case d: DataMessage =>
                val msg = DataMessage(d.tenantId, waitAckBuffer.nextSequence(), d.message, d.data)
                messageBuffer.enqueue(msg)
            case _ => // ignore
        }

        if (downstreamWaiting && isAuthorized) {
            checkWindowAndSend()
            downstreamWaiting = false
        }
      }

      override def onPull(): Unit = {
          // pull as long as capacity available,
          // otherwise wait until upstream sends acknowledgements
          if (messageBuffer.isEmpty) {
              downstreamWaiting = true
          } else {
              checkWindowAndSend()
          }

          if (!messageBufferFull && !hasBeenPulled(outbound_in)) {
            pull(outbound_in)
          }
      }

      override def onUpstreamFinish(): Unit = {
        if (messageBuffer.nonEmpty) {
           // emit the rest
           emit(outbound_out, WindowMessage(messageBuffer.length))
           emitMultiple(outbound_out, messageBuffer.toIterator)
        }
        completeStage()
      }

    })

    override def preStart(): Unit = {
        emit(outbound_out, ident)
        emit(outbound_out, auth)
        // a detached stage needs to start upstream demand
        // itself as it is not triggered by downstream demand
        pull(outbound_in)
    }

  }
}

object MessageLogger {
    def loggingStage: BidiFlow[Message, Message, Message, Message, NotUsed] = {
      // function that takes a string, prints it with some fixed prefix in front and returns the string again
      def logger(prefix: String) = (msg: Message) => {
        println(prefix + msg.toString())
        msg
      }

      val inboundLogger = logger("< ")
      val outboundLogger = logger("> ")

      // create BidiFlow with a separate logger function for each of both streams
      BidiFlow.fromFunctions(inboundLogger, outboundLogger)
    }
}

object MessageSerializer {

    // convert the integer in a fix length of 32-bit (4 byte) by filling with '0' from left
    // 0000 0000 0000 0000
    def toBytes32(i : Integer) : Array[Byte] = {
      val result = new Array[Byte](4)
      result(0) = (i >> 24).toByte
      result(1) = (i >> 16).toByte
      result(2) = (i >> 8).toByte
      result(3) = (i).toByte
      result
    }

    // convert fix legnth 32-bit to an integer in BIG_ENDIAN order
    // xxxx xxxx xxxx xxxx
    def fromBytes32(bytes : Array[Byte]) : Integer = {
      val i = java.nio.ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.BIG_ENDIAN).getInt();
      i
    }

    def serializationStage: BidiFlow[ByteString, Message, Message, ByteString, NotUsed] = {
      // expecting either
      // 0A -> unauthorized due to invalid tenantid and token
      // 1A0000 -> authorized
      // 1Axxxx -> authorized and acknowledgement of sequence xxxx (32bit integer)
      // everything else is parsed as an unknown message
      val read = Flow[ByteString]
        // chunk could contain multiple frames, "1A<seq>1A<seq>1A<seq>", where length(seq) eq 4 bytes
        // split chunk to get frames of of kind "1A<seq>",
        .mapConcat(chunk => {
          chunk.grouped(6).toList
        })
        // convert from ByteString to Message
        .map( message => {
          val buffer = message.toArray
          val version = buffer(0)
          val typee = buffer(1)
          if (typee != 65.toByte) {
            // unknown ack message
            UnknownMessage(message)
          } else if (version == 48.toByte) {
            // tenantId/token not valid
            UnauthorizedMessage()
          } else if (version == 49.toByte) {
            // everything fine. authorized.
            //@todo: add parsing of sequence
            val sequence = fromBytes32(message.takeRight(4).toArray)
            AcknowledgeMessage(sequence)
          } else {
            UnknownMessage(message)
          }
        })

      val write = Flow[Message]
        // convert from Message to ByteString
        .map(message => {
           val byteStream = new ByteArrayOutputStream()
           message match {
             // identify client for debug purposes
             // length as 8-bit (1 byte) integer
             // frame: "1I<length of clientId><clientId>"
             case IdentMessage(clientId) => {
               byteStream.write("1I".getBytes())
               byteStream.write(clientId.length())
               byteStream.write(clientId.getBytes())
               ByteString.fromArray(byteStream.toByteArray())
             }
             // authentication with single tenant (2T)
             // length as 8-bit (1 byte) integer
             // frame: "2T<length of tenantId><tenantId><length of token><token>"
             case AuthMessage(tenant, token) => {
               byteStream.write("2T".getBytes())
               byteStream.write(tenant.length())
               byteStream.write(tenant.getBytes())
               byteStream.write(token.length())
               byteStream.write(token.getBytes())
               ByteString.fromArray(byteStream.toByteArray())
             }
             // window size, i.e. the number of messages sent next
             // length as 32-bit (4 byte) integer
             // frame: "1W<window size>"
             case WindowMessage(size) => {
               byteStream.write("1W".getBytes)
               byteStream.write(toBytes32(size))
               ByteString.fromArray(byteStream.toByteArray())
             }
             // data message of key/value pairs
             // integers in 32-bit (4 byte) BIG-ENDIAN
             // frame: "1D<sequence><number of keys><length of key1><key1><length of value1><value1>..."
             // note: ALCH_TENANT_ID is required as the target space id, all other are optional
             case DataMessage(tenantId, sequence, message, data) => {
               val numKeys = toBytes32(data.size + 2)
               byteStream.write("1D".getBytes)
               byteStream.write(toBytes32(sequence))
               byteStream.write(numKeys)
               byteStream.write(toBytes32("ALCH_TENANT_ID".length()))
               byteStream.write("ALCH_TENANT_ID".getBytes)
               byteStream.write(toBytes32(tenantId.length()))
               byteStream.write(tenantId.getBytes)
               byteStream.write(toBytes32("message".length()))
               byteStream.write("message".getBytes)
               byteStream.write(toBytes32(message.length()))
               byteStream.write(message.getBytes)
               data.foreach({ p =>
                 byteStream.write(toBytes32(p._1.length()))
                 byteStream.write(p._1.getBytes)
                 byteStream.write(toBytes32(p._2.length()))
                 byteStream.write(p._2.getBytes)
              })
              ByteString.fromArray(byteStream.toByteArray())
             }
             case _ => ???
           }
        })

      // create BidiFlow with these to separated flows
      BidiFlow.fromFlows(read, write)
    }
}

object MessageEncryptor {
    def tlsStage(role: TLSRole)(implicit system: ActorSystem) = {
      val sslConfig = AkkaSSLConfig.get(system)
      val config = sslConfig.config

      // create a ssl-context that ignores self-signed certificates
      implicit val sslContext: SSLContext = {
          object WideOpenX509TrustManager extends X509TrustManager {
              override def checkClientTrusted(chain: Array[X509Certificate], authType: String) = ()
              override def checkServerTrusted(chain: Array[X509Certificate], authType: String) = ()
              override def getAcceptedIssuers = Array[X509Certificate]()
          }

          val context = SSLContext.getInstance("TLS")
          context.init(Array[KeyManager](), Array(WideOpenX509TrustManager), null)
          context
      }
          // protocols
      val defaultParams = sslContext.getDefaultSSLParameters()
      val defaultProtocols = defaultParams.getProtocols()
      val protocols = sslConfig.configureProtocols(defaultProtocols, config)
      defaultParams.setProtocols(protocols)

      // ciphers
      val defaultCiphers = defaultParams.getCipherSuites()
      val cipherSuites = sslConfig.configureCipherSuites(defaultCiphers, config)
      defaultParams.setCipherSuites(cipherSuites)

      val firstSession = new TLSProtocol.NegotiateNewSession(None, None, None, None)
          .withCipherSuites(cipherSuites: _*)
        .withProtocols(protocols: _*)
        .withParameters(defaultParams)

      val clientAuth = getClientAuth(config.sslParametersConfig.clientAuth)
      clientAuth map { firstSession.withClientAuth(_) }

      val tls = TLS.apply(sslContext, firstSession, role)

      val pf: PartialFunction[TLSProtocol.SslTlsInbound, ByteString] = {
        case TLSProtocol.SessionBytes(_, sb) => ByteString.fromByteBuffer(sb.asByteBuffer)
      }

      val tlsSupport = BidiFlow.fromFlows(
          Flow[ByteString].map(TLSProtocol.SendBytes),
          Flow[TLSProtocol.SslTlsInbound].collect(pf));

      tlsSupport.atop(tls);
    }

    def getClientAuth(auth: ClientAuth) = {
      if (auth.equals(ClientAuth.want)) {
        Some(TLSClientAuth.want)
      } else if (auth.equals(ClientAuth.need)) {
        Some(TLSClientAuth.need)
      } else if (auth.equals(ClientAuth.none)) {
        Some(TLSClientAuth.none)
      } else {
        None
      }
    }
}

class LogmetLogActor(
    address: InetSocketAddress,
    tenantId: String,
    token: String,
    clientId: String,
    maximumBufferSize: Integer = 100,
    debug: Boolean = false)
      extends Actor {

    implicit val materializer = ActorMaterializer()
    import context.system

    val ident = IdentMessage(clientId)
    val auth = AuthMessage(tenantId, token)

    val connection = Tcp().outgoingConnection(address.getHostName, address.getPort)
    val sink = Sink.ignore

    val source = Source.queue[Message](10000, OverflowStrategy.dropNew)
    val tlsConnectionFlow = MessageEncryptor.tlsStage(TLSRole.client).reversed
    val logFlow = if (debug) MessageLogger.loggingStage else BidiFlow.identity[Message, Message]
    val serializationFlow = MessageSerializer.serializationStage

    /**
        +---------------------------+         +----------------------------+         +---------------------------+
        | Flow                      |         | Serialization BidiFlow     |         | tlsConnectionFlow         |
        |                           |         |                            |         |                           |
        | +------+        +------+  |         |  +----------------------+  |         |  +------+        +------+ |
        | | SRC  | ~Out~> |      | ~~>O2   I1~~> |    Message to Byte   | ~~>O1   I1~~> |      |  ~O1~> |      | |
        | |      |        | LOGG |  |         |  +----------------------+  |         |  | TLS  |        | CONN | |
        | | SINK | <~In~  |      | <~~I2   O2<~  |    Byte to Message   | <~~I2   O2<~~ |      | <~I2~  |      | |
        | +------+        +------+  |         |  +----------------------+  |         |  +------+        +------+ |
        +---------------------------+         +----------------------------+         +---------------------------+
    **/
    def sendLogs(activation: WhiskActivation) {

        val count = activation.logs.logs.length
        val bufferFlow = new MessageBufferWithBackpressure(ident, auth)
        val sourceQueue = connection.join(tlsConnectionFlow).join(serializationFlow).join(logFlow).join(bufferFlow).to(sink).runWith(source)

        // authenticate
        //sourceQueue offer ident
        //sourceQueue offer auth

        val keys = Map() ++
            Map("activationid" -> activation.activationId.asString) ++
            Map("name" -> activation.name.toString()) ++
            Map("start" -> activation.start.toString()) ++
            Map("end" -> activation.end.toString()) ++
            Map("duration" -> activation.duration.map(_.toString()).toString()) ++
            Map("subject" -> activation.subject.toString())

        //sourceQueue offer WindowMessage(activation.logs.logs.size)

        activation.logs.logs.map { log =>
            val dataMessage = DataMessage(tenantId, 0, log, keys)
            sourceQueue offer dataMessage
        }
        //sourceQueue.complete()
    }

    // receive when authenticated
    def receive = LoggingReceive {
      case activation: WhiskActivation =>
        sendLogs(activation)
       case _ =>
        println("got something unkown ")
    }
}
