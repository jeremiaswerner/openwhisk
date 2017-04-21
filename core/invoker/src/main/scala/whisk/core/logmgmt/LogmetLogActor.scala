package whisk.core.logmgmt;

import java.io.ByteArrayOutputStream
import java.net.InetSocketAddress
import java.security.cert.X509Certificate
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions.mapAsScalaConcurrentMap
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

import com.typesafe.sslconfig.akka.AkkaSSLConfig
import com.typesafe.sslconfig.ssl.ClientAuth

import akka.NotUsed
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.ReceiveTimeout
import akka.actor.Status.Failure
import akka.actor.actorRef2Scala
import akka.event.LoggingReceive
import akka.stream.ActorMaterializer
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

    def add(dataMessage: DataMessage, maximumBufferSize: Integer=100) = {
       if (synchronizedMessageMap.keySet.size > maximumBufferSize) {
         println(s"ignore message with sequence ${dataMessage.sequence}")
       } else {
         synchronizedMessageMap.put(dataMessage.sequence, dataMessage)
       }
    }

    def remove(sequence: Integer) = {
       synchronizedMessageMap.remove(sequence)
       println(s"buffer contains ${synchronizedMessageMap.keySet.size} unacknowledged messages")
    }

    def nextSequence() = sequence.getAndIncrement

    def currentSequence() = sequence.get()

    def toSortedSeq() = synchronizedMessageMap.toSeq.sortBy(_._1)

    def get(seq: Integer) = synchronizedMessageMap.get(seq)
}

object MessageTransmitter {

    def send(buffer: MessageBuffer, sequence: Integer, actor: ActorRef ): Unit = {
      actor ! WindowMessage(1)
      actor ! buffer.get(sequence)
    }

    def sendUntil(buffer: MessageBuffer, sequence: Integer, actor: ActorRef ): Unit = {
      buffer.toSortedSeq().foreach { case (s, m) if (s <= sequence) => send(buffer, s, actor) }
    }

    // re-send all data messages to the actor with sequence number lower than 'sequence'
    def resendUntil(buffer: MessageBuffer, sequence: Integer, actor: ActorRef, duration: FiniteDuration = 5.seconds)(implicit system: ActorSystem, execution: ExecutionContext) : Unit = {
      // data messages needs to be send in order of the sequence, otherwise will only retrieve an ack for the highest sequence
      //@todo: leverage window size here to send multiple messages at once
      sendUntil(buffer, sequence, actor)
      system.scheduler.scheduleOnce(duration) {
        // schedule with current sequence number to avoid sending new arrivals
        resendUntil(buffer, buffer.currentSequence(), actor)
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

      val inputLogger = logger("> ")
      val outputLogger = logger("< ")

      // create BidiFlow with a separate logger function for each of both streams
      BidiFlow.fromFunctions(outputLogger, inputLogger)
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
    import context.dispatcher

    val ident = IdentMessage(clientId)
    val auth = AuthMessage(tenantId, token)

    val buffer = new MessageBuffer()

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
    val connection = Tcp().outgoingConnection(address.getHostName, address.getPort)
    val sink = Sink.actorRef(self, {println("initialized")})
    val source = Source.actorRef(10000, OverflowStrategy.dropNew)
    val tlsConnectionFlow = MessageEncryptor.tlsStage(TLSRole.client).reversed
    val logFlow = if (debug) MessageLogger.loggingStage else BidiFlow.identity[Message, Message]
    val sourceActor = connection.join(tlsConnectionFlow).join(MessageSerializer.serializationStage).join(logFlow).to(sink).runWith(source)

    authenticate()

    // workaround to keep connection alive, Sink terminates after 120s
    system.scheduler.schedule(60.seconds, 60.seconds) {
      sourceActor ! auth
    }

    def convertToDataMessage(activation: WhiskActivation) = {
      val keys = Map() ++
        Map("activationid" -> activation.activationId.asString) ++
        Map("name" -> activation.name.toString()) ++
        Map("start" -> activation.start.toString()) ++
        Map("end" -> activation.end.toString()) ++
        Map("duration" -> activation.duration.map(_.toString()).toString()) ++
        Map("subject" -> activation.subject.toString())
      val dataMessages = activation.logs.logs.map { log => DataMessage(tenantId, buffer.nextSequence(), log, keys) }
      dataMessages.toSeq
    }

    // receive when authenticated
    def receive = LoggingReceive {
      case activation: WhiskActivation =>
        convertToDataMessage(activation).foreach { dataMessage =>
          buffer.add(dataMessage)
          MessageTransmitter.send(buffer, dataMessage.sequence, sourceActor)
        }
      case AcknowledgeMessage(sequence) if sequence == 0  =>
        // still authenticated. ignore.
      case AcknowledgeMessage(sequence) if sequence > 0  =>
        buffer.remove(sequence)
      case UnauthorizedMessage =>
        authenticate()
      case _ =>
        println("got something unkown ")
    }

    def receiveWhenNotAuthenticated : PartialFunction[Any, Unit] = LoggingReceive {
      case activation: WhiskActivation =>
        convertToDataMessage(activation).foreach { dataMessage =>
          buffer.add(dataMessage)
        }
      case AcknowledgeMessage(sequence) if sequence == 0 =>
        println("supi, I'am authorized")
        context become receive
        MessageTransmitter.resendUntil(buffer, buffer.currentSequence(), sourceActor)
      case AcknowledgeMessage(sequence) if sequence > 0  =>
        // get an acknowledgement message while waiting for (re)authentication
        buffer.remove(sequence)
      case _ =>
        println("got something unkown ")
    }

    def authenticate() = {
      context become receiveWhenNotAuthenticated
      sourceActor ! ident
      sourceActor ! auth
    }
}
