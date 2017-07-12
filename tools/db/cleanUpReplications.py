#!/usr/bin/env python
"""Python script to replicate and replay databases.

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
"""


import argparse
import time
import re
import couchdb.client

def cleanUpReplications(args):
    """Clean up replication databases and documents"""
    sourceDb = couchdb.client.Server(args.sourceDbUrl)
    replicator = sourceDb["_replicator"]

    def isReplicationDocumentPrefix(doc):
        if args.dbContains in doc.id:
            return True
        return False

    # Create backup of all databases with given prefix
    print("----- Delete databases -----")
    for db in filter(lambda dbName: args.dbContains in dbName, sourceDb):
        if args.printOnly == "true":
            print("found database: %s" % db)
        else:
            print("delete database: %s" % db)
            sourceDb.delete(db)


    # Delete all documents in the _replicator-database of old backups to avoid that they continue after they are deprecated
    print("----- Delete backup-documents matching source or target db with prefix %s -----" % args.dbContains)
    for doc in filter(lambda doc: isReplicationDocumentPrefix(doc), replicator.view('_all_docs', include_docs=True)):
        if args.printOnly == "true":
            print("found backup document: %s" % doc.id)
        else:
            # Get again the latest version of the document to delete the right revision and avoid Conflicts
            print("delete backup document: %s" % doc.id)
            replicator.delete(replicator[doc.id])



parser = argparse.ArgumentParser(description="Utility to delete databases and replication documents that contain a defined string")
parser.add_argument("--sourceDbUrl", required=True, help="Server URL of the source database, that has to be backed up. E.g. 'https://xxx:yyy@domain.couch.com:443'")
parser.add_argument("--dbContains", required=True, help="Any string that could match the database name and replication document of the databases, that should be backed up.")
parser.add_argument("--printOnly", default="true", help="Only list databases and documents to be deleted, default 'true'. 'false' otherwise ")
parser.set_defaults(func=cleanUpReplications)

arguments = parser.parse_args()
arguments.func(arguments)
