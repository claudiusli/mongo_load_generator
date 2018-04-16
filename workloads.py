#!/usr/bin/env python
##
# A python application to show the benefits of workload isolation
#
# Thank you to Paul Done. Large portions of this script are stolen from
# mongo_stock_ticker.py available at
# https://github.com/pkdone/PyMongoStockTicker

import sys
from pymongo import MongoClient
import threading
import random
from datetime import datetime
from pprint import pprint
####
# Main start function
####
def main():
    print(' ')

    if len(sys.argv) < 3:
        print('Error: Insufficient arguments provided')
        print_usage()
    else:
        command = sys.argv[1].strip().upper()
        mongoURL = sys.argv[2].strip()
        COMMANDS.get(command, print_commands_error)(mongoURL)

####
# Initialize the database with records
####
def do_init(mongoURL):
    mongo_client = MongoClient(mongoURL)
    if mongo_client[DB][COLL].find_one() is not None:
        print('-- Initialization of collection "%s.%s" not performed because\n'
              '   collection already exists (run with command "CLEAN" first)\n'
              % (DB, COLL))
        return
    for threadcount in xrange(10):
        t = threading.Thread(target=insert_new_docs, args=(mongo_client,))
        t.start()

####
# Generate docs docs and insert them into the database
####
def insert_new_docs(mongo_client):
    MAXBLOCKS = 100
    BLOCKSIZE = 1000
    for i in xrange(MAXBLOCKS):
        docs = []
        for j in xrange(BLOCKSIZE):
            docs.append(
                {
                    "accountNumber": random.randint(1,1000),
                    "singupDate": datetime.utcnow(),
                    "payment": random.randrange(50,200,5),
                    "copay": random.randrange(20,60,10),
                    "deductible": random.randrange(100,500,100)
                    }
                )
        mongo_client[DB][COLL].insert_many(docs)

####
# Remove the database collection and its documents
####
def do_clean(mongoURL):
    mongo_client = MongoClient(mongoURL)
    print('-- Dropping collection "%s.%s" and all its documents\n'
          % (DB, COLL))
    mongo_client.drop_database(DB)

####
# Generate an operational workload
# This will run until terminated and provides a baseline steady workload
####
def do_operate(mongoURL):
    mongo_client = MongoClient(mongoURL)
    while(True):
        mongo_client[DB][COLL].update(
            {
                "accountNumber": random.randint(1,1000)
            },
            {
                "$set":
                {
                    "singupDate": datetime.utcnow(),
                    "payment": random.randrange(50,200,5),
                    "copay": random.randrange(20,60,10),
                    "deductible": random.randrange(100,500,100)
                }
            },
            upsert=True
        )

####
# Generate an analytic workload
# This will briefly generate a large number of reads
def do_analyze(mongoURL):
    mongo_client = MongoClient(mongoURL)
    for threadcount in xrange(10):
        t = threading.Thread(target=read_all_docs, args=(mongo_client,))
        t.start()

def read_all_docs(mongo_client):
    for iteration in xrange(10):
        docs = mongo_client[DB][COLL].find()
        for doc in docs:
            pprint(doc['accountNumber'])

####
# Print out how to use this script
####
def print_usage():
    print('\nUsage:')
    print('\n $ ./workloads.py <COMMAND> <CONNECT STRING>')
    print('\n Command options:')

    for key in COMMANDS.keys():
        print(' * %s' %key)

    print(' ')

####
# Print script start-up error reason
####
def print_commands_error(command):
    print('Error: Illegal command argument provided: "%s"' % command)
    print_usage()

####
# Swallow the verbiage that is spit out when using 'Ctrl-C' to kill the script
# and instead jsut print a simple single line message
####
def keyboard_shutdown():
    print('Interrupted\n')

    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)

# Constants
DB = 'workloads'
COLL = 'policies'
COMMANDS = {
    'INIT':    do_init,
    'OPERATE': do_operate,
    'ANALYZE': do_analyze,
    'CLEAN': do_clean
}

####
#Main
####
if __name__ == '__main__':
    main()
