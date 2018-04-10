#!/usr/bin/env python
##
# A python application to show the benefits of workload isolation
#
# Thank you to Paul Done. Large portions of this script are stolen from
# mongo_stock_ticker.py available at
# https://github.com/pkdone/PyMongoStockTicker

import sys
from pymongo import MongoClient

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
    if collection().find_one() is not None:
        print('-- Initialization of collection "%s.%s" not performed because\n'
              '   collection already exists (run with command "CLEAN" first)\n'
              % (DB, COLL))
        return
    
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

####
# Generate an analytic workload
# This will briefly generate a large number of reads
def do_analyze(mongoURL):
    mongo_client = MongoClient(mongoURL)

####
# Get a handle on database.collection
####
def collection():
    return mongo_client[DB][COLL]

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
