#!/bin/bash

# The command line syntax for running this script is
#      generate-log.sh [Number of iterations] [Output file name]
#
# From Chapter 2, page 21 of the book, we use the following parameters to
# create our first output sample data file to be processed by
# Elastic MapReduce
#
#     chmod +x generate-log.sh
#     generate-log.sh 1000 ./sample-syslog.log


# Create a single formatted line in the output
log_message()
{
        Current_Date=`date +'%b %d %H:%M:%S'`
        Host=`hostname`

        # Output is written to the file name specified as the second
        # entry on the command line
        echo "$Current_Date $Host $0[$$]: $1" >> $2
}

# Generate log events
#
# The number of iterations of 10 entries is specified be the first
# command line entry.
for (( i = 1; i <= $1 ; i++ )) 
do
     log_message "INFO: Login successful for user Alice" $2 
     log_message "INFO: Login successful for user Bob" $2
     log_message "WARNING: Login failed for user Mallory" $2
     log_message "SEVERE: Received SEGFAULT signal from process Eve" $2
     log_message "INFO: Logout occurred for user Alice" $2
     log_message "INFO: User Walter accessed file /var/log/messages" $2
     log_message "INFO: Login successful for user Chuck" $2
     log_message "INFO: Password updated for user Craig" $2
     log_message "SEVERE: Disk write failure" $2
     log_message "SEVERE: Unable to complete transaction - Out of memory" $2
done
