#!/bin/bash

# The command line syntax for running this script is
#      example2-2.sh [S3 bucket location] [S3 object name]
#
# From Chapter 2, page 23 of the book, we use the following parameters to
# load our sample syslog file into S3 so it can be processed by
# Elastic MapReduce.  This script requires that the s3cmd utility has
# been downloaded and is in the path.  The s3cmd utility can be
# downloaded from http://s3tools.org/s3cmd.  It is useful to remember
# that S3 bucket names must be unique and the example used in the book
# is already in use by the authors.  You will need to come up with your
# own unique S3 bucket name when running this script.
#
#     chmod +x example2-2.sh
#     example2-2.sh s3://program-emr sample-syslog.log

s3cmd mb $1
s3cmd put $2 $1
