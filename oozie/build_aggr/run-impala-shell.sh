#!/usr/bin/env sh

# as of this writing there does not seem to be an Impala action for Oozie
# this hack adapted from:
# https://community.cloudera.com/t5/Interactive-Short-cycle-SQL/Impala-schedule-with-oozie-tutorial/td-p/23906
# - SJJ 27 Nov 2015

if [ "$#" -ne "3" ]
then
  printf "usage: %s [impalad_host] [hiveql_script] [logfile_prefix]\n" "`basename $0`" >&2
  exit 1
fi

IMPD_HOST=$1
HQL_SCRIPT=$2
LOG_PRE=$3

LOG_DIR=/tmp/oozie-impala-shell-logs
mkdir -p $LOG_DIR

# logging is interesting in a YARN container
LOG=${LOG_DIR}/${LOG_PRE}.log

echo "`date` Running: impala-shell --impalad=${IMPD_HOST} -f ${HQL_SCRIPT}" >> $LOG
export PYTHON_EGG_CACHE=./myeggs
echo "`date` Impala output: " >> $LOG
impala-shell --impalad=${IMPD_HOST} -f ${HQL_SCRIPT} >> $LOG 2>&1
EXIT_CODE=$?

# right now the log is on the local YARN container, so shove into HDFS for later
hdfs dfs -mkdir -p $LOG_DIR
hdfs dfs -put $LOG $LOG_DIR

exit $EXIT_CODE

