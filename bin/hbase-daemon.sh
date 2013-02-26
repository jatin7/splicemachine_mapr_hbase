#!/usr/bin/env bash
#
#/**
# * Copyright 2007 The Apache Software Foundation
# *
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */
# 
# Runs a Hadoop hbase command as a daemon.
#
# Environment Variables
#
#   HBASE_CONF_DIR   Alternate hbase conf dir. Default is ${HBASE_HOME}/conf.
#   HBASE_LOG_DIR    Where log files are stored.  PWD by default.
#   HBASE_PID_DIR    The pid files are stored. /tmp by default.
#   HBASE_IDENT_STRING   A string representing this instance of hadoop. $USER by default
#   HBASE_NICENESS The scheduling priority for daemons. Defaults to 0.
#
# Modelled after $HADOOP_HOME/bin/hadoop-daemon.sh

usage="Usage: hbase-daemon.sh [--config <conf-dir>]\
 (start|stop|restart|status) <hbase-command> \
 <args...>"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/hbase-config.sh

# get arguments
startStop=$1
shift

command=$1
shift

hbase_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
    num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
    while [ $num -gt 1 ]; do
        prev=`expr $num - 1`
        [ -f "$log.$prev" ] && mv -f "$log.$prev" "$log.$num"
        num=$prev
    done
    mv -f "$log" "$log.$num";
    fi
}

wait_until_done ()
{
    p=$1
    cnt=${HBASE_SLAVE_TIMEOUT:-300}
    origcnt=$cnt
    while kill -0 $p > /dev/null 2>&1; do
      if [ $cnt -gt 1 ]; then
        cnt=`expr $cnt - 1`
        sleep 1
      else
        echo "Process did not complete after $origcnt seconds, killing."
        kill -9 $p
        exit 1
      fi
    done
    return 0
}

# get log directory
if [ "$HBASE_LOG_DIR" = "" ]; then
  export HBASE_LOG_DIR="$HBASE_HOME/logs"
fi
mkdir -p "$HBASE_LOG_DIR"

if [ "$HBASE_PID_DIR" = "" ]; then
  HBASE_PID_DIR=/tmp
fi

if [ "$HBASE_IDENT_STRING" = "" ]; then
  export HBASE_IDENT_STRING="$USER"
fi

# Some variables
# Work out java location so can print version into log.
if [ "$JAVA_HOME" != "" ]; then
  #echo "run java in $JAVA_HOME"
  JAVA_HOME=$JAVA_HOME
fi
if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
fi
JAVA=$JAVA_HOME/bin/java
export HBASE_LOG_PREFIX=hbase-$HBASE_IDENT_STRING-$command-$HOSTNAME
export HBASE_LOGFILE=$HBASE_LOG_PREFIX.log
export HBASE_ROOT_LOGGER="INFO,DRFA"
export HBASE_SECURITY_LOGGER="INFO,DRFAS"
logout=$HBASE_LOG_DIR/$HBASE_LOG_PREFIX.out  
loggc=$HBASE_LOG_DIR/$HBASE_LOG_PREFIX.gc
loglog="${HBASE_LOG_DIR}/${HBASE_LOGFILE}"
pid=$HBASE_PID_DIR/hbase-$HBASE_IDENT_STRING-$command.pid

if [ -n "$SERVER_GC_OPTS" ]; then
  export SERVER_GC_OPTS=${SERVER_GC_OPTS/"-Xloggc:<FILE-PATH>"/"-Xloggc:${loggc}"}
fi
if [ -n "$CLIENT_GC_OPTS" ]; then
  export CLIENT_GC_OPTS=${CLIENT_GC_OPTS/"-Xloggc:<FILE-PATH>"/"-Xloggc:${loggc}"}
fi

# Set default scheduling priority
if [ "$HBASE_NICENESS" = "" ]; then
    export HBASE_NICENESS=0
fi

case $startStop in

  (start)
    mkdir -p "$HBASE_PID_DIR"
    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    hbase_rotate_log $logout
    hbase_rotate_log $loggc
    echo starting $command, logging to $logout
    # Add to the command log file vital stats on our environment.
    echo "`date` Starting $command on `hostname`" >> $loglog
    echo "`ulimit -a`" >> $loglog 2>&1

    # Wait for filesystem to initialize
    i=0
    while [ $i -lt 600 ]; do
      if [ `expr $i % 60` = "0" ]; then
        # Log message for every 60 seconds
        echo "`date` Waiting for filesystem to come up"  >> $loglog 2>&1
      fi
      hadoop fs -stat "/" >/dev/null 2>&1
      if [ $? -eq 0 ] ; then
       i=9999
       break
      fi

      sleep 3
      i=$[i+3]
    done

    if [ $i -ne 9999 ] ; then
      echo "`date` Giving up after 600 attempts"  >> $loglog 2>&1
      exit 1
    fi

    # Create Hbase volume and set compression off
    hbaseVolume=${HBASE_VOLUME:-mapr.hbase}
    mountDir=$(grep "maprfs://" "${HBASE_CONF_DIR}"/hbase-site.xml | sed "s/^.*maprfs:\/\///" | sed "s/<\/.*$//")
    maprcli volume info -name "${hbaseVolume}" > /dev/null 2>&1
    if [ $? -eq 0 ] ; then
      echo "`date` HBase root is on volume '${hbaseVolume}'."  >> $loglog 2>&1
    else
      echo "`date` HBase volume '${hbaseVolume}' not found, creating."  >> $loglog 2>&1
      maprcli volume create -name "${hbaseVolume}" -path "${mountDir}" -replicationtype low_latency >> $loglog 2>&1
    fi

    # set 64M chunksize and turn off compression
    hadoop mfs -setcompression off "${mountDir}" >> $loglog 2>&1
    hadoop mfs -setchunksize 67108864 "${mountDir}" >> $loglog 2>&1

    nohup nice -n $HBASE_NICENESS "$HBASE_HOME"/bin/hbase \
        --config "${HBASE_CONF_DIR}" \
        $command "$@" $startStop > "$logout" 2>&1 < /dev/null &
    echo $! > $pid
    sleep 1; head "$logout"
    ;;

  (stop)
    if [ -f $pid ]; then
      # kill -0 == see if the PID exists 
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo -n stopping $command
        echo "`date` Terminating $command" >> $loglog
        kill `cat $pid` > /dev/null 2>&1
        cnt=${HBASE_SLAVE_TIMEOUT:-300}
        origcnt=$cnt
        while kill -0 `cat $pid` > /dev/null 2>&1; do
          if [ $cnt -gt 1 ]; then
            cnt=`expr $cnt - 1`
            echo -n "."
            sleep 1;
          else
            echo "Process did not complete after $origcnt seconds, killing."
            kill -9 `cat $pid` > /dev/null 2>&1
            break;
          fi
        done
        rm $pid
        echo
      else
        retval=$?
        echo no $command to stop because kill -0 of pid `cat $pid` failed with status $retval
      fi
    else
      echo no $command to stop because no pid file $pid
    fi
    ;;

  (status)
    if [ -f "$pid" ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`.
        exit 0
      fi
        echo $pid exists with pid `cat $pid` but no $command.
        exit 1
    fi
    echo $command not running.
    exit 1
    ;;

  (restart)
    thiscmd=$0
    args=$@
    # stop the command
    $thiscmd --config "${HBASE_CONF_DIR}" stop $command $args &
    wait_until_done $!
    # wait a user-specified sleep period
    sp=${HBASE_RESTART_SLEEP:-3}
    if [ $sp -gt 0 ]; then
      sleep $sp
    fi
    # start the command
    $thiscmd --config "${HBASE_CONF_DIR}" start $command $args &
    wait_until_done $!
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac
