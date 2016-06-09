#!/bin/bash

CUR_DIR=$(cd $(dirname "$0"); pwd)
APPASSEMBLER_DIR=$(cd $(dirname "$CUR_DIR"); pwd)
CCHOST_NAME=`cat ${APPASSEMBLER_DIR}/conf/master`

echo $(hostname)

#Import cluster properties
. $APPASSEMBLER_DIR/conf/cluster.properties

#Get the IP address of the cc
CCHOST=`$CUR_DIR/getip.sh`

if test -z $CCTMP_DIR
then
	echo "Can't load cluster.properties"
	exit
fi


#Remove the temp dir
rm -rf $CCTMP_DIR
mkdir $CCTMP_DIR

#Remove the logs dir
rm -rf $CCLOGS_DIR
mkdir $CCLOGS_DIR

#Export JAVA_HOME and JAVA_OPTS
export JAVA_HOME=$JAVA_HOME
export JAVA_OPTS=$CCJAVA_OPTS

#Launch hyracks cc script
chmod u+x $CUR_DIR/hyrackscc
$CUR_DIR/hyrackscc\
 -client-net-ip-address $CCHOST\
 -cluster-net-ip-address $CCHOST\
 -client-net-port $CC_CLIENTPORT\
 -cluster-net-port $CC_CLUSTERPORT\
 -max-heartbeat-lapse-periods 999999\
 -default-max-job-attempts 0\
 -job-history-size 3\
 -app-cc-main-class edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUCCBootstrapImpl\
 &> $CCLOGS_DIR/cc.log &
