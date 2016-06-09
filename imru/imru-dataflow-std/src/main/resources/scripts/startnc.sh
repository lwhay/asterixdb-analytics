#!/bin/bash

CUR_DIR=$(cd $(dirname "$0"); pwd)
APPASSEMBLER_DIR=$(cd $(dirname "$CUR_DIR"); pwd)
CCHOST_NAME=`cat conf/master`

echo $(hostname) $1

#Import cluster properties
. $APPASSEMBLER_DIR/conf/cluster.properties

if test -z "$NCTMP_DIR"
then
	echo "Can't load cluster.properties"
	exit
fi

CCHOST=$1
if test -z "$CCHOST"
then
	echo "login to cluster controller to get ip address"
	CCHOST=`ssh ${CCHOST_NAME} "cd ${CURRENT_PATH}; ${CUR_DIR}/getip.sh"`
fi

#Get the IP address of the nc
IPADDR=`$CUR_DIR/getip.sh`

#Get node ID
NODEID=`hostname | cut -d '.' -f 1`

if test -z "$CCHOST"
then
	echo "no parameter"
	exit
fi

#Clean up temp dir

rm -rf $NCTMP_DIR
mkdir $NCTMP_DIR

#Clean up log dir
rm -rf $NCLOGS_DIR
mkdir $NCLOGS_DIR


#Clean up I/O working dir
io_dirs=$(echo $IO_DIRS | tr "," "\n")
for io_dir in $io_dirs
do
	rm -rf $io_dir
	mkdir $io_dir
done

#Set JAVA_HOME
export JAVA_HOME=$JAVA_HOME


#Set JAVA_OPTS
export JAVA_OPTS=$NCJAVA_OPTS

#Enter the temp dir
cd $NCTMP_DIR

#Launch hyracks nc
$CUR_DIR/hyracksnc\
 -cc-host $CCHOST\
 -cc-port $CC_CLUSTERPORT\
 -cluster-net-ip-address $IPADDR\
 -data-ip-address $IPADDR\
 -result-ip-address $IPADDR\
 -node-id $NODEID\
 -iodevices "${IO_DIRS}"\
 -app-nc-main-class edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUNCBootstrapImpl\
 &> $NCLOGS_DIR/$NODEID.log &
