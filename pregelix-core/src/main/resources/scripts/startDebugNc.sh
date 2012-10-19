hostname

#Get the IP address of the cc
CCHOST_NAME=`cat conf/master`
CCHOST=`dig +short $CCHOST_NAME`

#Import cluster properties
. conf/cluster.properties
. conf/debugnc.properties

#Clean up temp dir

rm -rf $NCTMP_DIR2
mkdir $NCTMP_DIR2

#Clean up log dir
rm -rf $NCLOGS_DIR2
mkdir $NCLOGS_DIR2


#Clean up I/O working dir
io_dirs=$(echo $IO_DIRS2 | tr "," "\n")
for io_dir in $io_dirs
do
	rm -rf $io_dir
	mkdir $io_dir
done

#Set JAVA_HOME
export JAVA_HOME=$JAVA_HOME

#Get OS
OS_NAME=`uname -a|awk '{print $1}'`
LINUX_OS='Linux'

if [ $OS_NAME = $LINUX_OS ];
then
	#Get IP Address
	IPADDR=`/sbin/ifconfig eth0 | grep "inet addr" | awk '{print $2}' | cut -f 2 -d ':'`
else
	IPADDR=`/sbin/ifconfig en1 | grep "inet " | awk '{print $2}' | cut -f 2 -d ':'`
fi

#Get node ID
NODEID=`hostname | cut -d '.' -f 1`
NODEID=${NODEID}2

#Set JAVA_OPTS
export JAVA_OPTS=$NCJAVA_OPTS2

cd $HYRACKS_HOME
HYRACKS_HOME=`pwd`

#Enter the temp dir
cd $NCTMP_DIR2

#Launch hyracks nc
$HYRACKS_HOME/hyracks-server/target/appassembler/bin/hyracksnc -cc-host $CCHOST -cc-port $CC_CLUSTERPORT -cluster-net-ip-address $IPADDR  -data-ip-address $IPADDR -node-id $NODEID -iodevices "${IO_DIRS2}" &> $NCLOGS_DIR2/$NODEID.log &
