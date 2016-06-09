hostname

CUR_DIR=$(cd $(dirname "$0"); pwd)
APPASSEMBLER_DIR=$(cd $(dirname "$CUR_DIR"); pwd)
. $APPASSEMBLER_DIR/conf/cluster.properties

if test -z $CCTMP_DIR
then
	echo "Can't load cluster.properties"
	exit
fi

#Kill process
PID=`ps -ef|grep ${USER}|grep java|grep hyracks|awk '{print $2}'`
echo $PID
echo kill -9 $PID
kill -9 $PID

#Clean up CC temp dir
rm -rf $CCTMP_DIR/*
