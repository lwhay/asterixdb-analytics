hostname

CUR_DIR=$(cd $(dirname "$0"); pwd)
APPASSEMBLER_DIR=$(cd $(dirname "$CUR_DIR"); pwd)
. $APPASSEMBLER_DIR/conf/cluster.properties

if test -z $IO_DIRS
then
	echo "Can't load cluster.properties"
	exit
fi

#Kill process
PID=`ps -ef|grep ${USER}|grep java|grep 'Dapp.name=hyracksnc'|awk '{print $2}'`

if [ "$PID" == "" ]; then
  USERID=`id | sed 's/^uid=//;s/(.*$//'`
  PID=`ps -ef|grep ${USERID}|grep java|grep 'Dapp.name=hyracksnc'|awk '{print $2}'`
fi

echo $PID
echo kill -9 $PID
kill -9 $PID

#Clean up I/O working dir
io_dirs=$(echo $IO_DIRS | tr "," "\n")
for io_dir in $io_dirs
do
	rm -rf $io_dir/*
done

#Clean up NC temp dir
rm -rf $NCTMP_DIR/*
