CUR_DIR=$(cd $(dirname "$0"); pwd)
APPASSEMBLER_DIR=$(cd $(dirname "$CUR_DIR"); pwd)

#Get the IP address of the cc
CCHOST=`$CUR_DIR/getip.sh`

for i in `cat $APPASSEMBLER_DIR/conf/slaves`
do
   ssh $i "cd ${APPASSEMBLER_DIR}; bin/startnc.sh $CCHOST"
done
