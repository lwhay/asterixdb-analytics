CUR_DIR=$(cd $(dirname "$0"); pwd)
APPASSEMBLER_DIR=$(cd $(dirname "$CUR_DIR"); pwd)

for i in `cat ${APPASSEMBLER_DIR}/conf/slaves`
do
   ssh $i "cd ${APPASSEMBLER_DIR}; bin/stopnc.sh"
done
