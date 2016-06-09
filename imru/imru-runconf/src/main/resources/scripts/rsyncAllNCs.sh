#
# This script 
# 1. add public key of this machine to all slaves
# 2. synchronize imru-dist to all slaves.
#

CUR_DIR=$(cd $(dirname "$0"); pwd)
APPASSEMBLER_DIR=$(cd $(dirname "$CUR_DIR"); pwd)
HOME=$(cd ~; pwd)

#Get the IP address of the cc
CCHOST=`$CUR_DIR/getip.sh`

if test ! "(" -e $HOME/.ssh/id_rsa.pub ")"
then
	ssh-keygen -t rsa -f $HOME/.ssh/id_rsa -N ''
fi

PUB_KEY=$(cat $HOME/.ssh/id_rsa.pub)


for i in `cat $APPASSEMBLER_DIR/conf/slaves`
do
	ssh $i "cd ~/.ssh;grep \"$PUB_KEY\" authorized_keys > /dev/null;if test \$? -ne 0;then echo 'add pub key';echo \"$PUB_KEY\" > authorized_keys;chmod 600 authorized_keys;else echo 'already has pub key';fi;mkdir -p ${APPASSEMBLER_DIR}"
	rsync -vrultzC ${APPASSEMBLER_DIR}/ $i:${APPASSEMBLER_DIR}/
done
