CUR_DIR=$(cd $(dirname "$0"); pwd)

$CUR_DIR/startcc.sh
sleep 5
$CUR_DIR/startAllNCs.sh
