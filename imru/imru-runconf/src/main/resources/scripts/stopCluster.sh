CUR_DIR=$(cd $(dirname "$0"); pwd)

${CUR_DIR}/stopAllNCs.sh
sleep 2
${CUR_DIR}/stopcc.sh
