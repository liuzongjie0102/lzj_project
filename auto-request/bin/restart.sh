#!/bin/bash

FWDIR="$(cd `dirname $0`/..; pwd)"

echo 停止服务
${FWDIR}/bin/stop.sh

echo kill进程
kill -9 `ps -ef|grep java|grep ${FWDIR} |grep -v grep|awk '{print $2}'`
echo "" > ${FWDIR}/java.pid

echo 启动服务
${FWDIR}/bin/start.sh