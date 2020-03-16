#!/bin/bash

APP_NAME=edc-plsqltool-ms-test
APP_VERSION=1.0.0
APP_ENV=${appenv:-dev}
LOG_MAX_SIZE=20MB
XMX_SIZE=300M

JAVA_HOME=/usr/java/jdk1.8.0_31

# change directory to program dir
FWDIR="$(cd `dirname $0`/..; pwd)"
cd ${FWDIR}

if [ ! -d ${FWDIR}/java.pid ]; then
    touch ${FWDIR}/java.pid
fi

OSUSER=$(id -nu)
PSNUM=$(cat ${FWDIR}/java.pid)
if [[ "$PSNUM" -ne "" ]]; then
    echo ${APP_NAME}" has been started! stop first."
    exit;
fi

nohup ${JAVA_HOME}/bin/java -Xmx${XMX_SIZE} -Dspring.profiles.active=${APP_ENV} -Dsun.security.krb5.debug=true -Dapp.name=${APP_NAME} -Dapp.env=${APP_ENV} -Dmax.size=${LOG_MAX_SIZE} -Duser.dir=${FWDIR} -jar ${APP_NAME}-${APP_VERSION}.jar > nohup.out 2>&1 &
echo $! > ${FWDIR}/java.pid
exit
