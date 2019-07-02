#!/bin/bash

# jenkins.sh
#
# Copyright © 2019 Dynamic Object Language Labs Inc.
#
# This software is licensed under the terms of the
# Apache License, Version 2.0 which can be found in
# the file LICENSE at the root of this distribution.

# NOTE this script will exit on the first failure

set -e

JAVA_VER=`java -version 2>&1 | grep 'version' | cut -d ' ' -f 3`

echo "Java version $JAVA_VER"
if [[ ^$JAVA_VER =~ "1.7" ]];
then
    echo "We have java 7"
    # https://github.com/boot-clj/boot/wiki/JVM-Options
    export BOOT_JVM_OPTIONS="-Xmx2g -client -XX:+TieredCompilation -XX:TieredStopAtLevel=1 -XX:MaxPermSize=128m -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -Xverify:none -Dhttps.protocols=TLSv1.2"
    echo "boot jvm options: $BOOT_JVM_OPTIONS"
fi

program=$(basename $0)
code=$(dirname $0)

#echo $program

cd "$code/.."
base_dir="$(pwd -P)"

#echo "Startup Dir: $base_dir"

#export PATH=${PATH}:/bin:$code/bin

check_ret () {
ret=$1
msg=$2
#echo "$ret"
if [ $ret -ne 0 ]; then
        echo "$msg failed with return code $ret"
        exit 1
fi
}

echo ""
echo "Ensuring jahmm is downloaded first. lein version 2.7.1 is required"
echo "lein version is:"
lein -v
check_ret $? "lein -v"

echo ""
lein deps
check_ret $? "jahmm download using lein deps"

#echo " "
#echo "-- pamela-tools dependencies --"
#boot show --deps

echo ""
echo "Compile / aot test / Checking for errors in clj files"
boot check-errors
check_ret $? "boot check-errors"

echo ""
echo "Running unit tests"
boot test
check_ret $? "boot test"

echo ""
echo "-- clean target/ -- "
rm -rf target

echo ""
echo "Creating pamela-tools jar file"
boot build
check_ret $? "boot build"

echo ""
echo "Creating uber jars"
boot uber-dispatcher
check_ret $? "boot uber-dispatcher"

boot uber-plant-sim
check_ret $? "boot uber-plant-sim"

echo""
boot uber-rmq-logger
check_ret $? "boot uber-rmq-logger"

echo ""
boot uber-log-player
check_ret $? "boot uber-log-player"

echo ""
ls -lh target/*.jar

echo ""
echo "--- Done jenkins.sh --- "