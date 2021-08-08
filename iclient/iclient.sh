#!/bin/sh

AuthToken=""

while [ "$AuthToken" == "" ]
do
  sleep 1
  AuthToken=`curl -v -s -H "X-Auth-User: test:tester" -H "X-Auth-Key: testing" swift:8080/auth/v1.0 2>&1 | awk /"X-Auth-Token:"/'{print $3}'`
done

curl -v -s -H "X-Auth-Token: $AuthToken" swift:8080/v1/AUTH_test/con -X PUT

DollarQuestionMark=1

while [ "$DollarQuestionMark" != "0" ]
do
  sleep 1
  curl imgr:15346/version -f > /dev/null -s
  DollarQuestionMark=$?
done

curl -v -s imgr:15346/volume -X POST -d "{\"StorageURL\":\"http://swift:8080/v1/AUTH_test/con\",\"AuthToken\":\"$AuthToken\"}"

sleep 10000
