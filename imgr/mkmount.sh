#!/bin/sh

AuthToken=""

while [ "$AuthToken" == "" ]
do
  sleep 1
  AuthToken=`curl -v -s -H "X-Auth-User: test:tester" -H "X-Auth-Key: testing" swift:8080/auth/v1.0 2>&1 | awk /"X-Auth-Token:"/'{print $3}'`
done

echo
echo "    AuthToken: $AuthToken"
echo

curl -s -f -H "X-Auth-Token: $AuthToken" swift:8080/v1/AUTH_test/con -X PUT > /dev/null

DollarQuestionMark=1

while [ "$DollarQuestionMark" != "0" ]
do
  sleep 1
  curl -s -f dev:15346/version > /dev/null
  DollarQuestionMark=$?
done

curl -s -f dev:15346/volume -X POST -d "{\"StorageURL\":\"http://swift:8080/v1/AUTH_test/con\",\"AuthToken\":\"$AuthToken\"}" > /dev/null

curl -s -f dev:15346/volume/testvol -X PUT -d "{\"StorageURL\":\"http://swift:8080/v1/AUTH_test/con\"}" > /dev/null
