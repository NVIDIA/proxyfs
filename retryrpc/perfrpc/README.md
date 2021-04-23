# PerfRPC
perfrpc is a tool to test and demonstrate the performance of the retryrpc layer.

## Starting a perfrpc Server

  `# ./perfrpc server -ipaddr 127.0.0.1 -port 53167 -tlsdir ./tls`

  where `tls` is the directory where the server should *write* the TLS credentials

## Starting a perfrpc Client

  `# ./perfrpc client -ipaddr 127.0.0.1 -port 53167 -tlsdir ./tls -clients 1000 -messages 100 -warmupcnt 10`

  `clients` is the number of clients the tool should create

  `messages` is the number of messages each client will send in parallel using goroutines

  `tls` is the directory where the client should *read* the TLS credentials

  `warmupcnt` is the number of messages each client will send to setup the connection and test the connection before running the performance test

## What is produced?

perfrpc clients will print a message such as

  `===== PERFRPC - Clients: 10 Messages per Client: 10 Total Messages: 100 ---- Test Duration: 6.207882ms`

illustrating how many clients and total messages were sent followed by the length of time to run the test
