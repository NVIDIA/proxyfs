CC=gcc
CFLAGS=-I. -I/opt/ss/include -fPIC -g
# The -lrt flag is needed to avoid a link error related to clock_* methods if glibc < 2.17
LDFLAGS += -ljson-c -lpthread -L/opt/ss/lib64 -lrt -lm
DEPS = proxyfs.h
LIBINSTALL?=/usr/lib
LIBINSTALL_CENTOS?=/usr/lib64
INCLUDEDIR?=/usr/include

%.o: %.c $(DEPS)
	$(CC) $(CFLAGS) -c -o $@ $<

all: libproxyfs.so.1.0.0 test

libproxyfs.so.1.0.0: proxyfs_api.o proxyfs_jsonrpc.o proxyfs_req_resp.o json_utils.o base64.o socket.o pool.o ioworker.o time_utils.o fault_inj.o
	$(CC) -shared -fPIC -Wl,-soname,libproxyfs.so.1 -o libproxyfs.so.1.0.0 proxyfs_api.o proxyfs_jsonrpc.o proxyfs_req_resp.o json_utils.o base64.o socket.o pool.o ioworker.o time_utils.o fault_inj.o $(LDFLAGS) -lc
	ln -f -s ./libproxyfs.so.1.0.0 ./libproxyfs.so.1
	ln -f -s ./libproxyfs.so.1.0.0 ./libproxyfs.so


test: proxyfs_api.o proxyfs_jsonrpc.o proxyfs_req_resp.o json_utils.o base64.o socket.o pool.o ioworker.o time_utils.o fault_inj.o test.o
	$(CC) -o test proxyfs_api.o proxyfs_jsonrpc.o proxyfs_req_resp.o json_utils.o base64.o socket.o pool.o ioworker.o time_utils.o fault_inj.o test.o $(CFLAGS) $(LDFLAGS)

pfs_log: proxyfs_api.o proxyfs_jsonrpc.o proxyfs_req_resp.o json_utils.o base64.o socket.o pool.o ioworker.o time_utils.o fault_inj.o pfs_log.o
	$(CC) -o pfs_log proxyfs_api.o proxyfs_jsonrpc.o proxyfs_req_resp.o json_utils.o base64.o socket.o pool.o ioworker.o time_utils.o fault_inj.o pfs_log.o $(CFLAGS) $(LDFLAGS)

pfs_ping: proxyfs_api.o proxyfs_jsonrpc.o proxyfs_req_resp.o json_utils.o base64.o socket.o pool.o ioworker.o time_utils.o fault_inj.o pfs_ping.o
	$(CC) -o pfs_ping proxyfs_api.o proxyfs_jsonrpc.o proxyfs_req_resp.o json_utils.o base64.o socket.o pool.o ioworker.o time_utils.o fault_inj.o pfs_ping.o $(CFLAGS) $(LDFLAGS) -lm

pfs_rw: proxyfs_api.o proxyfs_jsonrpc.o proxyfs_req_resp.o json_utils.o base64.o socket.o pool.o ioworker.o time_utils.o fault_inj.o pfs_rw.o
	$(CC) -o pfs_rw proxyfs_api.o proxyfs_jsonrpc.o proxyfs_req_resp.o json_utils.o base64.o socket.o pool.o ioworker.o time_utils.o fault_inj.o pfs_rw.o $(CFLAGS) $(LDFLAGS) -lm

install:
	cp -f libproxyfs.so.1.0.0 $(LIBINSTALL)/libproxyfs.so.1.0.0
	ln -f -s $(LIBINSTALL)/libproxyfs.so.1.0.0 $(LIBINSTALL)/libproxyfs.so.1
	ln -f -s $(LIBINSTALL)/libproxyfs.so.1.0.0 $(LIBINSTALL)/libproxyfs.so
	cp -f ./proxyfs.h $(INCLUDEDIR)/

installcentos:
	cp -f libproxyfs.so.1.0.0 $(LIBINSTALL_CENTOS)/libproxyfs.so.1.0.0
	ln -f -s $(LIBINSTALL_CENTOS)/libproxyfs.so.1.0.0 $(LIBINSTALL_CENTOS)/libproxyfs.so.1
	ln -f -s $(LIBINSTALL_CENTOS)/libproxyfs.so.1.0.0 $(LIBINSTALL_CENTOS)/libproxyfs.so
	cp -f ./proxyfs.h $(INCLUDEDIR)/

clean:
	rm -f *.o libproxyfs.so.1.0.0 ./libproxyfs.so.1 ./libproxyfs.so test pfs_log pfs_ping pfs_rw

