CC=gcc
CFLAGS=-I.
JSON_C_DIR=/Users/kmalone/code/gowork/src/github.com/swiftstack/samba/json-c
CFLAGS += -I $(JSON_C_DIR)/include/json-c
LDFLAGS+= -L $(JSON_C_DIR)/lib -ljson-c
DEPS =

%.o: %.c $(DEPS)
	$(CC) $(CFLAGS) -c -o $@ $<

client_test: client_test.o
		$(CC) $(LDFLAGS) -o client_test client_test.o -I.
