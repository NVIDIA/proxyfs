# Copyright (c) 2015-2021, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

CC=gcc
CFLAGS=-I.
JSON_C_DIR=/Users/kmalone/code/gowork/src/github.com/NVIDIA/samba/json-c
CFLAGS += -I $(JSON_C_DIR)/include/json-c
LDFLAGS+= -L $(JSON_C_DIR)/lib -ljson-c
DEPS =

%.o: %.c $(DEPS)
	$(CC) $(CFLAGS) -c -o $@ $<

client_test: client_test.o
		$(CC) $(LDFLAGS) -o client_test client_test.o -I.
