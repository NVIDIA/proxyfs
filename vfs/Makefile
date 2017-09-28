CC           = gcc
CFLAGS       = -g -Os -fPIC
INSTALLCMD   = /usr/bin/install -c
LDFLAGS      = -lproxyfs
LDSHFLAGS    = -shared

ifneq ($(MAKECMDGOALS),clean)
ifndef SAMBA_SOURCE
$(error SAMBA_SOURCE must be set to samba source directory)
endif
include $(SAMBA_SOURCE)/VERSION
FLAGS = $(CFLAGS) -I$(SAMBA_SOURCE) -I$(SAMBA_SOURCE)/source3 -I$(SAMBA_SOURCE)/source3/include -I$(SAMBA_SOURCE)/lib/talloc -I$(SAMBA_SOURCE)/lib/tevent -I$(SAMBA_SOURCE)/lib/replace -I$(SAMBA_SOURCE)/bin/default -I$(SAMBA_SOURCE)/bin/default/include -I.

# In case the user wants vfs module to be build without proxyfs rpc client installed, can supply the path to
# proxyfs rpc library proxfs.h header file.
ifdef PROXYFS_RPC_INCLUDE
FLAGS += -I$(PROXYFS_RPC_INCLUDE)
endif
endif

DEPS         = vfs_proxyfs.h

VFS_CENTOS_LIBDIR   ?= /usr/lib64/samba/vfs
VFS_LIBDIR   ?= /usr/lib/x86_64-linux-gnu/samba/vfs

all: proxyfs.so

%.o: %.c $(DEPS)

	@echo "Compiling $<"
	$(CC) -DSAMBA_VERSION_MAJOR=$(SAMBA_VERSION_MAJOR) -DSAMBA_VERSION_MINOR=$(SAMBA_VERSION_MINOR) $(FLAGS) -c $<

proxyfs.so: vfs_proxyfs.o
	@echo "Linking $@"
	$(CC)  -o $@ $< $(LDSHFLAGS) $(LDFLAGS)

install:
	$(INSTALLCMD) -d $(VFS_LIBDIR)
	$(INSTALLCMD) -m 755 proxyfs.so $(VFS_LIBDIR)

installcentos:
	$(INSTALLCMD) -d $(VFS_CENTOS_LIBDIR)
	$(INSTALLCMD) -m 755 proxyfs.so $(VFS_CENTOS_LIBDIR)

clean:
	rm -f vfs_proxyfs.o proxyfs.so
