CC           = gcc
CFLAGS       = -g -Os -fPIC
INSTALLCMD   = /usr/bin/install -c
SAMBA_SOURCE ?= ../../samba/
JRPC_SOURCE  ?= ../jrpcclient/
JRPC_OBJECT  ?= ../jrpcclient/
LDFLAGS      = -L$(JRPC_OBJECT) -lproxyfs
LDSHFLAGS    = -shared
FLAGS        =  $(CFLAGS) -I$(JRPC_SOURCE) -I$(SAMBA_SOURCE) -I$(SAMBA_SOURCE)/source3 -I$(SAMBA_SOURCE)/source3/include -I$(SAMBA_SOURCE)/lib/talloc -I$(SAMBA_SOURCE)/lib/tevent -I$(SAMBA_SOURCE)/lib/replace -I$(SAMBA_SOURCE)/bin/default -I$(SAMBA_SOURCE)/bin/default/include -I.
DEPS         = vfs_proxyfs.h

# When the VM is provisioned, the cookbooks setup
include $(SAMBA_SOURCE)/VERSION

#VFS_LIBDIR  ?= /usr/lib
VFS_CENTOS_LIBDIR   ?= /usr/lib64/samba/vfs
VFS_LIBDIR   ?= /usr/lib/x86_64-linux-gnu/samba/vfs
#VFS_LIBDIR  = /usr/local/samba/lib/vfs

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
