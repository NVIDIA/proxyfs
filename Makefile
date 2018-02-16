gosubdirs = \
	blunder \
	cleanproxyfs \
	conf \
	dlm \
	evtlog evtlog/pfsevtlogd \
	fs \
	fsworkout \
	fuse \
	halter \
	headhunter \
	httpserver \
	inode \
	inodeworkout \
	jrpcfs \
	logger \
	mkproxyfs mkproxyfs/mkproxyfs \
	pfs-stress \
	pfsconfjson pfsconfjsonpacked \
	pfs-crash \
	pfsworkout \
	platform \
	proxyfsd proxyfsd/proxyfsd \
	ramswift ramswift/ramswift \
	stats \
	statslogger \
	swiftclient \
	utils

uname := $(shell uname)

ifeq ($(uname),Linux)
    distro := $(shell python -c "import platform; print platform.linux_distribution()[0]")

    all: fmt install stringer generate test python-test vet c-clean c-build c-install c-test

    all-deb-builder: fmt install stringer generate vet c-clean c-build c-install-deb-builder
else
    all: fmt install stringer generate test vet
endif

.PHONY: all all-deb-builder bench c-build c-clean c-install c-install-deb-builder c-test clean cover fmt generate install python-test stringer test vet

bench:
	@set -e; \
	for gosubdir in $(gosubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir bench; \
	done

c-build:
	$(MAKE) -w -C jrpcclient all
	$(MAKE) -w -C vfs

c-clean:
	$(MAKE) -w -C jrpcclient clean
	$(MAKE) -w -C vfs clean

c-install:
ifeq ($(distro),CentOS Linux)
	sudo -E $(MAKE) -w -C jrpcclient installcentos
	sudo -E $(MAKE) -w -C vfs installcentos
else
	sudo -E $(MAKE) -w -C jrpcclient install
	sudo -E $(MAKE) -w -C vfs install
endif

c-install-deb-builder:
ifeq ($(distro),CentOS Linux)
	$(MAKE) -w -C jrpcclient installcentos
	$(MAKE) -w -C vfs installcentos
else
	$(MAKE) -w -C jrpcclient install
	$(MAKE) -w -C vfs install
endif

c-test:
	cd jrpcclient ; ./regression_test.py --just-test-libs

clean:
	rm -f $(GOPATH)/bin/stringer
	@for gosubdir in $(gosubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir clean; \
	done

cover:
	@set -e; \
	for gosubdir in $(gosubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir cover; \
	done

fmt:
	@set -e; \
	for gosubdir in $(gosubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir fmt; \
	done

generate:
	@set -e; \
	for gosubdir in $(gosubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir generate; \
	done

install:
	@set -e; \
	for gosubdir in $(gosubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir install; \
	done

python-test:
	cd pfs_middleware && tox -e py27,py27-old-swift,lint

stringer:
	go install github.com/swiftstack/ProxyFS/vendor/golang.org/x/tools/cmd/stringer

test:
	@set -e; \
	for gosubdir in $(gosubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir test; \
	done;

vet:
	@set -e; \
	for gosubdir in $(gosubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir vet; \
	done
