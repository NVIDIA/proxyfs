gosubdirs = \
	blunder \
	cleanproxyfs \
	conf \
	dlm \
	fs \
	fsworkout \
	fuse \
	headhunter \
	httpserver \
	inode \
	inodeworkout \
	jrpcfs \
	logger \
	mkproxyfs mkproxyfs/mkproxyfs \
	pfs-stress \
	pfsconfjson pfsconfjsonpacked \
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
    all: fmt install stringer generate test vet c-clean c-build c-install c-test
else
    all: fmt install stringer generate test vet
endif

.PHONY: all bench c-build c-clean c-install c-test clean cover fmt generate install stringer test vet

bench:
	@for gosubdir in $(gosubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir bench; \
	done

c-build:
	$(MAKE) --no-print-directory -C jrpcclient all
	$(MAKE) --no-print-directory -C vfs

c-clean:
	$(MAKE) --no-print-directory -C jrpcclient clean
	$(MAKE) --no-print-directory -C vfs clean

c-install:
	# TODO: For both jrpcclient & vfs:
	#         Need to handle deb-builder case that doesn't want to use "sudo -E" to invoke $(MAKE)
	#         Need to handle non-CentOS case that wants to just do $(MAKE) install
	sudo -E $(MAKE) --no-print-directory -C jrpcclient installcentos
	sudo -E $(MAKE) --no-print-directory -C vfs installcentos

c-test:
	cd jrpcclient ; ./regression_test.py --just-test-libs ; cd -

clean:
	rm -f $(GOPATH)/bin/stringer
	@for gosubdir in $(gosubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir clean; \
	done

cover:
	@for gosubdir in $(gosubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir cover; \
	done

fmt:
	@for gosubdir in $(gosubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir fmt; \
	done

generate:
	@for gosubdir in $(gosubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir generate; \
	done

install:
	@for gosubdir in $(gosubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir install; \
	done

stringer:
	go install github.com/swiftstack/ProxyFS/vendor/golang.org/x/tools/cmd/stringer

test:
	@for gosubdir in $(gosubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir test; \
	done

vet:
	@for gosubdir in $(gosubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir vet; \
	done
