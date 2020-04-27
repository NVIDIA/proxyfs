gopregeneratesubdirs = \
	make-static-content

gopkgsubdirs = \
	blunder \
	bucketstats \
	conf \
	confgen \
	dlm \
	evtlog \
	fs \
	fuse \
	halter \
	headhunter \
	httpserver \
	inode \
	jrpcfs \
	liveness \
	logger \
	mkproxyfs \
	platform \
	proxyfsd \
	ramswift \
	retryrpc \
	stats \
	statslogger \
	swiftclient \
	transitions \
	trackedlock \
	utils \
	version

gobinsubdirs = \
	cleanproxyfs \
	fsworkout \
	inodeworkout \
	pfs-crash \
	pfs-fsck \
	pfs-restart-test \
	pfs-stress \
	pfs-swift-load \
	pfsagentd \
	pfsagentd/pfsagentd-swift-auth-plugin \
	pfsagentConfig \
	pfsagentConfig/pfsagentConfig \
	pfsconfjson \
	pfsconfjsonpacked \
	pfsworkout \
	confgen/confgen \
	evtlog/pfsevtlogd \
	mkproxyfs/mkproxyfs \
	proxyfsd/proxyfsd \
	ramswift/ramswift

gobinsubdirsforci = \
	pfsconfjson \
	pfsconfjsonpacked \
	confgen/confgen \
	mkproxyfs/mkproxyfs \
	proxyfsd/proxyfsd

gosubdirsforci = $(gopkgsubdirs) $(gobinsubdirsforci);
gosubdirspathsforci = $(addprefix github.com/swiftstack/ProxyFS/,$(gosubdirsforci))

uname = $(shell uname)
machine = $(shell uname -m)

ifeq ($(uname),Linux)
    ifeq ($(machine),armv7l)
        all: version fmt pre-generate generate install test

        ci: version fmt pre-generate generate install test cover

        minimal: pre-generate generate install
    else
        distro := $(shell python -c "import platform; print platform.linux_distribution()[0]")

        all: version fmt pre-generate generate install test python-test c-clean c-build c-install c-test

        ci: version fmt pre-generate generate install test cover python-test c-clean c-build c-install c-test

        all-deb-builder: version fmt pre-generate generate install c-clean c-build c-install-deb-builder

        minimal: pre-generate generate install c-clean c-build c-install
    endif
else
    all: version fmt pre-generate generate install test

    ci: version fmt pre-generate generate install test cover

    minimal: pre-generate generate install
endif

.PHONY: all all-deb-builder bench c-build c-clean c-install c-install-deb-builder c-test ci clean cover fmt generate install pre-generate python-test test version

bench:
	@set -e; \
	for gosubdir in $(gopkgsubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir bench; \
	done; \
	for gosubdir in $(gobinsubdirs); do \
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
	@set -e; \
	rm -f $(GOPATH)/bin/stringer; \
	for gosubdir in $(gopregeneratesubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir clean; \
	done; \
	for gosubdir in $(gopkgsubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir clean; \
	done; \
	for gosubdir in $(gobinsubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir clean; \
	done

cover:
	@set -e; \
	go get -u github.com/ory/go-acc; \
	go-acc -o coverage.coverprofile $(gosubdirspathsforci)

fmt:
	@set -e; \
	$(MAKE) --no-print-directory -C make-static-content fmt; \
	for gosubdir in $(gopkgsubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir fmt; \
	done; \
	for gosubdir in $(gobinsubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir fmt; \
	done

generate:
	@set -e; \
	for gosubdir in $(gopkgsubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir generate; \
	done; \
	for gosubdir in $(gobinsubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir generate; \
	done

install:
	@set -e; \
	for gosubdir in $(gopkgsubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir install; \
	done; \
	for gosubdir in $(gobinsubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir install; \
	done

pre-generate:
	@set -e; \
	go install github.com/swiftstack/ProxyFS/vendor/golang.org/x/tools/cmd/stringer; \
	for gosubdir in $(gopregeneratesubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir install; \
	done

python-test:
	cd meta_middleware && tox -e lint
	cd pfs_middleware && tox -e py27-release,py27-minver,py36-release,lint

test:
	@set -e; \
	for gosubdir in $(gopkgsubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir test; \
	done; \
	for gosubdir in $(gobinsubdirs); do \
		if [ $$gosubdir == "pfsagentd" ]; \
		then \
			echo "Skipping pfsagentd"; \
		else \
			$(MAKE) --no-print-directory -C $$gosubdir test; \
		fi \
	done

version:
	@go version
