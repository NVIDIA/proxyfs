# Copyright (c) 2015-2021, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

gopregeneratesubdirs = \
	make-static-content

gopkgsubdirs = \
	blunder \
	bucketstats \
	conf \
	confgen \
	dlm \
    etcdclient \
	evtlog \
	fs \
	fuse \
	halter \
	headhunter \
	httpserver \
	iauth \
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
	version \
	emswift/emswiftpkg \
	icert/icertpkg
	imgr/imgrpkg

goplugindirs = \
	iauth/iauth-swift

gobinsubdirs = \
	cleanproxyfs \
	emswift \
	fsworkout \
	icert \
	imgr \
	inodeworkout \
	pfs-crash \
	pfs-fsck \
	pfs-jrpc \
	pfs-restart-test \
	pfs-stress \
	pfs-swift-load \
	pfsagentd \
	pfsagentd/pfsagentd-init \
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
	emswift \
	imgr \
	pfsconfjson \
	pfsconfjsonpacked \
	confgen/confgen \
	mkproxyfs/mkproxyfs \
	proxyfsd/proxyfsd

gosubdirsforci = $(gopkgsubdirs) $(gobinsubdirsforci);
gosubdirspathsforci = $(addprefix github.com/NVIDIA/proxyfs/,$(gosubdirsforci))

uname = $(shell uname)
machine = $(shell uname -m)

ifeq ($(uname),Linux)
    ifeq ($(machine),armv7l)
        all: version fmt pre-generate generate install test

        ci: version fmt pre-generate generate install test cover

        minimal: pre-generate generate install
    else
        distro := $(shell python -c "import platform; print platform.linux_distribution()[0]")

        all: version fmt pre-generate generate install test python-test

        ci: version fmt pre-generate generate install test cover python-test

        all-deb-builder: version fmt pre-generate generate install

        minimal: pre-generate generate install
    endif
else
    all: version fmt pre-generate generate install test

    ci: version fmt pre-generate generate install test cover

    minimal: pre-generate generate install
endif

pfsagent: pre-generate generate pfsagent-install

.PHONY: all all-deb-builder bench ci clean cover fmt generate install pfsagent pfsagent-install pre-generate python-test test version

bench:
	@set -e; \
	for gosubdir in $(gopkgsubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir bench; \
	done; \
	for gosubdir in $(gobinsubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir bench; \
	done

clean:
	@set -e; \
	rm -f $(GOPATH)/bin/stringer; \
	for gosubdir in $(gopregeneratesubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir clean; \
	done; \
	for gosubdir in $(gopkgsubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir clean; \
	done; \
	for gosubdir in $(goplugindirs); do \
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
	for gosubdir in $(goplugindirs); do \
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
	for gosubdir in $(goplugindirs); do \
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
	for gosubdir in $(goplugindirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir install; \
	done; \
	for gosubdir in $(gobinsubdirs); do \
		$(MAKE) --no-print-directory -C $$gosubdir install; \
	done

pfsagent-install:
	$(MAKE) --no-print-directory -C pfsagentd install
	$(MAKE) --no-print-directory -C pfsagentd/pfsagentd-init install
	$(MAKE) --no-print-directory -C pfsagentd/pfsagentd-swift-auth-plugin install

pre-generate:
	@set -e; \
	go install golang.org/x/tools/cmd/stringer; \
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
	for gosubdir in $(goplugindirs); do \
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
