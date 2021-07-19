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
	ilayout \
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
	icert/icertpkg \
	iclient/iclientpkg \
	imgr/imgrpkg \
	iswift/iswiftpkg

goplugindirs = \
	iauth/iauth-swift

gobinsubdirs = \
	cleanproxyfs \
	emswift \
	fsworkout \
	icert \
	iclient \
	imgr \
	inodeworkout \
	iswift \
	pfs-crash \
	pfs-fsck \
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

ifeq ($(uname),Linux)
    all: version fmt pre-generate generate install test python-test

    ci: version fmt pre-generate generate install test cover python-test

    all-deb-builder: version fmt pre-generate generate install

    minimal: pre-generate generate install
else
    all: version fmt pre-generate generate install test

    ci: version fmt pre-generate generate install test cover

    minimal: pre-generate generate install
endif

pfsagent: pre-generate generate pfsagent-install

.PHONY: all all-deb-builder bench ci clean cover fmt generate install pfsagent pfsagent-install pre-generate python-test test version rework

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
	cd pfs_middleware && LATEST_SWIFT_TAG=` git ls-remote --refs --tags https://github.com/NVIDIA/swift.git | cut -d '/' -f 3 | grep ^ss-release- | sort --version-sort | tail -n 1 ` tox -e py27-release,py27-minver,py36-release,lint

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

rework:
	$(MAKE) -f Makefile.rework
