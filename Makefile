# Copyright (c) 2015-2021, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

gopkgdirs = \
	bucketstats \
	conf \
	iauth \
	ilayout \
	retryrpc \
	utils \
	version \
	icert/icertpkg \
	iclient/iclientpkg \
	imgr/imgrpkg \
	iswift/iswiftpkg

goplugindirs = \
	iauth/iauth-swift

gobindirs = \
	icert \
	iclient \
	imgr \
	iswift

godirsforci = $(gopkgdirs) $(goplugindirs) $(gobindirs);
godirpathsforci = $(addprefix github.com/NVIDIA/proxyfs/,$(godirsforci))

all: version fmt generate build test

ci: version fmt generate build test cover

.PHONY: all bench build ci clean cover fmt generate test version

bench:
	@set -e; \
	for godir in $(gopkgdirs); do \
		$(MAKE) --no-print-directory -C $$godir bench; \
	done; \
	for godir in $(goplugindirs); do \
		$(MAKE) --no-print-directory -C $$godir bench; \
	done; \
	for godir in $(gobindirs); do \
		$(MAKE) --no-print-directory -C $$godir bench; \
	done

build:
	@set -e; \
	for godir in $(gopkgdirs); do \
		$(MAKE) --no-print-directory -C $$godir build; \
	done; \
	for godir in $(goplugindirs); do \
		$(MAKE) --no-print-directory -C $$godir build; \
	done; \
	for godir in $(gobindirs); do \
		$(MAKE) --no-print-directory -C $$godir build; \
	done

clean:
	@set -e; \
	for godir in $(gopkgdirs); do \
		$(MAKE) --no-print-directory -C $$godir clean; \
	done; \
	for godir in $(goplugindirs); do \
		$(MAKE) --no-print-directory -C $$godir clean; \
	done; \
	for godir in $(gobindirs); do \
		$(MAKE) --no-print-directory -C $$godir clean; \
	done; \
	rm -f go-acc

cover:
	@set -e; \
	go get -u github.com/ory/go-acc; \
	go build github.com/ory/go-acc; \
	./go-acc -o coverage.coverprofile $(godirpathsforci)

fmt:
	@set -e; \
	$(MAKE) --no-print-directory -C make-static-content fmt; \
	for godir in $(gopkgdirs); do \
		$(MAKE) --no-print-directory -C $$godir fmt; \
	done; \
	for godir in $(goplugindirs); do \
		$(MAKE) --no-print-directory -C $$godir fmt; \
	done; \
	for godir in $(gobindirs); do \
		$(MAKE) --no-print-directory -C $$godir fmt; \
	done

generate:
	@set -e; \
	for godir in $(gopkgdirs); do \
		$(MAKE) --no-print-directory -C $$godir generate; \
	done; \
	for godir in $(goplugindirs); do \
		$(MAKE) --no-print-directory -C $$godir generate; \
	done; \
	for godir in $(gobindirs); do \
		$(MAKE) --no-print-directory -C $$godir generate; \
	done

test:
	@set -e; \
	for godir in $(gopkgdirs); do \
		$(MAKE) --no-print-directory -C $$godir test; \
	done; \
	for godir in $(goplugindirs); do \
		$(MAKE) --no-print-directory -C $$godir test; \
	done; \
	for godir in $(gobindirs); do \
		$(MAKE) --no-print-directory -C $$godir test; \
	done; \

version:
	@go version
