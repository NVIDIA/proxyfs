# Copyright (c) 2015-2021, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

gopregeneratedirs = \
	ihtml/make-static-content

gopkgdirs = \
	bucketstats \
	conf \
	iauth \
	ihtml \
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
	idestroy \
	imgr \
	iswift

godirsforci = $(gopkgdirs) $(gobindirs)
godirpathsforci = $(addprefix github.com/NVIDIA/proxyfs/,$(godirsforci))

generatedfiles := \
	coverage.coverprofile

all: version fmt pre-generate generate build test

ci: version fmt pre-generate generate build test cover

minimal: version pre-generate generate build

.PHONY: all bench build ci clean cover fmt generate minimal pre-generate test version

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
	for godir in $(gopregeneratedirs); do \
		$(MAKE) --no-print-directory -C $$godir clean; \
	done; \
	for godir in $(gopkgdirs); do \
		$(MAKE) --no-print-directory -C $$godir clean; \
	done; \
	for godir in $(goplugindirs); do \
		$(MAKE) --no-print-directory -C $$godir clean; \
	done; \
	for godir in $(gobindirs); do \
		$(MAKE) --no-print-directory -C $$godir clean; \
	done; \
	for generatedfile in $(generatedfiles); do \
		rm -f $$generatedfile; \
	done; \
	rm -f go-acc

cover:
	@set -e; \
	go get -u github.com/ory/go-acc; \
	go build github.com/ory/go-acc; \
	./go-acc -o coverage.coverprofile $(godirpathsforci)

fmt:
	@set -e; \
	for godir in $(gopregeneratedirs); do \
		$(MAKE) --no-print-directory -C $$godir fmt; \
	done; \
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

pre-generate:
	@set -e; \
	for godir in $(gopregeneratedirs); do \
		$(MAKE) --no-print-directory -C $$godir build; \
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
