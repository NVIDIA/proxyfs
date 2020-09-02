all : clone-pfs clone-meta docker-build

.PHONY: all clone-pfs clone-meta docker-build

clone-pfs:
	rm -rf pfs_middleware
	cp -R ../../pfs_middleware .

clone-meta:
	rm -rf meta_middleware
	cp -R ../../meta_middleware .

docker-build:
	docker build -t saio-pfs:latest .
