module github.com/NVIDIA/proxyfs

go 1.17

replace github.com/coreos/bbolt => go.etcd.io/bbolt v1.3.6

replace go.etcd.io/bbolt v1.3.6 => github.com/coreos/bbolt v1.3.6

replace github.com/coreos/etcd => github.com/ozonru/etcd v3.3.20-grpc1.27-origmodule+incompatible

replace google.golang.org/grpc => google.golang.org/grpc v1.27.0

require (
	bazil.org/fuse v0.0.0-20200524192727-fb710f7dfd05
	github.com/NVIDIA/cstruct v0.0.0-20210817223100-441a06a021c8
	github.com/NVIDIA/fission v0.0.0-20211223045359-ebc0c4203e7b
	github.com/NVIDIA/sortedmap v0.0.0-20210902154213-c8c741ed94c5
	github.com/ansel1/merry v1.6.1
	github.com/creachadair/cityhash v0.1.1
	github.com/google/btree v1.0.1
	github.com/sirupsen/logrus v1.8.1
	github.com/stretchr/testify v1.7.0
	go.etcd.io/etcd v3.3.27+incompatible
	golang.org/x/net v0.0.0-20211216030914-fe4d6282115f
	golang.org/x/sys v0.0.0-20211216021012-1d35b9e2eb4e
)

require (
	github.com/ansel1/merry/v2 v2.0.0-beta.10 // indirect
	github.com/coreos/bbolt v0.0.0-00010101000000-000000000000 // indirect
	github.com/coreos/etcd v0.0.0-00010101000000-000000000000 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgrijalva/jwt-go v3.2.0+incompatible // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.4.3 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.16.0 // indirect
	github.com/jonboulle/clockwork v0.2.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v1.11.0 // indirect
	github.com/soheilhy/cmux v0.1.5 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20201229170055-e5319fda7802 // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.19.1 // indirect
	golang.org/x/text v0.3.6 // indirect
	golang.org/x/time v0.0.0-20211116232009-f0f3c7e86c11 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/genproto v0.0.0-20210224155714-063164c882e6 // indirect
	google.golang.org/grpc v1.36.0 // indirect
	google.golang.org/protobuf v1.26.0-rc.1 // indirect
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
	sigs.k8s.io/yaml v1.3.0 // indirect
)
