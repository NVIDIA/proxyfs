FROM centos:7.4.1708

RUN yum install -y yum-utils

RUN yum-config-manager --disable CentOS-Base
RUN yum-config-manager --disable CentOS-CR
RUN yum-config-manager --disable CentOS-Debuginfo
RUN yum-config-manager --disable CentOS-fasttrack
RUN yum-config-manager --disable CentOS-Media
RUN yum-config-manager --disable CentOS-Sources
RUN yum-config-manager --disable CentOS-Vault

RUN rm -rf /etc/yum.repos.d/CentOS-Base.repo
RUN rm -rf /etc/yum.repos.d/CentOS-CR.repo
RUN rm -rf /etc/yum.repos.d/CentOS-Debuginfo.repo
RUN rm -rf /etc/yum.repos.d/CentOS-fasttrack.repo
RUN rm -rf /etc/yum.repos.d/CentOS-Media.repo
RUN rm -rf /etc/yum.repos.d/CentOS-Sources.repo
RUN rm -rf /etc/yum.repos.d/CentOS-Vault.repo

RUN yum-config-manager --add-repo http://vault.centos.org/centos/7.4.1708/os/x86_64/
RUN yum-config-manager --add-repo http://vault.centos.org/centos/7.4.1708/updates/x86_64/
RUN yum-config-manager --add-repo http://vault.centos.org/centos/7.4.1708/extras/x86_64/
RUN yum-config-manager --add-repo http://vault.centos.org/centos/7.4.1708/centosplus/x86_64/
RUN yum-config-manager --enable vault.centos.org_centos_7.4.1708_os_x86_64_
RUN yum-config-manager --enable vault.centos.org_centos_7.4.1708_updates_x86_64_
RUN yum-config-manager --enable vault.centos.org_centos_7.4.1708_extras_x86_64_
RUN yum-config-manager --enable vault.centos.org_centos_7.4.1708_centosplus_x86_64_

RUN yum clean all

RUN yum install -y wget
RUN yum install -y git

RUN yum install -y --disableexcludes=all gcc

RUN useradd --user-group --groups wheel swift
RUN chmod 755 ~swift

RUN yum install -y attr
RUN yum install -y e2fsprogs

RUN mkdir -p /srv
RUN truncate -s 1GB /srv/swift-disk
RUN echo "y" | mkfs.ext4 -I 4096 /srv/swift-disk
RUN mkdir -p /srv/1/node/sdb1
RUN echo "/srv/swift-disk /srv/1/node/sdb1 ext4 loop,noatime,nodiratime,nobarrier 0 0" >> /etc/fstab

RUN mkdir -p /var/cache/swift /var/cache/swift2 /var/cache/swift3 /var/cache/swift4
RUN chown swift:swift /var/cache/swift*
RUN mkdir -p /var/run/swift
RUN chown swift:swift /var/run/swift

RUN yum install -y \
    memcached \
    sqlite \
    xfsprogs \
    libffi-devel \
    xinetd \
    openssl-devel \
    python-setuptools \
    python-coverage \
    python-devel \
    python-nose \
    pyxattr \
    python-eventlet \
    python-greenlet \
    python-paste-deploy \
    python-netifaces \
    python-dns \
    python-mock

RUN yum install -y centos-release-scl
RUN yum install -y rh-python36
RUN ln -s /opt/rh/rh-python36/root/bin/python3.6 /bin/python3.6
RUN ln -s /bin/python3.6 /bin/python3
RUN ln -s /opt/rh/rh-python36/root/usr/include /opt/rh/rh-python36/root/include

RUN yum install -y epel-release
RUN yum install -y python-pip
RUN pip install --upgrade pip
RUN pip install --upgrade setuptools

RUN yum install -y https://rpmfind.net/linux/fedora/linux/releases/30/Everything/x86_64/os/Packages/l/liberasurecode-1.6.0-3.fc30.x86_64.rpm
RUN yum install -y https://rpmfind.net/linux/fedora/linux/releases/30/Everything/x86_64/os/Packages/l/liberasurecode-devel-1.6.0-3.fc30.x86_64.rpm

WORKDIR ~swift
RUN git clone https://github.com/swiftstack/swift.git
WORKDIR swift
RUN git checkout ss-release-2.25.0.7
RUN pip install wheel
RUN python setup.py bdist_wheel
RUN pip install --no-binary cryptography -r requirements.txt
RUN python setup.py develop
RUN pip install python-openstackclient==3.12.0 python-glanceclient==2.7.0
RUN pip install -r test-requirements.txt

# TODO: May need to do something about:
#   systemctl enable memcached.service
#   systemctl start memcached.service

WORKDIR /etc/swift

RUN rm -f *.builder *.ring.gz backups/*.builder backups/*.ring.gz
RUN swift-ring-builder object.builder create 10 1 1
RUN swift-ring-builder object.builder add r1z1-127.0.0.1:8010/sdb1 1
RUN swift-ring-builder object.builder rebalance
RUN swift-ring-builder object-1.builder create 10 1 1
RUN swift-ring-builder object-1.builder add r1z1-127.0.0.1:8010/sdb1 1
RUN swift-ring-builder object-1.builder rebalance
RUN swift-ring-builder object-2.builder create 10 1 1
RUN swift-ring-builder object-2.builder add r1z1-127.0.0.1:8010/sdb1 1
RUN swift-ring-builder object-2.builder rebalance
RUN swift-ring-builder container.builder create 10 1 1
RUN swift-ring-builder container.builder add r1z1-127.0.0.1:8011/sdb1 1
RUN swift-ring-builder container.builder rebalance
RUN swift-ring-builder account.builder create 10 1 1
RUN swift-ring-builder account.builder add r1z1-127.0.0.1:8012/sdb1 1
RUN swift-ring-builder account.builder rebalance

COPY etc/swift .
RUN chown -R swift:swift .

WORKDIR /opt/ProxyFS

COPY pfs_middleware  pfs_middleware
COPY meta_middleware meta_middleware

RUN cd pfs_middleware  && python setup.py develop
RUN cd meta_middleware && python setup.py develop

RUN rm -rf /var/log/proxyfsd
RUN mkdir -p /var/log/proxyfsd
RUN touch /var/log/proxyfsd/proxyfsd.log
RUN chmod 777 /var
RUN chmod 777 /var/log
RUN chmod 777 /var/log/proxyfsd
RUN chmod 666 /var/log/proxyfsd/proxyfsd.log

WORKDIR /opt/ProxyFS
RUN wget -nv https://golang.org/dl/go1.13.6.linux-amd64.tar.gz
RUN tar -C /usr/local -xzf go1.13.6.linux-amd64.tar.gz

ENV GOPATH /opt/ProxyFS/GOPATH
ENV PATH   $PATH:/usr/local/go/bin:$GOPATH/bin
RUN mkdir -p $GOPATH/src/github.com/swiftstack
WORKDIR $GOPATH/src/github.com/swiftstack
RUN git clone https://github.com/swiftstack/ProxyFS.git

RUN yum install -y make fuse

WORKDIR $GOPATH/src/github.com/swiftstack/ProxyFS
RUN make version pre-generate generate install

WORKDIR /opt/ProxyFS

RUN mkdir     CommonMountPoint
RUN chmod 777 CommonMountPoint

RUN yum install -y rsyslog
RUN echo "\$ModLoad imudp" >> /etc/rsyslog.conf
RUN echo "\$UDPServerRun 514" >> /etc/rsyslog.conf

COPY docker_startup.sh .
COPY proxyfs.conf .
RUN chmod +x docker_startup.sh

CMD /opt/ProxyFS/docker_startup.sh

# To run a container for this image:
#
#   docker run -it --rm --device /dev/fuse --device /dev/loop0 --cap-add SYS_ADMIN -p 15346:15346 saio-pfs
