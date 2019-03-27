FROM centos:7.4.1708

# Yum install. Trying to do as much as possible at once.
# Here's an explanation of why we need to install each package:
## Install Golang
# -  gcc
# -  wget
## Install Python pip
# -  epel-release
# -  python-pip
## Setup ProxyFS build environment
# -  sudo
# -  json-c-devel
# -  fuse
## Install liberasurecode
# -  https://rpmfind.net/linux/fedora/linux/releases/28/Everything/x86_64/os/Packages/l/liberasurecode-1.5.0-5.fc28.x86_64.rpm
# -  https://rpmfind.net/linux/fedora/linux/releases/28/Everything/x86_64/os/Packages/l/liberasurecode-devel-1.5.0-5.fc28.x86_64.rpm
## Setup Samba
# -  git \
# -  gcc-c++-4.8.5-16.el7_4.2 \
# -  python-devel-2.7.5-58.el7 \
# -  gnutls-devel-3.3.26-9.el7 \
# -  libacl-devel-2.2.51-12.el7 \
# -  openldap-devel-2.4.44-5.el7 \
# -  samba-4.6.2-12.el7_4 \
# -  samba-client-4.6.2-12.el7_4 \
# -  cifs-utils-6.2-10.el7 \
## Build liberasurecode and pyeclib from source
# - gcc
# - make
# - autoconf
# - automake
# - libtool

RUN yum-config-manager --disable CentOS-Base
RUN yum-config-manager --disable CentOS-CR
RUN yum-config-manager --disable CentOS-Debuginfo
RUN yum-config-manager --disable CentOS-fasttrack
RUN yum-config-manager --disable CentOS-Media
RUN yum-config-manager --disable CentOS-Sources
RUN yum-config-manager --disable CentOS-Vault

RUN rm -rf \
    /etc/yum.repos.d/CentOS-Base.repo \
    /etc/yum.repos.d/CentOS-CR.repo \
    /etc/yum.repos.d/CentOS-Debuginfo.repo \
    /etc/yum.repos.d/CentOS-fasttrack.repo \
    /etc/yum.repos.d/CentOS-Media.repo \
    /etc/yum.repos.d/CentOS-Sources.repo \
    /etc/yum.repos.d/CentOS-Vault.repo

RUN yum-config-manager --add-repo http://vault.centos.org/centos/7.4.1708/os/x86_64/
RUN yum-config-manager --add-repo http://vault.centos.org/centos/7.4.1708/updates/x86_64/
RUN yum-config-manager --add-repo http://vault.centos.org/centos/7.4.1708/extras/x86_64/
RUN yum-config-manager --add-repo http://vault.centos.org/centos/7.4.1708/centosplus/x86_64/
RUN yum-config-manager --enable vault.centos.org_centos_7.4.1708_os_x86_64_
RUN yum-config-manager --enable vault.centos.org_centos_7.4.1708_updates_x86_64_
RUN yum-config-manager --enable vault.centos.org_centos_7.4.1708_extras_x86_64_
RUN yum-config-manager --enable vault.centos.org_centos_7.4.1708_centosplus_x86_64_

RUN yum clean all
RUN rm -rf /var/cache/yum

RUN rpm --import /etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7

RUN yum -y --disableexcludes=all install gcc
RUN yum -y install \
    wget \
    epel-release \
    sudo \
    json-c-devel \
    fuse \
    git \
    gcc-c++-4.8.5-16.el7_4.2 \
    python-devel-2.7.5-58.el7 \
    gnutls-devel-3.3.26-9.el7 \
    libacl-devel-2.2.51-12.el7 \
    openldap-devel-2.4.44-5.el7 \
    samba-4.6.2-12.el7_4 \
    samba-client-4.6.2-12.el7_4 \
    cifs-utils-6.2-10.el7 \
    gcc-4.8.5-16.el7_4.2 \
    make-3.82-23.el7 \
    autoconf-2.69-11.el7 \
    automake-1.13.4-3.el7 \
    libtool-2.4.2-22.el7_3
RUN yum -y install python-pip

# Get a py3 runtime
RUN yum -y install centos-release-scl
RUN yum -y install rh-python36 && \
    ln -s /opt/rh/rh-python36/root/bin/python3.6 /bin/python3.6 && \
    ln -s /opt/rh/rh-python36/root/usr/include /opt/rh/rh-python36/root/include

# Pip install. Trying to do as much as possible at once.
# Here's an explanation of why we need to install each package:
## Setup ProxyFS build environment
# - requests
## Install Python tox
# - tox
RUN pip install --upgrade pip
RUN pip install requests tox==3.5.3

# Install Golang
ENV GOLANG_TARFILE_NAME=go1.11.4.linux-amd64.tar.gz
RUN cd /tmp && wget -q https://dl.google.com/go/${GOLANG_TARFILE_NAME}
RUN tar -C /usr/local -xf /tmp/${GOLANG_TARFILE_NAME}
RUN rm -rf /tmp/${GOLANG_TARFILE_NAME}
ENV PATH=$PATH:/usr/local/go/bin

# Setup ProxyFS build environment
ENV GOPATH=/gopathroot
ENV PATH=$PATH:$GOPATH/bin
RUN echo "user_allow_other" >> /etc/fuse.conf

RUN git clone https://github.com/openstack/liberasurecode.git
RUN cd /liberasurecode && ./autogen.sh && ./configure && make && sudo make install && sed -i '1s/^/\/usr\/local\/lib\n/' /etc/ld.so.conf && ldconfig

RUN git clone https://github.com/openstack/pyeclib.git
RUN cd /pyeclib && pip install -e . && pip install -r test-requirements.txt

CMD ["/bin/bash", "/gopathroot/src/github.com/swiftstack/ProxyFS/test/container/launch.sh"]
