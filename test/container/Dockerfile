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
# -  http://www.rpmfind.net/linux/fedora/linux/releases/25/Everything/x86_64/os/Packages/l/liberasurecode-1.1.1-1.fc25.x86_64.rpm
# -  http://www.rpmfind.net/linux/fedora/linux/releases/25/Everything/x86_64/os/Packages/l/liberasurecode-devel-1.1.1-1.fc25.x86_64.rpm
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
RUN yum -y --disableexcludes=all install gcc
RUN yum -y install \
    wget \
    epel-release \
    sudo \
    json-c-devel \
    fuse \
    http://www.rpmfind.net/linux/fedora/linux/releases/25/Everything/x86_64/os/Packages/l/liberasurecode-1.1.1-1.fc25.x86_64.rpm \
    http://www.rpmfind.net/linux/fedora/linux/releases/25/Everything/x86_64/os/Packages/l/liberasurecode-devel-1.1.1-1.fc25.x86_64.rpm \
    git \
    gcc-c++-4.8.5-16.el7_4.2 \
    python-devel-2.7.5-58.el7 \
    gnutls-devel-3.3.26-9.el7 \
    libacl-devel-2.2.51-12.el7 \
    openldap-devel-2.4.44-5.el7 \
    samba-4.6.2-12.el7_4 \
    samba-client-4.6.2-12.el7_4 \
    cifs-utils-6.2-10.el7
RUN yum -y install python-pip

# Pip install. Trying to do as much as possible at once.
# Here's an explanation of why we need to install each package:
## Setup ProxyFS build environment
# - requests
## Install Python tox
# - tox
RUN pip install --upgrade pip
RUN pip install requests tox

# Install Golang
RUN cd /tmp
RUN wget -q https://dl.google.com/go/go1.10.linux-amd64.tar.gz
RUN tar -C /usr/local -xf go1.10.linux-amd64.tar.gz
RUN rm -rf go1.10.linux-amd64.tar.gz
ENV PATH=$PATH:/usr/local/go/bin

# Setup ProxyFS build environment
ENV GOPATH=/gopathroot
ENV PATH=$PATH:$GOPATH/bin
RUN echo "user_allow_other" >> /etc/fuse.conf

CMD ["/bin/bash", "/gopathroot/src/github.com/swiftstack/ProxyFS/test/container/launch.sh"]
