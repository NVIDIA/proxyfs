FROM centos
RUN yum install -y fuse gcc git make python2 tar wget
RUN ln -s /usr/bin/python2 /usr/bin/python
WORKDIR /opt/PFSAgent
RUN wget -nv https://golang.org/dl/go1.15.linux-amd64.tar.gz
RUN tar -C /usr/local -xzf go1.15.linux-amd64.tar.gz
ENV GOPATH /opt/PFSAgent/GOPATH
ENV PATH   $PATH:/usr/local/go/bin:$GOPATH/bin
RUN mkdir -p $GOPATH/src/github.com/swiftstack
WORKDIR $GOPATH/src/github.com/swiftstack
RUN git clone https://github.com/swiftstack/ProxyFS.git
WORKDIR $GOPATH/src/github.com/swiftstack/ProxyFS
RUN make version pfsagent
WORKDIR /opt/PFSAgent/fake_usr_local
RUN ln $GOPATH/bin/pfsagentd .
RUN ln $GOPATH/bin/pfsagentd-swift-auth-plugin  .
RUN ln $GOPATH/src/github.com/swiftstack/ProxyFS/pfsagentd/pfsagent.conf .
RUN tar -czf ../pfsagent.tar.gz *
WORKDIR /opt/PFSAgent
RUN mkdir AgentMountPoint
COPY pfsagent.conf .
