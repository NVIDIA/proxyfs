# Copyright (c) 2015 SwiftStack, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.


execute "clean-up" do
  command "rm /home/#{node['swift_user']}/postinstall.sh || true"
end

if node['extra_key'] then
  keys_file = "~#{node['swift_user']}/.ssh/authorized_keys"
  execute "add_extra_key" do
    command "echo '#{node['extra_key']}' >> #{keys_file}"
    not_if "grep -q '#{node['extra_key']}' #{keys_file}"
  end
end

if node[:platform_family].include?("rhel")

  # TODO: make the next two commands idempotent
  # TODO: make them work for other centos versions
  execute "enable epel" do
    command "yum -y install epel-release"
    action :run
  end

  required_packages = [
    "openssl-devel", # libssl-dev for swift; is this the same thing?
    "curl", "gcc", "memcached", "rsync", "make", "wget", 
    "sqlite", # is this the same as sqlite3?
    "xfsprogs", "git-core", 
    # "build-essential",  subsumed by gcc*, libxml*, make, openssl-devel
    "python-devel", "libffi-devel", 
    "libxml2-devel", "libxml2", "libxslt", "libxslt-devel",
  ]
 
  # TODO:  confirm package names for centos for these
  # no-no packages (PIP is the bomb, system packages are OLD SKOOL)
  unrequired_packages = [
    "python-requests",  "python-six", "python-urllib3",
    "python-pbr", "python-pip",
  ]

else # debian, we assume

  # deadsnakes for py2.6
  execute "deadsnakes key" do
    command "sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys DB82666C"
    action :run
    not_if "sudo apt-key list | grep 'Launchpad Old Python Versions'"
  end

  cookbook_file "/etc/apt/sources.list.d/fkrull-deadsnakes-trusty.list" do
    source "etc/apt/sources.list.d/fkrull-deadsnakes-trusty.list"
    mode 0644
  end

  execute "enable backports" do
    command "sudo sed -ie 's/# deb http:\\/\\/archive.ubuntu.com\\/ubuntu trusty-backports/deb http:\\/\\/archive.ubuntu.com\\/ubuntu trusty-backports/' /etc/apt/sources.list"
    action :run
    not_if "sudo grep -q '^deb .* trusty-backports' /etc/apt/sources.list"
  end

  execute "apt-get-update" do
    command "apt-get update && touch /tmp/.apt-get-update"
    if not node['full_reprovision']
      creates "/tmp/.apt-get-update"
    end
    action :run
  end

  # packages
  # pythons 2.6 and 3.4 removed; unneeded for proxyfs development
  required_packages = [
    "liberasurecode-dev",  # required for the EC biz
    "libssl-dev", # libssl-dev is required for building wheels from the cryptography package in swift.
    "curl", "gcc", "memcached", "rsync", "sqlite3", "xfsprogs", "git-core", "build-essential",
    "python-dev", "libffi-dev", 
    "libxml2-dev", "libxml2", "libxslt1-dev",
  ]

  # no-no packages (PIP is the bomb, system packages are OLD SKOOL)
  unrequired_packages = [
    "python-requests",  "python-six", "python-urllib3",
    "python-pbr", "python-pip",
  ]
end

extra_packages = node['extra_packages']
(required_packages + extra_packages).each do |pkg|
  package pkg do
    action :install
  end
end

unrequired_packages.each do |pkg|
  package pkg do
    action :purge
  end
end

# TODO: figure out how to install liberasurecode from a repo
# Down here because wget must be installed first; when we get real package
# install, move it up into rhel block above
if node[:platform_family].include?("rhel")
  execute "retrieve liberasurecode" do
    command "wget http://www.rpmfind.net/linux/fedora/linux/releases/25/Everything/x86_64/os/Packages/l/liberasurecode-1.1.1-1.fc25.x86_64.rpm -O /tmp/liberasurecode.rpm"
    action :run
  end

  execute "retrieve liberasurecode-devel" do
    command "wget http://www.rpmfind.net/linux/fedora/linux/releases/25/Everything/x86_64/os/Packages/l/liberasurecode-devel-1.1.1-1.fc25.x86_64.rpm -O /tmp/liberasurecode-devel.rpm"
    action :run
  end

  # NOTE:  These are suboptimal, but we'll move to real package install soon
  execute "install liberasurecode" do
    command "rpm -ivh /tmp/liberasurecode.rpm"
    not_if "rpm -qa | grep -q 'liberasurecode-[0-9]'"
    action :run
  end

  execute "install liberasurecode-devel" do
    command "rpm -ivh /tmp/liberasurecode-devel.rpm"
    not_if "rpm -qa | grep -q 'liberasurecode-devel'"
    action :run
  end
end

# it's a brave new world
execute "install pip" do
  command "curl https://bootstrap.pypa.io/get-pip.py | python"
  not_if "which pip"
end

# pip 8.0 is more or less broken on trusty -> https://github.com/pypa/pip/issues/3384
execute "upgrade pip" do
  command "pip install --upgrade 'pip>=8.0.2'"
end

execute "fix pip warning 1" do
  command "sed '/env_reset/a Defaults\talways_set_home' -i /etc/sudoers"
  not_if "grep always_set_home /etc/sudoers"
end

execute "fix pip warning 2" do
  command "pip install --upgrade ndg-httpsclient"
end

# setup environment

execute "update-path" do
  command "echo 'export PATH=/#{node['swift_user']}/bin:$PATH' >> /home/#{node['swift_user']}/.profile"
  not_if "grep /#{node['swift_user']}/bin /home/#{node['swift_user']}/.profile"
  action :run
end

# swift command line env setup

{
  "ST_AUTH" => "http://#{node['hostname']}:8080/auth/v1.0",
  "ST_USER" => "test:tester",
  "ST_KEY" => "testing",
}.each do |var, value|
  execute "swift-env-#{var}" do
    command "echo 'export #{var}=#{value}' >> /home/#{node['swift_user']}/.profile"
    not_if "grep #{var} /home/#{node['swift_user']}/.profile"
    action :run
  end
end

ruby_block "swift2 alias" do
  block do
    dot_profile = "/home/#{node['swift_user']}/.profile"

    editor = Chef::Util::FileEdit.new(dot_profile)
    editor.insert_line_if_no_match(
      /function swift2/,
      "function swift2 () { env ST_USER=\"test2:tester2\" ST_KEY=\"testing2\" swift $* ; }")
    editor.write_file
  end
end


# other useful env vars

{
  "NOSE_INCLUDE_EXE" => "true",
}.each do |var, value|
  execute "swift-env-#{var}" do
    command "echo 'export #{var}=#{value}' >> /home/#{node['swift_user']}/.profile"
    not_if "grep #{var} /home/#{node['swift_user']}/.profile"
    action :run
  end
end
