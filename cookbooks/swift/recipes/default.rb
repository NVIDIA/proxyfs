# Copyright (c) 2015-2021, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0


include_recipe "swift::setup"
include_recipe "swift::source"
include_recipe "swift::data"
include_recipe "swift::configs"
include_recipe "swift::rings"

# start main

cookbook_file "/usr/lib/systemd/system/swift.service" do
  source "usr/lib/systemd/system/swift.service"
  # notifies :restart, 'service[swift]'
  only_if { ::File.directory?("/usr/lib/systemd/system/") }
end

# Enable Swift to start at bootup on Centos
#
#
if node[:platform_family].include?("rhel")
  execute "Enable Swift to start at bootup on Centos" do
  command "/usr/bin/systemctl enable swift.service"
  end
end
  
cookbook_file "/etc/init/swift.conf" do
  source "etc/init/swift.upstart"
  # notifies :restart, 'service[swift]'
  only_if { ::File.directory?("/etc/init") }
end
  
execute "startmain" do
  command "sudo -u #{node['swift_user']} swift-init start main"
end

