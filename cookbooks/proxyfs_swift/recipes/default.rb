# Copyright (c) 2015-2021, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# rsync

rsync_service = "rsync"
if node[:platform_family].include?("rhel")
  rsync_service = "rsyncd"
end

template "/etc/rsyncd.conf" do
  source "etc/rsyncd.conf.erb"
  notifies :restart, "service[#{rsync_service}]"
  variables({
    :swift_user => node['swift_user'],
    :swift_group => node['swift_group'],
  })
end

# TODO:  I suppose this should be a file rather than a template
template "/etc/default/#{rsync_service}" do
  source "etc/default/#{rsync_service}.erb"
  notifies :restart, "service[#{rsync_service}]"
  not_if { ::File.exist?("/etc/default/#{rsync_service}") }
  variables({
  })
end

execute "enable-rsync" do
  command "sed -i 's/ENABLE=false/ENABLE=true/' /etc/default/#{rsync_service}"
  not_if "grep ENABLE=true /etc/default/#{rsync_service}"
  action :run
end

[
  "#{rsync_service}",
  "memcached",
  "rsyslog",
].each do |daemon|
  service daemon do
    action :start
  end
end

ruby_block "update_proxy_server_pipeline" do
  block do
    proxy_server_conf = "/etc/swift/proxy-server.conf"

    if File.file?(proxy_server_conf)
      file = Chef::Util::FileEdit.new(proxy_server_conf)
      file.search_file_replace(/^( *pipeline\s*=.*?)\s+dlo\s+(?:pfs\s+)?(.*)$/, '\1 dlo pfs \2')
      file.insert_line_if_no_match(/filter.pfs/, "")
      file.insert_line_if_no_match(/filter.pfs/, "[filter:pfs]")
      file.insert_line_if_no_match(/egg.pfs_middleware/, "use = egg:pfs_middleware#pfs")
      file.insert_line_if_no_match(/proxyfsd_host/, "proxyfsd_host = 127.0.0.1")
      file.insert_line_if_no_match(/proxyfsd_port/, "proxyfsd_port = 12345")
      file.insert_line_if_no_match(/bypass_mode/, "bypass_mode = read-write")
      file.write_file
    end
  end
end

proxy_conf_dir = "etc/swift/proxy-server/proxy-noauth.conf.d"
%w[ etc/swift/proxy-server etc/swift/proxy-server/proxy-noauth.conf.d ].each do |path|
  directory path do
    owner "#{node['swift_user']}"
    group "#{node['swift_group']}"
    action :create
  end
end
template "/#{proxy_conf_dir}/00_base.conf" do
  source "etc/swift/base.conf-template.erb"
  owner "#{node['swift_user']}"
  group "#{node['swift_group']}"
  variables({
    :swift_user => node['swift_user'],
    :swift_group => node['swift_group'],
  })
end
template "/#{proxy_conf_dir}/10_default.conf" do
  source "etc/swift/proxy-server/default.conf-template.erb"
  owner "#{node['swift_user']}"
  group "#{node['swift_group']}"
  variables({
    :post_as_copy => node['post_as_copy'],
  })
end
cookbook_file "#{proxy_conf_dir}/20_settings.conf" do
  source "#{proxy_conf_dir}/20_settings.conf"
  owner "#{node['swift_user']}"
  group "#{node['swift_group']}"
end

ruby_block "update_noauth_proxy_server_pipeline" do
  block do
    noauth_proxy_server_conf = "/etc/swift/proxy-server/proxy-noauth.conf.d/20_settings.conf"

    if File.file?(noauth_proxy_server_conf)
      file = Chef::Util::FileEdit.new(noauth_proxy_server_conf)
      file.search_file_replace(/ dlo /, " dlo meta ")
      file.insert_line_if_no_match(/filter.meta/, "")
      file.insert_line_if_no_match(/filter.meta/, "[filter:meta]")
      file.insert_line_if_no_match(/egg.meta_middleware/, "use = egg:meta_middleware#meta")
      file.write_file
    end
  end
end

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
  command "swift-init start main"
end
