# Copyright (c) 2015-2021, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0


# setup up some disk

[
  "/var/lib/swift",
  "/mnt/swift-disk",
].each do |d|
  directory d do
    action :create
  end
end

execute "create sparse file" do
  command "truncate -s #{node['loopback_gb']}GB /var/lib/swift/disk"
  creates "/var/lib/swift/disk"
  action :run
end

execute "create file system" do
  command "mkfs.xfs /var/lib/swift/disk"
  not_if "xfs_admin -l /var/lib/swift/disk"
  action :run

  notifies :run, "execute[set xfs uuid]", :immediately
end

execute "set xfs uuid" do
  # the string "generate" is treated specially by xfs_admin and means
  # "make one up for me"
  command "xfs_admin -U generate /var/lib/swift/disk"
  action :nothing
end

execute "update fstab" do
  command "echo '/var/lib/swift/disk /mnt/swift-disk xfs " \
    "loop,noatime,nouuid,nodiratime,nobarrier,logbufs=8 0 0' >> /etc/fstab"
  not_if "grep swift-disk /etc/fstab"
  action :run
end

execute "mount" do
  command "mount /mnt/swift-disk"
  not_if "mountpoint /mnt/swift-disk"
end

if node['ec_policy'].empty? then
  num_disks = node['disks']
else
  num_disks = [node['disks'], node['ec_disks']].max
end

(1..num_disks).each do |i|
  j = ((i - 1) % node['nodes']) + 1
  disk_path = "/mnt/swift-disk/sdb#{i}"
  node_path = "/srv/node#{j}"
  srv_path = node_path + "/sdb#{i}"
  directory disk_path do
    owner "#{node['swift_user']}"
    group "#{node['swift_group']}"
    action :create
  end
  directory node_path do
    owner "#{node['swift_user']}"
    group "#{node['swift_group']}"
    action :create
  end
  link srv_path do
    to disk_path 
  end
end

# run dirs

[
  "/var/run/swift",
  "/var/cache/swift",
].each do |d|
  directory d do
    owner "#{node['swift_user']}"
    group "#{node['swift_group']}"
    action :create
  end
end

(1..node['nodes']).each do |i|
  recon_cache_path = "/var/cache/swift/node#{i}"
  directory recon_cache_path do
    owner "#{node['swift_user']}"
    group "#{node['swift_group']}"
    recursive true
  end
end

# make $swift_user able to read /var/log/syslog
group "adm" do
  action :modify
  members "#{node['swift_user']}"
  append true
end
