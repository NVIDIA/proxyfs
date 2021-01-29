require 'json'

source_root = node['source_root']
proxyfs_user = node['proxyfs_user']
proxyfs_group = node['proxyfs_group']
is_dev = node['is_dev_environment']
package_spec_path = node['package_spec_path']

HOME_DIR = "/home/#{proxyfs_user}"
DOT_BASH_PROFILE = "#{HOME_DIR}/.bash_profile"
DOT_BASHRC = "#{HOME_DIR}/.bashrc"
ROOT_DOT_BASH_PROFILE = "/root/.bash_profile"
ROOT_DOT_BASHRC = "/root/.bashrc"
ETC_BASHRC = "/etc/bashrc"
REPO_CLONE_PARENT_DIR = "#{source_root}/src/github.com/swiftstack"
PROXYFS_BIN_DIR = "#{source_root}/bin"
PROXYFS_SRC_DIR = "#{REPO_CLONE_PARENT_DIR}/ProxyFS"

ruby_block "update_profile_and_bashrc" do
  block do

    unless File.exist?(DOT_BASH_PROFILE)
      File.open(DOT_BASH_PROFILE, "w") do |fh|
        # nothing to do here, just making an empty file
      end
    end

    file = Chef::Util::FileEdit.new(DOT_BASH_PROFILE)
    file.insert_line_if_no_match(/\. ~\/.bashrc/, ". ~/.bashrc")
    file.insert_line_if_no_match(/\. ~\/.profile/, "if [ -f ~/.profile ]; then . ~/.profile; fi")
    file.write_file

    unless File.exist?(ETC_BASHRC)
      File.open(ETC_BASHRC, "w") do |fh|
        # nothing to do here, just creating an empty file
      end
    end

    file = Chef::Util::FileEdit.new(ETC_BASHRC)
    file.insert_line_if_no_match(/ulimit/, "ulimit -c 0")
    file.write_file

    unless File.exist?(DOT_BASHRC)
      File.open(DOT_BASHRC, "w") do |fh|
        # nothing to do here, just creating an empty file
      end
    end

    file = Chef::Util::FileEdit.new(DOT_BASHRC)
    file.insert_line_if_no_match(/export GOPATH/, "export GOPATH=#{source_root}")
    file.insert_line_if_no_match(%r{usr/local/go/bin}, "export PATH=$GOPATH/bin:$PATH:/usr/local/go/bin")
    file.insert_line_if_no_match(/cdpfs/, "alias cdpfs='cd $GOPATH/src/github.com/swiftstack/ProxyFS'")
    file.insert_line_if_no_match(/cdfun/, "alias cdfun='cd /home/swift/code/functional-tests'")
    file.insert_line_if_no_match(/ls -lha/, "alias la='ls -lha'")
    file.insert_line_if_no_match(/ls -liha/, "alias li='ls -liha'")
    file.insert_line_if_no_match(/statmnt/, "alias statmnt='stat /mnt/*'")
    file.insert_line_if_no_match(/ST_AUTH/, "export ST_AUTH=http://localhost:8080/auth/v1.0")
    file.insert_line_if_no_match(/ST_USER/, "export ST_USER=test:tester")
    file.insert_line_if_no_match(/ST_KEY/, "export ST_KEY=testing")
    file.write_file

    unless File.exist?(ROOT_DOT_BASH_PROFILE)
      File.open(ROOT_DOT_BASH_PROFILE, "w") do |fh|
        # nothing to do here, just making an empty file
      end
    end

    file = Chef::Util::FileEdit.new(ROOT_DOT_BASH_PROFILE)
    file.insert_line_if_no_match(/\. ~\/.bashrc/, ". ~/.bashrc")
    file.insert_line_if_no_match(/\. ~\/.profile/, "if [ -f ~/.profile ]; then . ~/.profile; fi")
    file.write_file

    unless File.exist?(ROOT_DOT_BASHRC)
      File.open(ROOT_DOT_BASHRC, "w") do |fh|
        # nothing to do here, just creating an empty file
      end
    end

    file = Chef::Util::FileEdit.new(ROOT_DOT_BASHRC)
    file.insert_line_if_no_match(/export GOPATH/, "export GOPATH=#{source_root}")
    file.insert_line_if_no_match(%r{usr/local/go/bin}, "export PATH=$GOPATH/bin:$PATH:/usr/local/go/bin")
    file.insert_line_if_no_match(/cdpfs/, "alias cdpfs='cd $GOPATH/src/github.com/swiftstack/ProxyFS'")
    file.insert_line_if_no_match(/cdfun/, "alias cdfun='cd /home/swift/code/functional-tests'")
    file.insert_line_if_no_match(/ls -lha/, "alias la='ls -lha'")
    file.insert_line_if_no_match(/ls -liha/, "alias li='ls -liha'")
    file.insert_line_if_no_match(/statmnt/, "alias statmnt='stat /mnt/*'")
    file.insert_line_if_no_match(/ST_AUTH/, "export ST_AUTH=http://localhost:8080/auth/v1.0")
    file.insert_line_if_no_match(/ST_USER/, "export ST_USER=test:tester")
    file.insert_line_if_no_match(/ST_KEY/, "export ST_KEY=testing")
    file.write_file

  end
end

cookbook_file "/usr/local/go/src/runtime/runtime-gdb.py" do
  source "usr/local/go/src/runtime/runtime-gdb.py"
end

if node[:platform_family].include?("rhel")
  # Centos uses SELinux which causes Samba problems mounting.
  # Disable SeLinux.
  ruby_block "update_selinux" do
    block do
      sysconfig_selinux = "/etc/sysconfig/selinux"

      # The file will not exist if we are running in a Centos container
      # on a Ubuntu system.
      if File.file?(sysconfig_selinux)
        file = Chef::Util::FileEdit.new(sysconfig_selinux)
        file.search_file_replace(/^SELINUX=enforcing/, "SELINUX=permissive")
        file.write_file
      end
    end
  end
end

execute "Install pfs-swift-load-plot requirements" do
  command "pip install -r #{PROXYFS_SRC_DIR}/pfs-swift-load/requirements.txt"
end

execute "Create ProxyFS/bin dir" do
  command "mkdir #{PROXYFS_BIN_DIR}"
  not_if { ::Dir.exists?("#{PROXYFS_BIN_DIR}") }
end

execute "Copy pfs-swift-load-plot at /home/swift/code/ProxyFS/bin/" do
  command "install -m 0755 #{PROXYFS_SRC_DIR}/pfs-swift-load/pfs-swift-load-plot #{PROXYFS_BIN_DIR}/"
end

# TODO: install aws cli v2
execute "Install awscli and awscli-plugin-endpoint" do
  command "pip install awscli awscli-plugin-endpoint"
end

if is_dev
  ruby_block "fuse_user_allow_other" do
    block do
      file = Chef::Util::FileEdit.new("/etc/fuse.conf")
      file.search_file_delete_line(/#user_allow_other/)
      file.insert_line_if_no_match(/^user_allow_other/, "user_allow_other")
      file.write_file
    end
  end

  file "/etc/fuse.conf" do
    mode '0644' # globally readable
  end
end

directory '/CommonMountPoint' do
  # perms/owner don't really matter since it gets mounted over, but
  # this helps stop a developer from accidentally dumping stuff on the
  # root filesystem
  owner 'root'
end

directory '/var/lib/proxyfs' do
  mode '0755'
  owner proxyfs_user
  group proxyfs_group
end

directory '/var/log/proxyfsd' do
  mode '0755'
  owner  proxyfs_user
  group  proxyfs_group
end

link '/etc/proxyfsd' do
  to "#{source_root}/src/github.com/swiftstack/ProxyFS/proxyfsd/"
  link_type :symbolic
  owner proxyfs_user
  group proxyfs_group
end

link '/etc/pfsagentd' do
  to "#{source_root}/src/github.com/swiftstack/ProxyFS/pfsagentd/"
  link_type :symbolic
  owner proxyfs_user
  group proxyfs_group
end

execute "Provision start_and_mount_pfs" do
  command "install -m 0755 #{source_root}/src/github.com/swiftstack/ProxyFS/cookbooks/proxyfs/files/default/usr/bin/start_and_mount_pfs /usr/bin"
end

execute "Provision start_swift_only" do
  command "install -m 0755 #{source_root}/src/github.com/swiftstack/ProxyFS/cookbooks/proxyfs/files/default/usr/bin/start_swift_only /usr/bin"
end

execute "Provision start_proxyfsd_only" do
  command "install -m 0755 #{source_root}/src/github.com/swiftstack/ProxyFS/cookbooks/proxyfs/files/default/usr/bin/start_proxyfsd_only /usr/bin"
end

execute "Provision stop_proxyfsd_only" do
  command "install -m 0755 #{source_root}/src/github.com/swiftstack/ProxyFS/cookbooks/proxyfs/files/default/usr/bin/stop_proxyfsd_only /usr/bin"
end

execute "Provision unmount_and_stop_pfs" do
  command "install -m 0755 #{source_root}/src/github.com/swiftstack/ProxyFS/cookbooks/proxyfs/files/default/usr/bin/unmount_and_stop_pfs /usr/bin"
end

execute "Provision set_up_s3api" do
  command "install -m 0755 #{source_root}/src/github.com/swiftstack/ProxyFS/cookbooks/proxyfs/files/default/usr/bin/set_up_s3api /usr/bin"
end

execute "Provision set_up_swift3" do
  command "install -m 0755 #{source_root}/src/github.com/swiftstack/ProxyFS/cookbooks/proxyfs/files/default/usr/bin/set_up_swift3 /usr/bin"
end

execute "Provision enable_s3" do
  command "install -m 0755 #{source_root}/src/github.com/swiftstack/ProxyFS/cookbooks/proxyfs/files/default/usr/bin/enable_s3 /usr/bin"
end

execute "Provision disable_s3" do
  command "install -m 0755 #{source_root}/src/github.com/swiftstack/ProxyFS/cookbooks/proxyfs/files/default/usr/bin/disable_s3 /usr/bin"
end

execute "Provision detect_s3" do
  command "install -m 0755 #{source_root}/src/github.com/swiftstack/ProxyFS/cookbooks/proxyfs/files/default/usr/bin/detect_s3 /usr/bin"
end

execute "Provision pfs_stat" do
  command "install -m 0755 #{source_root}/src/github.com/swiftstack/ProxyFS/bin/pfs_stat /usr/bin"
end

cookbook_file "/usr/lib/systemd/system/proxyfsd.service" do
  source "usr/lib/systemd/system/proxyfsd.service"
  # notifies :restart, 'service[proxyfsd]'
  only_if { ::File.directory?("/usr/lib/systemd/system/") }
end

cookbook_file "/usr/lib/systemd/system/pfsagentd.service" do
  source "usr/lib/systemd/system/pfsagentd.service"
  only_if { ::File.directory?("/usr/lib/systemd/system/") }
end

cookbook_file "/etc/init/proxyfsd.conf" do
  source "etc/init/proxyfsd.upstart"
  # notifies :restart, 'service[proxyfsd]'
  only_if { ::File.directory?("/etc/init") }
end


#
# Dependency lists by OS
#
if node[:platform_family].include?("rhel")
  package_spec_file_path = File.read(package_spec_path + '/rhel.json')
else # assume debian
    package_spec_file_path = File.read(package_spec_path + '/debian.json')
end

package_spec = JSON.parse(package_spec_file_path)
packages = package_spec['proxyfs_packages'] + package_spec['gdb_packages'] + package_spec['utils_packages']
packages += package_spec['wireshark_packages'] if is_dev
packages += package_spec['ssh_packages'] if is_dev

packages.each do |pkg|
  if pkg.size >= 2
    # Specify a version if it's been provided
    package pkg[0] do
      action :install
      version pkg[1]
    end
  else
    # Just install whatever YUM provides otherwise
    package pkg[0] do
      action :install
    end
  end
end

if is_dev
  group 'wireshark' do
    action :create
    members [proxyfs_user]
  end

  file '/usr/bin/dumpcap' do
    group 'wireshark'
  end

  execute 'setcap' do
    command "setcap cap_net_raw,cap_net_admin=eip /usr/bin/dumpcap"
  end
end

#
# Create mount point and fstab entry
#
execute "Create PFSAgent mount point" do
  command "mkdir /mnt/pfsa_proxyfs_mount"
  not_if { ::Dir.exists?("/mnt/pfsa_proxyfs_mount") }
end

ruby_block "Create exports entry" do
  block do
    unless File.exist?("/etc/exports")
      File.open("/etc/exports", "w") do |fh|
        # nothing to do here, just making an empty file
      end
    end

    editor = Chef::Util::FileEdit.new("/etc/exports")
    editor.insert_line_if_no_match("CommonMountPoint", "/CommonMountPoint 127.0.0.1(rw,sync,fsid=1000,no_subtree_check,no_root_squash)")
    editor.write_file
  end
end

#
# Enable user processes to do FUSE stuff
#
bash 'Enable UserMode FUSE' do
  code <<-EOH
  chmod +x /bin/fusermount
  echo "user_allow_other" > /etc/fuse.conf
  EOH
end

#
# Build and install proxyfs
#

# TODO:  this not_if is incorrect, especially now that the same source tree can
# target centos or ubuntu.  We should check for the existence of the link
# below at the very least.
bash 'Build proxyfsd' do
  # Source profile because we may not have golang in our path yet
  code <<-EOH
  . #{DOT_BASH_PROFILE}
  make clean minimal
  EOH
  cwd PROXYFS_SRC_DIR
end

## TODO: If this link/file does not exist, we should rebuild everything
## TODO: do this as an install instead, for non dev environments?
link '/usr/bin/proxyfsd' do
  to "#{source_root}/bin/proxyfsd"
  link_type :symbolic
  owner proxyfs_user
  group proxyfs_group
end

link '/usr/bin/pfsagentd' do
  to "#{source_root}/bin/pfsagentd"
  link_type :symbolic
  owner proxyfs_user
  group proxyfs_group
end

link '/usr/bin/pfsagentd-swift-auth-plugin' do
  to "#{source_root}/bin/pfsagentd-swift-auth-plugin"
  link_type :symbolic
  owner proxyfs_user
  group proxyfs_group
end

cookbook_file "#{HOME_DIR}/.gdbinit" do
  source "home/unprivileged_user/.gdbinit"
  owner "#{proxyfs_user}"
  group "#{proxyfs_group}"
end

template "/root/.gdbinit" do
  source "root/.gdbinit.erb"
  owner "root"
  group "root"
  variables({
    :proxyfs_user => "#{proxyfs_user}"
  })
end

bash 'Configure awscli for swift user' do
    code <<-EOH
    mkdir ~swift/.aws
    cat > ~swift/.aws/config << EOF
[plugins]
endpoint = awscli_plugin_endpoint

[default]
s3 =
     endpoint_url = http://127.0.0.1:8080
     multipart_threshold = 64MB
     multipart_chunksize = 16MB
s3api =
     endpoint_url = http://127.0.0.1:8080
     multipart_threshold = 64MB
     multipart_chunksize = 16MB

[profile nfspfs]
s3 =
     endpoint_url = http://127.0.0.1:8080
     multipart_threshold = 64MB
     multipart_chunksize = 16MB
s3api =
     endpoint_url = http://127.0.0.1:8080
     multipart_threshold = 64MB
     multipart_chunksize = 16MB

[profile smbpfs]
s3 =
     endpoint_url = http://127.0.0.1:8080
     multipart_threshold = 64MB
     multipart_chunksize = 16MB
s3api =
     endpoint_url = http://127.0.0.1:8080
     multipart_threshold = 64MB
     multipart_chunksize = 16MB

[profile pfsapfs]
s3 =
     endpoint_url = http://127.0.0.1:8080
     multipart_threshold = 64MB
     multipart_chunksize = 16MB
s3api =
     endpoint_url = http://127.0.0.1:8080
     multipart_threshold = 64MB
     multipart_chunksize = 16MB
EOF
    cat > ~swift/.aws/credentials << EOF
[default]
aws_access_key_id = test:tester
aws_secret_access_key = testing

[nfspfs]
aws_access_key_id = test:tester
aws_secret_access_key = testing

[smbpfs]
aws_access_key_id = test:tester
aws_secret_access_key = testing

[pfsapfs]
aws_access_key_id = test:tester
aws_secret_access_key = testing
EOF
    chown -R swift:swift ~swift/.aws
    EOH
end

bash 'Configure awscli for root user' do
    code <<-EOH
    mkdir ~root/.aws
    cat > ~root/.aws/config << EOF
[plugins]
endpoint = awscli_plugin_endpoint

[default]
s3 =
     endpoint_url = http://127.0.0.1:8080
     multipart_threshold = 64MB
     multipart_chunksize = 16MB
s3api =
     endpoint_url = http://127.0.0.1:8080
     multipart_threshold = 64MB
     multipart_chunksize = 16MB

[profile nfspfs]
s3 =
     endpoint_url = http://127.0.0.1:8080
     multipart_threshold = 64MB
     multipart_chunksize = 16MB
s3api =
     endpoint_url = http://127.0.0.1:8080
     multipart_threshold = 64MB
     multipart_chunksize = 16MB

[profile smbpfs]
s3 =
     endpoint_url = http://127.0.0.1:8080
     multipart_threshold = 64MB
     multipart_chunksize = 16MB
s3api =
     endpoint_url = http://127.0.0.1:8080
     multipart_threshold = 64MB
     multipart_chunksize = 16MB

[profile pfsapfs]
s3 =
     endpoint_url = http://127.0.0.1:8080
     multipart_threshold = 64MB
     multipart_chunksize = 16MB
s3api =
     endpoint_url = http://127.0.0.1:8080
     multipart_threshold = 64MB
     multipart_chunksize = 16MB
EOF
    cat > ~root/.aws/credentials << EOF
[default]
aws_access_key_id = test:tester
aws_secret_access_key = testing

[nfspfs]
aws_access_key_id = test:tester
aws_secret_access_key = testing

[smbpfs]
aws_access_key_id = test:tester
aws_secret_access_key = testing

[pfsapfs]
aws_access_key_id = test:tester
aws_secret_access_key = testing
EOF
    chown -R root:root ~root/.aws
    EOH
end

execute 'set up s3api' do
  command "/usr/bin/set_up_s3api"
end

execute 'set up swift3' do
  command "/usr/bin/set_up_swift3"
end

execute 'enable s3api' do
  command "/usr/bin/enable_s3 s3api"
end
