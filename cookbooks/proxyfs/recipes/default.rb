require 'json'

golang_tarfile_name = 'go1.11.4.linux-amd64.tar.gz'

golang_tarfile_path = "/tmp/#{golang_tarfile_name}"
golang_tarfile_url  = "https://dl.google.com/go/#{golang_tarfile_name}"

source_root = node['source_root']
proxyfs_user = node['proxyfs_user']
proxyfs_group = node['proxyfs_group']
is_dev = node['is_dev_environment']
ss_packages = node['use_swiftstack_packages']
package_spec_path = node['package_spec_path']

GOROOT = "/usr/local/go"
HOME_DIR = "/home/#{proxyfs_user}"
DOT_BASH_PROFILE = "#{HOME_DIR}/.bash_profile"
DOT_BASHRC = "#{HOME_DIR}/.bashrc"
ROOT_DOT_BASH_PROFILE = "/root/.bash_profile"
ROOT_DOT_BASHRC = "/root/.bashrc"
REPO_CLONE_PARENT_DIR = "#{source_root}/src/github.com/swiftstack"
PROXYFS_SRC_DIR = "#{REPO_CLONE_PARENT_DIR}/ProxyFS"
VFS_SRC_DIR = "#{PROXYFS_SRC_DIR}/vfs"
JRPCCLIENT_SRC_DIR = "#{PROXYFS_SRC_DIR}/jrpcclient"
# We're doing this to only need to change SAMBA_PARENT_DIR in case we decide to
# change the location of samba again in the future.
SAMBA_PARENT_DIR = "#{VFS_SRC_DIR}"
SAMBA_SRC_DIR = "#{SAMBA_PARENT_DIR}/samba"

remote_file "#{golang_tarfile_path}" do
  source "#{golang_tarfile_url}"
  owner 'root'
  group 'root'
  mode '0400'
  action :create
  not_if { ::File.exists?(GOROOT) }
end

execute 'untar_golang' do
  command "tar -C /usr/local -xzf #{golang_tarfile_path}"
  not_if { ::File.exists?(GOROOT) }
end

file "/etc/profile.d/golang_path.sh" do
  content "export PATH=$PATH:#{GOROOT}/bin"
  mode '0644'
end

file "#{golang_tarfile_path}" do
  action :delete
end

if node[:platform_family].include?("rhel") and ss_packages
  cookbook_file "/etc/yum.repos.d/swiftstack-controller.repo" do
    source "etc/yum.repos.d/swiftstack-controller.repo"
    owner "root"
    group "root"
  end

  cookbook_file "/etc/pki/rpm-gpg/RPM-GPG-KEY-swiftstack-controller" do
    source "etc/pki/rpm-gpg/RPM-GPG-KEY-swiftstack-controller"
    owner "root"
    group "root"
  end

  execute "yum makecache" do
    command "yum makecache"
  end
end

ruby_block "update_profile_and_bashrc" do
  block do

    unless File.exist?(DOT_BASH_PROFILE)
      File.open(DOT_BASH_PROFILE, "w") do |fh|
        # nothing to do here, just making an empty file
      end
    end

    file = Chef::Util::FileEdit.new(DOT_BASH_PROFILE)
    file.insert_line_if_no_match(/ulimit/, "ulimit -c unlimited")
    file.insert_line_if_no_match(/\. ~\/.bashrc/, ". ~/.bashrc")
    file.insert_line_if_no_match(/\. ~\/.profile/, "if [ -f ~/.profile ]; then . ~/.profile; fi")
    file.write_file

    unless File.exist?(DOT_BASHRC)
      File.open(DOT_BASHRC, "w") do |fh|
        # nothing to do here, just making an empty file
      end
    end

    file = Chef::Util::FileEdit.new(DOT_BASHRC)
    file.insert_line_if_no_match(/export GOPATH/, "export GOPATH=#{source_root}")
    if ss_packages
      file.insert_line_if_no_match(%r{usr/local/go/bin}, "export PATH=$GOPATH/bin:$PATH:/usr/local/go/bin:/opt/ss/bin:/opt/ss/sbin")
    else
      file.insert_line_if_no_match(%r{usr/local/go/bin}, "export PATH=$GOPATH/bin:$PATH:/usr/local/go/bin")
    end
    file.insert_line_if_no_match(/cdpfs/, "alias cdpfs='cd $GOPATH/src/github.com/swiftstack/ProxyFS'")
    file.insert_line_if_no_match(/cdsamba/, "alias cdsamba='cd #{SAMBA_SRC_DIR}'")
    file.insert_line_if_no_match(/ls -lha/, "alias la='ls -lha'")
    file.insert_line_if_no_match(/ls -liha/, "alias li='ls -liha'")
    file.insert_line_if_no_match(/statmnt/, "alias statmnt='stat /mnt/*'")
    file.write_file

    unless File.exist?(ROOT_DOT_BASH_PROFILE)
      File.open(ROOT_DOT_BASH_PROFILE, "w") do |fh|
        # nothing to do here, just making an empty file
      end
    end

    file = Chef::Util::FileEdit.new(ROOT_DOT_BASH_PROFILE)
    file.insert_line_if_no_match(/ulimit/, "ulimit -c unlimited")
    file.insert_line_if_no_match(/\. ~\/.bashrc/, ". ~/.bashrc")
    file.insert_line_if_no_match(/\. ~\/.profile/, "if [ -f ~/.profile ]; then . ~/.profile; fi")
    file.write_file

    unless File.exist?(ROOT_DOT_BASHRC)
      File.open(ROOT_DOT_BASHRC, "w") do |fh|
        # nothing to do here, just making an empty file
      end
    end

    file = Chef::Util::FileEdit.new(ROOT_DOT_BASHRC)
    file.insert_line_if_no_match(/export GOPATH/, "export GOPATH=#{source_root}")
    if ss_packages
      file.insert_line_if_no_match(%r{usr/local/go/bin}, "export PATH=$GOPATH/bin:$PATH:/usr/local/go/bin:/opt/ss/bin:/opt/ss/sbin")
    else
      file.insert_line_if_no_match(%r{usr/local/go/bin}, "export PATH=$GOPATH/bin:$PATH:/usr/local/go/bin")
    end
    file.insert_line_if_no_match(/cdpfs/, "alias cdpfs='cd $GOPATH/src/github.com/swiftstack/ProxyFS'")
    file.insert_line_if_no_match(/cdsamba/, "alias cdsamba='cd #{SAMBA_SRC_DIR}'")
    file.insert_line_if_no_match(/ls -lha/, "alias la='ls -lha'")
    file.insert_line_if_no_match(/ls -liha/, "alias li='ls -liha'")
    file.insert_line_if_no_match(/statmnt/, "alias statmnt='stat /mnt/*'")
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

execute "Install PIP" do
  command "curl https://bootstrap.pypa.io/get-pip.py | python"
  not_if { system("pip --version") }
end

execute "Install pfs-swift-load-plot requirements" do
  command "pip install -r #{PROXYFS_SRC_DIR}/pfs-swift-load/requirements.txt"
end

execute "Copy pfs-swift-load-plot at /home/swift/code/ProxyFS/bin/" do
  command "install -m 0755 #{PROXYFS_SRC_DIR}/pfs-swift-load/pfs-swift-load-plot #{PROXYFS_SRC_DIR}/bin"
end

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

template "/usr/bin/start_and_mount_pfs" do
  mode '0755'
  source "usr/bin/start_and_mount_pfs.erb"
  variables({
    :swift_user => node['swift_user'],
    :swift_uid => node['swift_uid'],
    :swift_gid => node['swift_gid']
  })
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
  # Do we want to allow re-provisioning to overwrite the pfs_stat script in case it has changes?
  # not_if { ::File.exists?("/usr/bin/pfs_stat") }
end

cookbook_file "/usr/lib/systemd/system/proxyfsd.service" do
  source "usr/lib/systemd/system/proxyfsd.service"
  # notifies :restart, 'service[proxyfsd]'
  only_if { ::File.directory?("/usr/lib/systemd/system/") }
end

if node[:platform_family].include?("rhel") and ss_packages
  cookbook_file "/usr/lib/systemd/system/smb.service" do
    source "usr/lib/systemd/system/smb.service"
    # notifies :restart, 'service[smb]'
    only_if { ::File.directory?("/usr/lib/systemd/system/") }
  end

  cookbook_file "/usr/lib/systemd/system/nmb.service" do
    source "usr/lib/systemd/system/nmb.service"
    # notifies :restart, 'service[nmb]'
    only_if { ::File.directory?("/usr/lib/systemd/system/") }
  end
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
  if ss_packages
    package_spec_file_path = File.read(package_spec_path + '/rhel_ss.json')
  else
    package_spec_file_path = File.read(package_spec_path + '/rhel.json')
  end
else # assume debian
    package_spec_file_path = File.read(package_spec_path + '/debian.json')
end

package_spec = JSON.parse(package_spec_file_path)
packages = package_spec['samba_packages'] + package_spec['samba_deps'] + package_spec['proxyfs_packages'] + package_spec['nfs_packages'] + package_spec['gdb_packages'] + package_spec['utils_packages']
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
# Always remake the samba symbolic link since the user may be switching between
# Centos and Ubuntu
#
execute "Remove samba symbolic link" do
  command "rm -f samba"
  cwd SAMBA_PARENT_DIR
end


#
# Check out and build samba
#
# For now we're hard-coding the OS. It should be parametrized.
OS_DISTRO="centos"
OS_DISTRO_VERSION="7.5"

SAMBA_VERSION = ss_packages ? "4.6.2" : ""

bash 'Check out samba + build headers if needed' do
  code <<-EOH
  if [ "#{SAMBA_VERSION}" = "" ]; then
    SAMBA_VERSION="`smbstatus -V | cut -d' ' -f2 | tr -d '\\n'`"
  else
    SAMBA_VERSION="#{SAMBA_VERSION}"
  fi
  SAMBA_DIR="build-samba-`echo ${SAMBA_VERSION} | sed -e 's:\\.:-:g'`-#{OS_DISTRO}-#{OS_DISTRO_VERSION.gsub(".", "-")}"

  if [ -d "#{SAMBA_PARENT_DIR}/${SAMBA_DIR}" ]; then
    ln -s ${SAMBA_DIR} #{SAMBA_SRC_DIR}
  else
    # Check out samba
    git clone -b samba-${SAMBA_VERSION} --single-branch --depth 1 https://github.com/samba-team/samba.git ${SAMBA_DIR}

    ln -s ${SAMBA_DIR} #{SAMBA_SRC_DIR}

    cd #{SAMBA_SRC_DIR}

    # Configure samba src
    # lockfile dropped by `waf configure`
    if [ ! -f "#{SAMBA_SRC_DIR}/.lock-wscript" ]; then
      ./configure
    fi

    # Build samba headers
    if [ ! -f "#{SAMBA_SRC_DIR}/bin/default/librpc/gen_ndr/server_id.h" ]; then
      make GEN_NDR_TABLES
    fi
  fi
  EOH
  cwd SAMBA_PARENT_DIR
end

#
# Configure Samba
#

if ss_packages
  smb_conf = "/opt/ss/etc/samba/smb.conf"
else
  smb_conf = "/etc/samba/smb.conf"
end

execute "Setup #{smb_conf}" do
  command "cat sample_entry_smb_conf.txt > #{smb_conf}"
  cwd "#{VFS_SRC_DIR}"
end

ruby_block "update_smb_conf" do
  block do
    file = Chef::Util::FileEdit.new(smb_conf)
    file.search_file_replace(/valid users = CHANGEME/, "valid users = #{node['swift_user']}")
    file.write_file
  end
end

smbpasswd_path = ss_packages ? "/opt/ss/bin/smbpasswd" : "/bin/smbpasswd"
execute "Setup Samba password" do
  command "printf \"#{node['swift_user']}\n#{node['swift_user']}\n\" | #{smbpasswd_path} -a -s #{node['swift_user']}"
end

#
# Create mount point and fstab entry
#
execute "Create SMB mount point" do
  command "mkdir /mnt/smb_proxyfs_mount"
  cwd "#{VFS_SRC_DIR}"
  not_if { ::Dir.exists?("/mnt/smb_proxyfs_mount") }
end

execute "Create NFS mount point" do
  command "mkdir /mnt/nfs_proxyfs_mount"
  not_if { ::Dir.exists?("/mnt/nfs_proxyfs_mount") }
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
# Build and install proxyfs
#

# TODO:  this not_if is incorrect, especially now that the same source tree can
# target centos or ubuntu.  We should check for the existence of the link
# below at the very least.
bash 'Build proxyfsd' do
  # Source profile because we may not have golang in our path yet
  code <<-EOH
  . #{DOT_BASH_PROFILE}
  make
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

if ss_packages
  # Creating link to jrpcclient's libs into the new /opt/ss path
  link '/opt/ss/lib64/libproxyfs.so.1.0.0' do
    to "#{JRPCCLIENT_SRC_DIR}/libproxyfs.so.1.0.0"
    link_type :symbolic
    owner "root"
    group "root"
  end

  link '/opt/ss/lib64/libproxyfs.so.1' do
    to "#{JRPCCLIENT_SRC_DIR}/libproxyfs.so.1.0.0"
    link_type :symbolic
    owner "root"
    group "root"
  end

  link '/opt/ss/lib64/libproxyfs.so' do
    to "#{JRPCCLIENT_SRC_DIR}/libproxyfs.so.1.0.0"
    link_type :symbolic
    owner "root"
    group "root"
  end

  # Creating link to vfs' libs into the new /opt/ss path
  bash 'Link VFS' do
    code <<-EOH
    /usr/bin/install -c -d /opt/ss/lib64/samba/vfs
    /usr/bin/install -c -m 755 proxyfs.so /opt/ss/lib64/samba/vfs
    EOH
    cwd VFS_SRC_DIR
  end
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

[profile default]
aws_access_key_id = test:tester
aws_secret_access_key = testing
s3 =
     endpoint_url = http://127.0.0.1:8080
     multipart_threshold = 64MB
     multipart_chunksize = 16MB
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

[profile default]
aws_access_key_id = test:tester
aws_secret_access_key = testing
s3 =
     endpoint_url = http://127.0.0.1:8080
     multipart_threshold = 64MB
     multipart_chunksize = 16MB
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
