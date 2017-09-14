tarfile_name = 'go1.8.3.linux-amd64.tar.gz'

tarfile_path = "/tmp/#{tarfile_name}"
tarfile_url  = "https://storage.googleapis.com/golang/#{tarfile_name}"

source_root = node['source_root']
proxyfs_user = node['proxyfs_user']
proxyfs_group = node['proxyfs_group']
is_dev = node['is_dev_environment']

GOROOT = "/usr/local/go"
HOME_DIR = "/home/#{proxyfs_user}"
DOT_BASH_PROFILE = "#{HOME_DIR}/.bash_profile"
DOT_BASHRC = "#{HOME_DIR}/.bashrc"
ROOT_DOT_BASH_PROFILE = "/root/.bash_profile"
ROOT_DOT_BASHRC = "/root/.bashrc"
REPO_CLONE_PARENT_DIR = "#{source_root}/src/github.com/swiftstack"
SAMBA_SRC_DIR = "#{REPO_CLONE_PARENT_DIR}/samba"
PROXYFS_SRC_DIR = "#{REPO_CLONE_PARENT_DIR}/ProxyFS"

remote_file "#{tarfile_path}" do
  source "#{tarfile_url}"
  owner 'root'
  group 'root'
  mode '0400'
  action :create
  not_if { ::File.exists?(GOROOT) }
end

execute 'untar_golang' do
  command "tar -C /usr/local -xzf #{tarfile_path}"
  not_if { ::File.exists?(GOROOT) }
end

file "/etc/profile.d/golang_path.sh" do
  content "export PATH=$PATH:#{GOROOT}/bin"
  mode '0644'
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
    file.insert_line_if_no_match(%r{usr/local/go/bin}, "export PATH=$GOPATH/bin:$PATH:/usr/local/go/bin")
    file.insert_line_if_no_match(/cdpfs/, "alias cdpfs='cd $GOPATH/src/github.com/swiftstack/ProxyFS'")
    file.insert_line_if_no_match(/cdsamba/, "alias cdsamba='cd #{SAMBA_SRC_DIR}'")
    file.insert_line_if_no_match(/ls -la/, "alias la='ls -la'")
    file.insert_line_if_no_match(/ls -lia/, "alias li='ls -lia'")
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
    file.insert_line_if_no_match(%r{usr/local/go/bin}, "export PATH=$GOPATH/bin:$PATH:/usr/local/go/bin")
    file.insert_line_if_no_match(/cdpfs/, "alias cdpfs='cd $GOPATH/src/github.com/swiftstack/ProxyFS'")
    file.insert_line_if_no_match(/cdsamba/, "alias cdsamba='cd #{SAMBA_SRC_DIR}'")
    file.insert_line_if_no_match(/ls -la/, "alias la='ls -la'")
    file.insert_line_if_no_match(/ls -lia/, "alias li='ls -lia'")
    file.insert_line_if_no_match(/statmnt/, "alias statmnt='stat /mnt/*'")
    file.write_file

  end
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

execute "Provision unmount_and_stop_pfs" do
  command "install -m 0755 #{source_root}/src/github.com/swiftstack/ProxyFS/cookbooks/proxyfs/files/default/usr/bin/unmount_and_stop_pfs /usr/bin"
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

cookbook_file "/etc/init/proxyfsd.conf" do
  source "etc/init/proxyfsd.upstart"
  # notifies :restart, 'service[proxyfsd]'
  only_if { ::File.directory?("/etc/init") }
end

cookbook_file "/usr/lib/systemd/system/ss-nmb.service" do
  source "usr/lib/systemd/system/ss-nmb.service"
  only_if { ::File.directory?("/usr/lib/systemd/system/") }
end

cookbook_file "/usr/lib/systemd/system/ss-smb.service" do
  source "usr/lib/systemd/system/ss-smb.service"
  only_if { ::File.directory?("/usr/lib/systemd/system/") }
end

bash 'Add SwiftStack repo in order to install old Samba' do
  code <<-EOH
    export RELEASEVER=`sed 's/.* release //' /etc/redhat-release | cut -d ' ' -f 1 | cut -d. -f1`
    curl -so /etc/pki/rpm-gpg/RPM-GPG-KEY-swiftstack-controller "https://demo.swiftstack.com:443/yum_repos/RPM-GPG-KEY-swiftstack-controller"
    rpm --import /etc/pki/rpm-gpg/RPM-GPG-KEY-swiftstack-controller
    curl -so /etc/yum.repos.d/swiftstack-controller.repo "https://demo.swiftstack.com:443/yum_repos/el${RELEASEVER}/swiftstack-controller.repo"
    yum makecache
  EOH
end

#
# Dependency lists by OS
#
if node[:platform_family].include?("rhel")

  # packages
  samba_package = ["ss-samba", "samba-client"]

  samba_deps = %w(
    gcc
    gcc-c++
    python-devel
    gnutls-devel
    libacl-devel
    openldap-devel
    ss-samba
    cifs-utils
  )

  proxyfs_packages = [
    "json-c-devel",
    "fuse",
  ]

  wireshark_packages = [
    "wireshark",
    "libcap",
  ]

  nfs_packages = [
    "nfs-utils"
  ]

else # assume debian

  # packages
  samba_package = ["ss-samba", "smbclient"]

  samba_deps = %w(
    gcc
    python-dev
    libgnutls-dev
    libacl1-dev
    libldap2-dev
    ss-samba
    pkg-config
    cifs-utils
  )

  proxyfs_packages = [
    "libjson-c-dev",
    "fuse",
  ]

  wireshark_packages = [
    "wireshark",
    "libcap2-bin",
  ]

  nfs_packages = [
    "nfs-kernel-server",
    "nfs-common"
  ]

end

packages = samba_package + samba_deps + proxyfs_packages + nfs_packages
packages += wireshark_packages if is_dev

packages.each do |pkg|
  package pkg do
    action :install
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
  cwd REPO_CLONE_PARENT_DIR
  not_if { ::File.exists?(SAMBA_SRC_DIR) }
end


#
# Check out and build samba
#
if node[:platform_family].include?("rhel")
  execute "Check out samba" do
    command "git clone -b v4-6-stable --single-branch --depth 1 https://github.com/samba-team/samba.git samba4-6-centos"
    cwd REPO_CLONE_PARENT_DIR
    not_if { ::File.exists?("#{REPO_CLONE_PARENT_DIR}/samba4-6-centos") }
  end

  link "#{REPO_CLONE_PARENT_DIR}/samba" do
    to "samba4-6-centos"
    link_type :symbolic
  end

else
  execute "Check out samba" do
    command "git clone -b v4-3-stable --single-branch --depth 1 https://github.com/samba-team/samba.git samba4-3-ubuntu"
    cwd REPO_CLONE_PARENT_DIR
    not_if { ::File.exists?("#{REPO_CLONE_PARENT_DIR}/samba4-3-ubuntu") }
  end

  link "#{REPO_CLONE_PARENT_DIR}/samba" do
    to "samba4-3-ubuntu"
    link_type :symbolic
  end

end

execute "Configure samba src" do
  command "./configure"
  cwd SAMBA_SRC_DIR
  # lockfile dropped by `waf configure`
  not_if { ::File.exists?("#{SAMBA_SRC_DIR}/.lock-wscript") }
end

execute "Build samba headers" do
  command "make GEN_NDR_TABLES"
  cwd SAMBA_SRC_DIR
  not_if { ::File.exists?(SAMBA_SRC_DIR + "/bin/default/librpc/gen_ndr/server_id.h") }
end

#
# Configure Samba
#
execute "Setup Samba password" do
  command "printf \"#{node['swift_user']}\n#{node['swift_user']}\n\" | smbpasswd -a -s #{node['swift_user']}"
end

cookbook_file "/opt/ss/etc/samba/smb.conf" do
  source "opt/ss/etc/samba/smb.conf"
#   only_if { ::File.directory?("/opt/ss/etc/samba/") }
end

execute "Print warning in /etc/samba/smb.conf" do
  command "echo \"# WARNING! This file is no longer used. Use /opt/ss/etc/samba/smb.conf instead.\" >> /etc/samba/smb.conf "
  cwd "#{PROXYFS_SRC_DIR}/vfs"
end

execute "Setup /opt/ss/etc/samba/smb.conf" do
  command "cat sample_entry_smb_conf.txt >> /opt/ss/etc/samba/smb.conf "
  cwd "#{PROXYFS_SRC_DIR}/vfs"
end

ruby_block "update_smb_conf" do
  block do
    smb_conf = "/opt/ss/etc/samba/smb.conf"

    file = Chef::Util::FileEdit.new(smb_conf)
    file.search_file_replace(/valid users = CHANGEME/, "valid users = #{node['swift_user']}")
    file.write_file
  end
end

#
# Create mount point and fstab entry
#
execute "Create SMB mount point" do
  command "mkdir /mnt/smb_proxyfs_mount"
  cwd "#{PROXYFS_SRC_DIR}/vfs"
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
  ./regression_test.py
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
