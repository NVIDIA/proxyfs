package confgen

import (
	"fmt"
	"os"
)

// writeGlobals writes the global section of smb.conf
func writeGlobals(smbConfPerVG *os.File, volumeGroup *VolumeGroup) (err error) {

	_, err = smbConfPerVG.WriteString("#!/bin/bash\n")
	if nil != err {
		return
	}
	return
}

// writeShares writes the per share version of the smb.conf file
// TODO - list each share and name section on the per VG case...
func writeShares(smbConfPerVG *os.File, volumeGroup *VolumeGroup) (err error) {
	return
}

// createSMBConf writes the per VG smb.conf file
func createSMBConf(vipDirPath string, volumeGroup *VolumeGroup) (err error) {
	var (
		smbConfPerVG *os.File
	)
	fmt.Printf("")
	smbConfPerVG, err = os.OpenFile(vipDirPath, os.O_CREATE|os.O_WRONLY, smbConfPerm)
	if nil != err {
		return
	}
	defer smbConfPerVG.Close()

	// Write the globals section
	// TODO - close file if error out here.... defer close???
	err = writeGlobals(smbConfPerVG, volumeGroup)
	if nil != err {
		return
	}

	// Write the shares in the VG
	err = writeShares(smbConfPerVG, volumeGroup)
	if nil != err {
		return
	}

	/*
		_, err = smbConfPerVG.WriteString("#!/bin/bash\n")
		if nil != err {
			return
		}
		_, err = smbConfPerVG.WriteString("set -e\n")
		if nil != err {
			return
		}
	*/

	return
}

/* My sample configuration from AWS cluster....
[global]

# ----------------------- Network-Related Options -------------------------

        server string = Samba Server Version %v
        map to guest = bad user

        netbios name = ip-192-168-17-177
        server min protocol = NT1


# --------------------------- Temporary Options -------------------------
# Until we resolve permissions
force user = root

# --------------------------- Logging Options -----------------------------

        # log files split per-machine:
        log file = /opt/ss/var/log/samba/log.%m
        # maximum size of 50KB per log file, then rotate:
        max log size = 50


# ----------------------- Standalone Server Options ------------------------
        security = user
        passdb backend = tdbsam:/opt/ss/lib64/samba/passdb.tdb
        restrict anonymous = 2
        rpc_server:lsarpc = embedded


# ----------------------- Domain Members Options ------------------------
;realm =
;winbind nss info =
winbind use default domain = yes
winbind refresh tickets = yes
winbind enum users = yes
winbind enum groups = yes
winbind expand groups = 5
winbind nested groups = yes

;idmap config *:backend = tdb
;idmap config *:range = 501 - 3554431

;idmap config :backend = rid
;idmap config :default = yes
;idmap config :range = 500 - 3554431

;template shell = /sbin/nologin
;domain master = no
;preferred master = no

;kerberos method = secrets and keytab


#----------------------------- Name Resolution -------------------------------

;wins server =
;remote announce = 0.0.0.0/

# --------------------------- ProxyFS Options ---------------------------

        proxyfs:PrivateIPAddr = 192.168.17.177
        proxyfs:TCPPort = 12345
		proxyfs:FastTCPPort = 32345

#============================ Share Definitions ==============================



[volume3]
        comment = ProxyFS volume volume3
        path = /opt/ss/var/lib/dummy_path
        proxyfs:volume = volume3
        valid users = "blake"
        public = yes
        writable = yes
        printable = no
        browseable = yes
        oplocks = False
        level2 oplocks = False
        aio read size = 1
        aio write size = 1
        case sensitive = yes
        preserve case = yes
        short preserve case = yes
        strict sync = yes

        vfs objects = proxyfs





[vol-vg32-2]
        comment = ProxyFS volume vol-vg32-2
        path = /opt/ss/var/lib/dummy_path
        proxyfs:volume = vol-vg32-2
        valid users = "swift", "bob", "ed", "blake"
        public = yes
        writable = yes
        printable = no
        browseable = yes
        oplocks = False
        level2 oplocks = False
        aio read size = 1
        aio write size = 1
        case sensitive = yes
        preserve case = yes
        short preserve case = yes
        strict sync = yes

		vfs objects = proxyfs

*/

// Controller template....
/*
[global]

# ----------------------- Network-Related Options -------------------------
{% if workgroup %}
	workgroup = {{ workgroup }}
{% endif %}
	server string = Samba Server Version %v
	map to guest = {{ map_to_guest }}

	netbios name = {{ hostname }}
	server min protocol = {{ server_min_protocol }}


# --------------------------- Temporary Options -------------------------
# Until we resolve permissions
{{ ad_disabled }}force user = root

# --------------------------- Logging Options -----------------------------

	# log files split per-machine:
	log file = /opt/ss/var/log/samba/log.%m
	# maximum size of 50KB per log file, then rotate:
	max log size = 50
{% if audit_logging %}
	log level = full_audit:1
{% endif %}

# ----------------------- Standalone Server Options ------------------------
	security = {{ security }}
	{{ ad_disabled }}passdb backend = tdbsam:/opt/ss/lib64/samba/passdb.tdb
	restrict anonymous = 2
	rpc_server:lsarpc = {{ rpc_server_lsarpc }}


# ----------------------- Domain Members Options ------------------------
{{ ad_enabled }}realm = {{ realm }}
{{ ad_id_mgmt }}winbind nss info = {{ id_schema }}
winbind use default domain = yes
winbind refresh tickets = yes
winbind enum users = yes
winbind enum groups = yes
winbind expand groups = 5
winbind nested groups = yes

{{ ad_enabled }}idmap config *:backend = tdb
{{ ad_enabled }}idmap config *:range = {{ id_range_min1 }} - {{ id_range_max1 }}

{{ ad_enabled }}idmap config {{ workgroup }}:backend = {{ backend }}
{{ ad_enabled }}idmap config {{ workgroup }}:default = yes
{{ ad_enabled }}idmap config {{ workgroup }}:range = {{ id_range_min2 }} - {{ id_range_max2 }}

{{ ad_enabled }}template shell = /sbin/nologin
{{ ad_enabled }}domain master = no
{{ ad_enabled }}preferred master = no

{{ ad_enabled }}kerberos method = secrets and keytab


#----------------------------- Name Resolution -------------------------------

{{ ad_enabled }}wins server =
{{ ad_enabled }}remote announce = {{ browser_announce }}/{{ workgroup }}

# --------------------------- ProxyFS Options ---------------------------

	proxyfs:PrivateIPAddr = {{ private_ip_addr }}
	proxyfs:TCPPort = {{ rpc_server_port }}
	proxyfs:FastTCPPort = {{ rpc_server_fastport }}

#============================ Share Definitions ==============================

{% for export in exports %}

[{{ export.name }}]
	comment = ProxyFS volume {{ export.name }}
	path = {{ path }}
	proxyfs:volume = {{ export.volume_name }}
	valid users = {{ export.valid_users }}
	public = yes
	writable = yes
	printable = no
	browseable = {% if export.browseable %}yes{% else %}no{% endif %}
	oplocks = False
	level2 oplocks = False
	aio read size = 1
	aio write size = 1
	case sensitive = yes
	preserve case = yes
	short preserve case = yes
	strict sync = {% if export.strict_sync %}yes{% else %}no{% endif %}
{% if audit_logging %}
	full_audit:success = mkdir rmdir read pread write pwrite rename unlink
	full_audit:prefix = %u|%I|%m|%S
	full_audit:failure = mkdir rmdir read pread write pwrite rename unlink
	full_audit:syslog = false
	vfs objects = full_audit proxyfs
{% else %}
	vfs objects = proxyfs
{% endif %}
{% if export.encryption_required %}
	smb encrypt = required
{% endif %}

{% endfor %}
*/

/* prototype vg file
 *
 # See smb.conf.example for a more detailed config file or
# read the smb.conf manpage.
# Run 'testparm' to verify the config is correct after
# you modified it.

[global]
        workgroup = SAMBA
        security = user

        passdb backend = tdbsam

        printing = cups
        printcap name = cups
        load printers = yes
        cups options = raw
        bind interfaces only = yes
        interfaces = 192.168.60.21
        pid directory = /opt/ss/var/run/samba/vg2
        lock directory = /opt/ss/var/cache/samba/vg2
        private dir = /opt/ss/var/cache/samba/vg2

[vg2]
        comment = Home Directories
        browseable = yes
        writable = yes
        guest ok  = yes
        read only = No
		path = /home/vagrant/subdir2
*/
