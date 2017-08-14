#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <netdb.h>
#include <json-c/json.h>
#include <pthread.h>
#include <proxyfs.h>
#include <proxyfs_testing.h>
#include "fault_inj.h"


// Flag that can be set from a command line arg to make tests less chatty
static bool quiet = true;

// Flag that can be set from a command line arg to make tests silent
static bool silent = false;

// Normally we don't want to dump out read data from failed reads
static bool dumpFailedReadData = false;

// Whether to print stats info, can be lots of output
static bool printStats = false;

char tlog_prefix[] = "test";

#define TLOG(fmt, ...) \
    do { if (!silent) printf("%s [%p]: " fmt, tlog_prefix, ((void*)((uint64_t)pthread_self())), ##__VA_ARGS__); fflush(stdout); } while (0)

typedef enum {
    MOUNT = 0,
    MKDIR,
    MKDIR_PATH,
    CREATE,
    CREATE_PATH,
    LOOKUP,
    LOOKUP_PATH,
    WRITE,
    READ,
    UNLINK,
    UNLINK_PATH,
    UNMOUNT,
    TYPE,
    FLUSH,
    LINK,
    LINK_PATH,
    SYMLINK,
    SYMLINK_PATH,
    GETSTAT,
    GETSTAT_PATH,
    RESIZE,
    READ_SYMLINK,
    READ_SYMLINK_PATH,
    RENAME,
    RENAME_PATH,
    READDIR,
    READDIR_PLUS,
    SET_TIME,
    SET_TIME_PATH,
    RMDIR,
    RMDIR_PATH,
    LOG,
    CHMOD,
    CHMOD_PATH,
    CHOWN,
    CHOWN_PATH,
    AIO_SEND,
    STATVFS,
} pfs_funcname_t;


// Global to hold param enum-to-string translation table
char* funcs[] = {
    "proxyfs_mount",
    "proxyfs_mkdir",
    "proxyfs_mkdir_path",
    "proxyfs_create",
    "proxyfs_create_path",
    "proxyfs_lookup",
    "proxyfs_lookup_path",
    "proxyfs_write",
    "proxyfs_read",
    "proxyfs_unlink",
    "proxyfs_unlink_path",
    "proxyfs_unmount",
    "proxyfs_type",
    "proxyfs_flush",
    "proxyfs_link",
    "proxyfs_link_path",
    "proxyfs_symlink",
    "proxyfs_symlink_path",
    "proxyfs_get_stat",
    "proxyfs_get_stat_path",
    "proxyfs_resize",
    "proxyfs_read_symlink",
    "proxyfs_read_symlink_path",
    "proxyfs_rename",
    "proxyfs_rename_path",
    "proxyfs_readdir",
    "proxyfs_readdir_plus",
    "proxyfs_settime",
    "proxyfs_settime_path",
    "proxyfs_rmdir",
    "proxyfs_rmdir_path",
    "proxyfs_log",
    "proxyfs_chmod",
    "proxyfs_chmod_path",
    "proxyfs_chown",
    "proxyfs_chown_path",
    "proxyfs_async_io_send",
    "proxyfs_statvfs",
};

// NOTE: Add new test groups above __MAX_TEST_GROUPS__
#define FOREACH_TEST_GROUP(TEST_GROUP)   \
    TEST_GROUP(ERROR_TESTS)              \
    TEST_GROUP(MOUNT_TESTS)              \
    TEST_GROUP(MKDIRCREATE_TESTS)        \
    TEST_GROUP(LINK_TESTS)               \
    TEST_GROUP(LOOKUP_TESTS)             \
    TEST_GROUP(READWRITE_TESTS)          \
    TEST_GROUP(READSYMLINK_TESTS)        \
    TEST_GROUP(READDIR_TESTS)            \
    TEST_GROUP(STATSIZE_TESTS)           \
    TEST_GROUP(STATVFS_TESTS)            \
    TEST_GROUP(RENAME_TESTS)             \
    TEST_GROUP(UNLINKRMDIR_TESTS)        \
    TEST_GROUP(CHOWNCHMOD_TESTS)         \
    TEST_GROUP(WRITEREAD4K_TEST)         \
    TEST_GROUP(WRITEREAD65K_TEST)        \
    TEST_GROUP(READPASTEOF_TEST)         \
    TEST_GROUP(SYSLOGWRITE_TEST)         \
    TEST_GROUP(ASYNC_READWRITE_TESTS)    \
    TEST_GROUP(__MAX_TEST_GROUPS__)

// Generate the test group enum from FOREACH_TEST_GROUP above
#define GENERATE_ENUM(ENUM) ENUM,
typedef enum {
    FOREACH_TEST_GROUP(GENERATE_ENUM)
} test_groups_t;

// Create test_group-is-enabled table. Default to all tests enabled.
#define GENERATE_BOOL(TEST_GROUP) true,
static bool enabledTests[] = {
    FOREACH_TEST_GROUP(GENERATE_BOOL)
};

// Create a test_group-to-string array
#define GENERATE_STRING(STRING) #STRING,
static const char *TEST_GROUP_STRING[] = {
    FOREACH_TEST_GROUP(GENERATE_STRING)
};

void disableAllTests() {
    int i = 0;
    for (i=0; i <= __MAX_TEST_GROUPS__; i++) {
        enabledTests[i] = false;
    }
}

void disableTest(test_groups_t test) {
    if (test >= __MAX_TEST_GROUPS__) { return; }
    enabledTests[test] = false;
}

void enableTest(test_groups_t test) {
    if (test >= __MAX_TEST_GROUPS__) { return; }
    enabledTests[test] = true;
}

bool isEnabled(test_groups_t test) {
    if (test >= __MAX_TEST_GROUPS__) { return false; }

    //printf("  TEST %s is %s\n", TEST_GROUP_STRING[test],(enabledTests[test]?"ENABLED":"DISABLED"));
    return enabledTests[test];
}

void print_test_settings() {
    printf("\nTest enable/disable settings:\n");
    int i = 0;
    for (i=0; i < __MAX_TEST_GROUPS__; i++) {
        printf("  %s: %s\n",TEST_GROUP_STRING[i], (enabledTests[i]?"true":"false"));
    }
}

#define BAD_BASENAME_SIZE (NAME_MAX + 24)
#define BAD_FULLPATH_SIZE (PATH_MAX + 24)

static char volName[]         = "CommonVolume";
static char badVolName[]      = "Volume_is_LxWxH";
static char rootDir[]         = "";
static char rootDirSlash[]    = "/";
static char subDir1[]         = "TestSubDir1";
static char subDir2[]         = "TestSubDir2";
static char subDir3[]         = "TestSubDir3";
static char subDir4[]         = "TestSubDir4";
static char subDir5[]         = "TestSubDir5";
static char subDir1Slash[]    = "/TestSubDir1";
static char subSubDir1[]      = "TestSubDir1/TestSubDir3";
static char subSubDir2[]      = "TestSubDir2/TestSubDir4";
static char badSubSubDir3[]   = "NotADir/TestSubDir5";
static char file1[]           = "TestNormalFile";
static char file2[]           = "TestNormalFileByPath";
static char badBasename[BAD_BASENAME_SIZE];
static char badFullpath[BAD_FULLPATH_SIZE];
static char subdirfile1[]     = "TestSubDirFile";
static char subdirfile1Path[] = "TestSubDir1/TestSubDirFile";
static char subdirfile2[]     = "SyslogFile";
static char subdirfile2Path[] = "TestSubDir1/SyslogFile";
static char notAFile[]        = "DoesNotExist";
static char subdirNotAFile[]  = "TestSubDir1/DoesNotExist";
static char link1[]           = "TestLink";
static char link2[]           = "TestLinkByPath";
static char bad_link[]        = "NotALink";
static char bad_link1[]       = "TestSubDir3/NotALink";
static char symlink1[]        = "TestSymlink";
static char symlink2[]        = "TestSymlinkByPath";

typedef enum {
    SUBDIR1 = 0,
    SUBDIR2,
    SUBDIR1_SLASH,
    SUBSUBDIR1,
    SUBSUBDIR2,
    BAD_PARENT_SUBDIR,
    FILE1,
    FILE2,
    BAD_BASENAME,
    BAD_FULLPATH,
    BAD_INODE,
    SUBDIR_FILE1,
    SUBDIR_FILE2,
    LINK1,
    LINK2,
    BAD_LINK,
    SYMLINK1,
    SYMLINK2,
    ROOT_DIR,
    __MAX_FILE_ID__,   // DON'T USE, ADD ABOVE
} file_id_t;

typedef struct {
    bool        is_enabled;
    uint16_t    file_type;
    uint64_t    parent_inode;
    char*       basename;
    char*       fullpath;
    uint64_t    inode;
    mode_t      mode;
    uid_t       uid;
    gid_t       gid;
} file_info_t;

// Global to hold filename-to-inum param translation table
file_info_t file_info[] = {
    { true, DT_DIR, 0, subDir1,       subDir1,         0,     -1,  -1,  -1  }, // SUBDIR1
    { true, DT_DIR, 0, subDir2,       subDir2,         0,     -1,  -1,  -1  }, // SUBDIR2
    { true, DT_DIR, 0, subDir1Slash,  subDir1Slash,    0,     -1,  -1,  -1  }, // SUBDIR1_SLASH
    { true, DT_DIR, 0, subDir3,       subSubDir1,      0,     -1,  -1,  -1  }, // SUBSUBDIR1
    { true, DT_DIR, 0, subDir4,       subSubDir2,      0,     -1,  -1,  -1  }, // SUBSUBDIR2
    { true, DT_DIR, 0, subDir5,       badSubSubDir3,   0,     -1,  -1,  -1  }, // BAD_PARENT_SUBDIR
    { true, DT_REG, 0, file1,         file1,           0,     -1,  -1,  -1  }, // FILE1
    { true, DT_REG, 0, file2,         file2,           0,     -1,  -1,  -1  }, // FILE2
    { true, DT_REG, 0, badBasename,   badBasename,     0,     -1,  -1,  -1  }, // BAD_BASENAME
    { true, DT_REG, 0, badBasename,   badFullpath,     0,     -1,  -1,  -1  }, // BAD_FULLPATH
    { true, DT_REG, 0, notAFile,      subdirNotAFile,  77777, -1,  -1,  -1  }, // BAD_INODE
    { true, DT_REG, 0, subdirfile1,   subdirfile1Path, 0,     -1,  -1,  -1  }, // SUBDIR_FILE1
    { true, DT_REG, 0, subdirfile2,   subdirfile2Path, 0,     -1,  -1,  -1  }, // SUBDIR_FILE2
    { true, DT_LNK, 0, link1,         link1,           0,     -1,  -1,  -1  }, // LINK1
    { true, DT_LNK, 0, link2,         link2,           0,     -1,  -1,  -1  }, // LINK2
    { true, DT_LNK, 0, bad_link,      bad_link1,       0,     -1,  -1,  -1  }, // BAD_LINK
    { true, DT_LNK, 0, symlink1,      symlink1,        0,     -1,  -1,  -1  }, // SYMLINK1
    { true, DT_LNK, 0, symlink2,      symlink2,        0,     -1,  -1,  -1  }, // SYMLINK2
    { true, DT_DIR, 0, rootDir,       rootDirSlash,    0,     -1,  -1,  -1  }, // ROOT_DIR
};

void init_globals() {
    // Create a basename that is too long. Fill it with the letter 'b'.
    memset((void*)badBasename, 'b', BAD_BASENAME_SIZE-2);
    badBasename[BAD_BASENAME_SIZE-1] = '\0';

    // Create a fullpath that is too long. Fill it with the letter 'f',
    // and then insert a slash to separate it into a too-long path plus
    // a basename that is an acceptable size.
    memset((void*)badFullpath, 'f', BAD_FULLPATH_SIZE-2);
    badFullpath[BAD_FULLPATH_SIZE-9] = '/';
    badFullpath[BAD_FULLPATH_SIZE-1] = '\0';
}

void enable_file(file_id_t id) {
    if (id >= __MAX_FILE_ID__) return;
    file_info[id].is_enabled = true;
}

void disable_file(file_id_t id) {
    if (id >= __MAX_FILE_ID__) return;
    file_info[id].is_enabled = false;
}

void disable_all_files() {
    int id = 0;
    for (id=0; id<__MAX_FILE_ID__; id++) {
        file_info[id].is_enabled = false;
    }
}

void update_file_info(file_id_t id, uint64_t parent_ino, uint64_t inode) {
    if (id >= __MAX_FILE_ID__) return;
    file_info[id].inode        = inode;
    file_info[id].parent_inode = parent_ino;

}

void update_fullpath(file_id_t id, char* new_fullpath) {
    if (id >= __MAX_FILE_ID__) return;
    file_info[id].fullpath = new_fullpath;
}

void update_parent(int error, file_id_t id, uint64_t parent_ino) {
    if (error != 0) return;
    if (id >= __MAX_FILE_ID__) return;
    file_info[id].parent_inode = parent_ino;
}

void update_uid_gid(int error, file_id_t id, uid_t new_uid, gid_t new_gid) {
    if (error != 0) return;
    if (id >= __MAX_FILE_ID__) return;
    if (new_uid != -1) file_info[id].uid  = new_uid;
    if (new_gid != -1) file_info[id].gid  = new_gid;
}

void update_mode(int error, file_id_t id, mode_t new_mode) {
    if (error != 0) return;
    if (id >= __MAX_FILE_ID__) return;
    if (new_mode == -1) return;
    file_info[id].mode = new_mode;

    // Add file type mode bits
    if (file_info[id].file_type == DT_DIR) {
        file_info[id].mode |= S_IFDIR;
        //printf("%s: updating mode for DIR %s to  0x%x\n",__FUNCTION__, file_info[id].basename, file_info[id].mode);
    } else if (file_info[id].file_type == DT_LNK) {
        file_info[id].mode |= S_IFLNK;
        //printf("%s: updating mode for SYMLINK %s to  0x%x\n",__FUNCTION__, file_info[id].basename, file_info[id].mode);
    } else if (file_info[id].file_type == DT_REG) {
        file_info[id].mode |= S_IFREG;
        //printf("%s: updating mode for FILE %s to  0x%x\n",__FUNCTION__, file_info[id].basename, file_info[id].mode);
    }
}

void update_perms(int error, file_id_t id, mode_t new_mode, uid_t new_uid, gid_t new_gid) {
    if (error != 0) return;
    if (id >= __MAX_FILE_ID__) return;
    if (new_mode != -1) update_mode(error, id, new_mode);
    update_uid_gid(error, id, new_uid, new_gid);
}

bool get_enabled(file_id_t id) {
    if (id >= __MAX_FILE_ID__) return false;
    return file_info[id].is_enabled;
}

uint64_t get_inode(file_id_t id) {
    if (id >= __MAX_FILE_ID__) return 0;
    return file_info[id].inode;
}

uint64_t get_parent_inode(file_id_t id) {
    if (id >= __MAX_FILE_ID__) return 0;
    return file_info[id].parent_inode;
}

char* get_name(file_id_t id) {
    if (id >= __MAX_FILE_ID__) return 0;
    return file_info[id].basename;
}

char* get_fullpath(file_id_t id) {
    if (id >= __MAX_FILE_ID__) return 0;
    return file_info[id].fullpath;
}

mode_t get_mode(file_id_t id) {
    if (id >= __MAX_FILE_ID__) return 0;
    return file_info[id].mode;
}

uid_t get_uid(file_id_t id) {
    if (id >= __MAX_FILE_ID__) return 0;
    return file_info[id].uid;
}

gid_t get_gid(file_id_t id) {
    if (id >= __MAX_FILE_ID__) return 0;
    return file_info[id].gid;
}

void print_one_file_info(file_info_t* fi) {
    printf(" basename:%s fullpath:%s parent:%ld inode:%ld mode:0x%x uid:%d gid:%d\n",
           fi->basename, fi->fullpath, fi->parent_inode, fi->inode,
           fi->mode, fi->uid, fi->gid);
}

void print_file_info() {
    printf("TEST FILE INFO:\n");
    int i = 0;
    for (i=0; i<__MAX_FILE_ID__; i++) {
        print_one_file_info(&file_info[i]);
    }
    printf("\n");
}

void print_stats(proxyfs_stat_t* stat) {
    if ((stat == NULL) || quiet) return;

    printf("  stat:    mode   = 0x%d\n"
           "           ino    = %" PRIu64 "\n"
           "           dev    = %" PRIu64 "\n"
           "           nlink  = %" PRIu64 "\n"
           "           uid    = %d\n"
           "           gid    = %d\n"
           "           size   = %" PRIu64 "\n"
           "           crtime = sec:%" PRIu64 " nsec:%" PRIu64 "\n"
           "           atime  = sec:%" PRIu64 " nsec:%" PRIu64 "\n"
           "           mtime  = sec:%" PRIu64 " nsec:%" PRIu64 "\n"
           "           ctime  = sec:%" PRIu64 " nsec:%" PRIu64 "\n",
           stat->mode,
           stat->ino,
           stat->dev,
           stat->nlink,
           stat->uid,
           stat->gid,
           stat->size,
           stat->crtim.sec, stat->crtim.nsec,
           stat->atim.sec, stat->atim.nsec,
           stat->mtim.sec, stat->mtim.nsec,
           stat->ctim.sec, stat->ctim.nsec);
}

void print_all_stats(proxyfs_stat_t* stat, uint64_t numEntries) {
    if ((stat == NULL) || quiet || !printStats) return;

    uint64_t i = 0;
    for (i=0; i<numEntries; i++) {
        printf("stat entry %" PRIu64 "/%" PRIu64 "\n", i+1, numEntries);
        print_stats(&stat[i]);
    }
}

void print_stat_vfs(struct statvfs* stat_vfs) {
    if ((stat_vfs == NULL) || quiet) return;

    printf("  statvfs: f_bsize    = %lu\n"
           "           f_frsize   = %lu\n"
           "           f_blocks   = %" PRIu64 "\n"
           "           f_bfree    = %" PRIu64 "\n"
           "           f_bavail   = %" PRIu64 "\n"
           "           f_files    = %" PRIu64 "\n"
           "           f_ffree    = %" PRIu64 "\n"
           "           f_favail   = %" PRIu64 "\n"
           "           f_fsid     = %lu\n"
           "           f_flag     = %lu\n"
           "           f_namemax  = %lu\n",
           stat_vfs->f_bsize,
           stat_vfs->f_frsize,
           stat_vfs->f_blocks,
           stat_vfs->f_bfree,
           stat_vfs->f_bavail,
           stat_vfs->f_files,
           stat_vfs->f_ffree,
           stat_vfs->f_favail,
           stat_vfs->f_fsid,
           stat_vfs->f_flag,
           stat_vfs->f_namemax);
}

// Globals. are. awesome. Or convenient. Or something. :/
//
// Mount handle
static mount_handle_t* mount_handle = NULL;
static mount_handle_t  bad_id_mount_handle = { NULL, 99999, 0 };

// API to return mount handle; makes it easier to mess with it in one place
mount_handle_t* mount_id() {
    if ( fail(BAD_MOUNT_ID) ) {
        return &bad_id_mount_handle;
    } else {
        return mount_handle;
    }
}

// API to return root dir inode out of the mount handle; makes it easier to mess with it in one place
uint64_t root_inode() {
    return mount_handle->root_dir_inode_num;
}

// Globals to store the uid/gid that did the last successful mount
static uid_t global_mounted_uid = 0;
static gid_t global_mounted_gid = 0;

// API to set mount uid and gid
void set_mount_uid_gid(uid_t new_uid, gid_t new_gid) {
    global_mounted_uid = new_uid;
    global_mounted_gid = new_gid;
}

// API to return mount uid
uid_t mount_uid() {
    return global_mounted_uid;
}

// API to return mount gid
uid_t mount_gid() {
    return global_mounted_gid;
}

// Read/write-related stuff
static uint8_t  bufToWrite[] = {0x41, 0x42, 0x43};
static uint64_t bytesWritten = 0;

// Rename-related
static char newName[80];

// Test stats
static int   private_numPassed = 0;
static int   private_numFailed = 0;
static char* private_failedTests[1024];

pthread_mutex_t test_results_lock = PTHREAD_MUTEX_INITIALIZER;

void test_passed() {
    pthread_mutex_lock(&test_results_lock);
    private_numPassed++;
    pthread_mutex_unlock(&test_results_lock);
}

void test_failed(char* failed_test) {
    pthread_mutex_lock(&test_results_lock);
    private_failedTests[private_numFailed] = failed_test;
    private_numFailed++;
    pthread_mutex_unlock(&test_results_lock);
}

int num_passed() {
    return private_numPassed;
}

int num_failed() {
    return private_numFailed;
}

void print_passed_failed() {
    if (silent) return;

    TLOG("Number of PASSED tests: %d\n", private_numPassed);
    TLOG("Number of FAILED tests: %d", private_numFailed);
    int i=0;
    if (private_numFailed > 0) {
        printf(" ( ");
        for (i = 0; i < private_numFailed; i++) {
            printf("%s ", private_failedTests[i]);
        }
        printf(")");
    }
    printf("\n\n");
}

void handle_api_return(char* funcToTest, int err, int exp_status) {
    if (err != exp_status) {
        goto fail;
    }

    // Otherwise this is a pass
    test_passed();
    TLOG("SUCCESS, Got status=%d from %s.\n\n",err,funcToTest);
    return;

fail:
    test_failed(funcToTest);
    TLOG("FAILURE, test %s: got status='%s(%d)' expected '%s(%d)'.\n\n",
         funcToTest, strerror(err), err, strerror(exp_status), exp_status);
}

void handle_api_return_with_inode(char* funcToTest, int err, int exp_status, uint64_t inode) {
    if (err != exp_status) {
        goto fail;
    }

    // Otherwise this is a pass
    test_passed();
    TLOG("SUCCESS, Got status=%d from %s, rtn_inode=%" PRIu64 ".\n\n",err,funcToTest,inode);
    return;

fail:
    test_failed(funcToTest);
    TLOG("FAILURE, got status=%d from %s, expected %d.\n\n",err,funcToTest,exp_status);
}

void handle_api_return_with_exp_inode(char* funcToTest, int err, int exp_status, uint64_t inode, uint64_t exp_inode) {
    if (err != exp_status) {
        goto fail;
    }

    // If we expected a failure status and got it, no need to check anything else
    if (exp_status != 0) {
        test_passed();
        TLOG("SUCCESS, Got status=%d from %s.\n\n",err,funcToTest);
        return;
    }

    if (inode == exp_inode) {
        test_passed();
        TLOG("SUCCESS, Got status=%d from %s.\n\n",err,funcToTest);
        return;
    } else {
        test_failed(funcToTest);
        TLOG("FAILURE, got status=%d from %s, rtn_inode=%" PRIu64 " (should be %" PRIu64 ").\n\n",err,funcToTest,inode, exp_inode);
        return;
    }

fail:
    test_failed(funcToTest);
    TLOG("FAILURE, got status=%d from %s, expected %d.\n\n",err,funcToTest,exp_status);
}

void handle_api_return_type(char* funcToTest, int err, int exp_status, uint16_t type, uint16_t exp_type) {
    if (err != exp_status) {
        goto fail;
    }

    // If we expected a failure status and got it, no need to check anything else
    if (exp_status != 0) {
        test_passed();
        TLOG("SUCCESS, Got status=%d from %s.\n\n",err,funcToTest);
        return;
    }

    if (type == exp_type) {
        test_passed();
        TLOG("SUCCESS, Got status=%d from %s.\n\n",err,funcToTest);
        return;
    } else {
        test_failed(funcToTest);
        TLOG("FAILURE, got status=%d from %s, type=%d (should be %d).\n\n",err,funcToTest,type, exp_type);
        return;
    }

fail:
    test_failed(funcToTest);
    TLOG("FAILURE, got status=%d from %s, expected %d.\n\n",err,funcToTest,exp_status);
}

void handle_api_return_with_exp_len(char* funcToTest, int err, int exp_status, uint64_t len, uint64_t exp_len) {
    if (err != exp_status) {
        goto fail;
    }

    if ((exp_status != 0) || ((exp_status == 0) && (len == exp_len))) {
        test_passed();
        TLOG("SUCCESS, Got status=%d from %s.\n\n",err,funcToTest);
        return;
    }

fail:
    test_failed(funcToTest);
    TLOG("FAILURE, got status=%d from %s, len=%" PRIu64 " (should be %" PRIu64 ").\n\n",err, funcToTest, len, exp_len);
}

void handle_api_return_with_exp_target(char* funcToTest, int err, int exp_status, const char* target, char* exp_target) {
    if (err != exp_status) {
        goto fail;
    }

    if ((exp_status != 0) || ((exp_status == 0) && (strcmp(target, file1) == 0))) {
        test_passed();
        TLOG("SUCCESS, Got status=%d from %s.\n\n",err,funcToTest);
        return;
    }

fail:
    test_failed(funcToTest);
    TLOG("FAILURE, got status=%d from %s, expected %d. target=%s, expected %s\n\n",err,funcToTest,exp_status,target,exp_target);
}

void handle_api_return_with_exp_len_and_buf(char* funcToTest, int err, int exp_status, uint64_t len, uint64_t exp_len, uint8_t* buf, uint8_t* exp_buf) {
    // Check expected status
    if (err != exp_status) {
        goto fail;
    }

    // If we expected a failure status and got it, no need to check anything else
    if (exp_status != 0) {
        test_passed();
        TLOG("SUCCESS, Got status=%d from %s.\n\n",err,funcToTest);
        return;
    }

    // Check expected length
    if (len != exp_len) {
        test_failed(funcToTest);
        TLOG("FAILURE, %s did NOT return the data we expected. Read %zu bytes, expected %zu\n\n", funcToTest, len, exp_len);
        return;
    }

    // validate that what we read was what we wrote
    if (buf == NULL) {
        test_failed(funcToTest);
        TLOG("FAILURE, %s returned a null data pointer.\n\n", funcToTest);
        return;
    } else if (memcmp(buf, exp_buf, len) == 0) {
        test_passed();
        TLOG("SUCCESS, %s returned the data we expected.\n\n", funcToTest);
        return;
    } else {
        test_failed(funcToTest);
        TLOG("FAILURE, %s did NOT return the data we expected.\n\n", funcToTest);
        // TLOG("FAILURE, %s did NOT return the data we expected. Read data:\n", funcToTest);
        // uint8_t i=0;
        // for (i=0; i<len; i++) {
        // 	printf("0x%02x ",buf[i]);
        // }
        // printf("\n\n");
        if (dumpFailedReadData) {
            FILE* fp = fopen("./failed_read_data", "w+");
            if (fp == NULL) {
                TLOG("Error opening failed_read_data, errno=%s\n",strerror(errno));
            }
            size_t bytesWritten = fwrite(buf, len, 1, fp);
            if (bytesWritten != len) {
                TLOG("Error writing to failed_read_data, wrote %ld bytes, expected %ld\n",bytesWritten,len);
            }
            fclose(fp);
        }
        return;
    }

fail:
    test_failed(funcToTest);
    TLOG("FAILURE, got status=%d from %s, len=%" PRIu64 " (should be %" PRIu64 ").\n\n", err, funcToTest, len, exp_len);
}

bool check_dirents(char* funcToTest, struct dirent* dir_ent, int prevDirLoc) {
    int numEntries = 1; // Always get back just one entry

    int expectedDirLoc = 0;
    if (prevDirLoc != -1) {
        expectedDirLoc = prevDirLoc + 1;
    }

    // validate that we got something
    if (dir_ent == NULL) {
        TLOG("FAILURE, %s returned a null dir_ent pointer.\n\n", funcToTest);
        return false;
    } else {
        // Check that file type for each dirent != 0
        int i=0;
        for (i = 0; i < numEntries; i++) {
            // Check that inode for each dirent != 0
            if (dir_ent[i].d_ino == 0) {
                TLOG("FAILURE, %s returned zero entry[%d].d_ino, expected nonzero.\n\n", funcToTest, i);
                return false;
#ifdef _DIRENT_HAVE_D_TYPE
            // Check that file type for each dirent != 0
            } else if (dir_ent[i].d_type == 0) {
                TLOG("FAILURE, %s returned zero entry[%d].d_type, expected nonzero.\n\n", funcToTest, i);
                return false;
#endif
            // Check that name for each dirent != NULL
            } else if (dir_ent[i].d_name == NULL) {
                TLOG("FAILURE, %s returned null entry[%d].d_name, expected nonzero.\n\n", funcToTest, i);
                return false;

            // Check that directory location is what we expected
            } else if (dir_ent[i].d_off != expectedDirLoc) {
                TLOG("FAILURE, %s returned entry[%d].d_off=%d, expected %d.\n\n", funcToTest, i, (int)dir_ent[i].d_off, expectedDirLoc);
                return false;
            }
        }
    }

    return true;
}

void handle_api_return_with_dirent(char* funcToTest, int err, uint64_t dir_inode, struct dirent* dir_ent, int prevDirLoc, int exp_status) {
    // Check expected status
    if ((exp_status != -1) && (err != exp_status)) {
        goto fail;
    }

    // If we expected a failure status and got it, no need to check anything else
    if ((exp_status != -1) || (exp_status != 0)) {
        goto pass;
    }

    // validate that we got something
    if (!check_dirents(funcToTest, dir_ent, prevDirLoc)) {
        test_failed(funcToTest);
        return;
    }

pass:
    // success if we got here
    test_passed();
    if (dir_ent != NULL) {
        TLOG("SUCCESS, Got status=%d from %s. inode=%" PRIu64 " dir_entry %d inode=%" PRIu64 " type=%d basename=%s\n\n",err,funcToTest,
           	dir_inode, (int)dir_ent->d_off, dir_ent->d_ino, dir_ent->d_type, dir_ent->d_name);
    } else {
        TLOG("SUCCESS, Got status=%d from %s\n\n",err,funcToTest);
    }
    return;

fail:
    test_failed(funcToTest);
    TLOG("FAILURE, got status=%d from %s.\n\n",err,funcToTest);
}

void handle_api_return_with_dirent_stat(char* funcToTest, int err, uint64_t dir_inode, struct dirent* dir_ent, proxyfs_stat_t* stat, int prevDirLoc, int exp_status) {
    // Check expected status
    if ((exp_status != -1) && (err != exp_status)) {
        goto fail;
    }

    // If we expected a failure status and got it, no need to check anything else
    if ((exp_status != -1) || (exp_status != 0)) {
        goto pass;
    }

    // validate that we got something
    if (!check_dirents(funcToTest, dir_ent, prevDirLoc)) {
        test_failed(funcToTest);
        return;
    }

    if (stat == NULL) {
        test_failed(funcToTest);
        TLOG("FAILURE, %s returned a null stat(%p) pointer.\n\n", funcToTest, stat);
        return;
    }

pass:
    // success if we got here
    test_passed();
    if (dir_ent != NULL) {
        TLOG("SUCCESS, Got status=%d from %s. inode=%" PRIu64 " dir_entry %d inode=%" PRIu64 " type=%d basename=%s\n\n",err,funcToTest,
           	dir_inode, (int)dir_ent->d_off, dir_ent->d_ino, dir_ent->d_type, dir_ent->d_name);
    } else {
        TLOG("SUCCESS, Got status=%d from %s\n\n",err,funcToTest);
    }
    print_all_stats(stat, 1);
    return;

fail:
    test_failed(funcToTest);
    TLOG("FAILURE, got status=%d from %s.\n\n",err,funcToTest);
}

void handle_api_return_with_stat(char* funcToTest, int err, int exp_status, uint64_t ino, int exp_mode, int exp_uid, int exp_gid, proxyfs_stat_t* stat, int expectedSize) {
    // Check expected status
    if (err != exp_status) {
        goto fail;
    }

    // If we expected a failure status and got it, no need to check anything else
    if (exp_status != 0) {
        test_passed();
        TLOG("SUCCESS, Got status=%d from %s.\n\n",err,funcToTest);
        return;
    }

    // validate that we got something
    if (stat == NULL) {
        test_failed(funcToTest);
        TLOG("FAILURE, %s returned a null stat pointer.\n\n", funcToTest);
        return;
    } else if ((expectedSize >= 0) && (stat->size != (uint64_t)expectedSize)) {
        test_failed(funcToTest);
        TLOG("FAILURE, %s returned size %zu, expected %d.\n\n", funcToTest, stat->size, expectedSize);
        print_stats(stat);
        return;
    } else if ((ino != 0) && (stat->ino != ino)) {
        test_failed(funcToTest);
        TLOG("FAILURE, %s returned inode %zu, expected %zu.\n\n", funcToTest, stat->ino, ino);
        print_stats(stat);
        return;
    } else if ((exp_mode != -1) && (stat->mode != (mode_t)exp_mode)) {
        test_failed(funcToTest);
        TLOG("FAILURE, %s returned mode %x, expected %x.\n\n", funcToTest, stat->mode, exp_mode);
        print_stats(stat);
        return;
    } else if ((exp_uid != -1) && (stat->uid != (uid_t)exp_uid)) {
        test_failed(funcToTest);
        TLOG("FAILURE, %s returned uid %d, expected %d.\n\n", funcToTest, stat->uid, exp_uid);
        print_stats(stat);
        return;
    } else if ((exp_gid != -1) && (stat->gid != (uid_t)exp_gid)) {
        test_failed(funcToTest);
        TLOG("FAILURE, %s returned gid %d, expected %d.\n\n", funcToTest, stat->gid, exp_gid);
        print_stats(stat);
        return;
    }

    // success if we got here
    test_passed();
    TLOG("SUCCESS, Got status=%d from %s.\n\n",err,funcToTest);
    print_all_stats(stat, 1);
    return;

fail:
    test_failed(funcToTest);
    TLOG("FAILURE, got status=%d from %s.\n\n",err,funcToTest);
}

void handle_api_return_with_atime_mtime(char* funcToTest, int err, int exp_status, proxyfs_stat_t* stat, proxyfs_timespec_t* exp_atime, proxyfs_timespec_t* exp_mtime) {
    // Check expected status
    if (err != exp_status) {
        goto fail;
    }

    // If we expected a failure status and got it, no need to check anything else
    if (exp_status != 0) {
        test_passed();
        TLOG("SUCCESS, Got status=%d from %s.\n\n",err,funcToTest);
        return;
    }

    // validate that we got something
    if (stat == NULL) {
        test_failed(funcToTest);
        TLOG("FAILURE, %s returned a null stat pointer.\n\n", funcToTest);
        return;
    } else {
        if (((exp_atime != NULL) && ((stat->atim.sec == exp_atime->sec) && (stat->atim.nsec == exp_atime->nsec))) &&
            ((exp_mtime != NULL) && ((stat->mtim.sec == exp_mtime->sec) && (stat->mtim.nsec == exp_mtime->nsec)))) {
            test_passed();
            TLOG("SUCCESS, %s returned the atime and mtime we expected.\n\n", funcToTest);
      	} else {
            test_failed(funcToTest);
            TLOG("FAILURE, %s did NOT return the atime and mtime we expected. \n\n", funcToTest);
            print_stats(stat);
        }
        return;
    }

    // success if we got here
    test_passed();
    TLOG("SUCCESS, Got status=%d from %s.\n\n",err,funcToTest);
    print_all_stats(stat, 1);
    return;

fail:
    test_failed(funcToTest);
    TLOG("FAILURE, got status=%d from %s.\n\n",err,funcToTest);
}

void handle_api_return_with_statvfs(char* funcToTest, int err, int exp_status, int expectedSize, struct statvfs* stat_vfs) {
    // Check expected status
    if (err != exp_status) {
        goto fail;
    }

    // If we expected a failure status and got it, no need to check anything else
    if (exp_status != 0) {
        test_passed();
        TLOG("SUCCESS, Got status=%d from %s.\n\n",err,funcToTest);
        return;
    }

    // validate that we got something
    if (stat_vfs == NULL) {
        test_failed(funcToTest);
        TLOG("FAILURE, %s returned a null stat_vfs pointer.\n\n", funcToTest);
        return;
    } else if ((expectedSize >= 0) && (stat_vfs->f_frsize != (uint64_t)expectedSize)) {
        test_failed(funcToTest);
        TLOG("FAILURE, %s returned size %zu, expected %d.\n\n", funcToTest, stat_vfs->f_frsize, expectedSize);
        print_stat_vfs(stat_vfs);
        return;
#if 0
    } else if ((ino != 0) && (stat_vfs->ino != ino)) {
        test_failed(funcToTest);
        TLOG("FAILURE, %s returned inode %zu, expected %zu.\n\n", funcToTest, stat_vfs->ino, ino);
        print_stat_vfs(stat_vfs);
        return;
    } else if ((exp_mode != -1) && (stat_vfs->mode != (mode_t)exp_mode)) {
        test_failed(funcToTest);
        TLOG("FAILURE, %s returned mode %x, expected %x.\n\n", funcToTest, stat_vfs->mode, exp_mode);
        print_stat_vfs(stat_vfs);
        return;
    } else if ((exp_uid != -1) && (stat_vfs->uid != (uid_t)exp_uid)) {
        test_failed(funcToTest);
        TLOG("FAILURE, %s returned uid %d, expected %d.\n\n", funcToTest, stat_vfs->uid, exp_uid);
        print_stat_vfs(stat_vfs);
        return;
    } else if ((exp_gid != -1) && (stat_vfs->gid != (uid_t)exp_gid)) {
        test_failed(funcToTest);
        TLOG("FAILURE, %s returned gid %d, expected %d.\n\n", funcToTest, stat_vfs->gid, exp_gid);
        print_stat_vfs(stat_vfs);
        return;
#endif
    }

    // success if we got here
    test_passed();
    TLOG("SUCCESS, Got status=%d from %s.\n\n",err,funcToTest);
    print_stat_vfs(stat_vfs);
    return;

fail:
    test_failed(funcToTest);
    TLOG("FAILURE, got status=%d from %s.\n\n",err,funcToTest);
}

void test_log(char* message) {
    int exp_status = 0;
    char* funcToTest = funcs[LOG];
    TLOG("Calling %s with message %s\n", funcToTest, message);
    int err = proxyfs_log(mount_id(), message);
    if (err == exp_status) {
        TLOG("SUCCESS, Got status=%d from %s.\n\n",err,funcToTest);
    } else {
        TLOG("FAILURE, got status=%d from %s, expected %d.\n\n",err,funcToTest,exp_status);
    }
}

int test_mount(char* volname, uint64_t options, uid_t userid, gid_t groupid, int exp_status) {
    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return 0;
    }

    char* funcToTest = funcs[MOUNT];

    TLOG("Calling %s for volume %s userid %d groupid %d, expect status %d\n", funcToTest, volname, userid, groupid, exp_status);
    int err = proxyfs_mount(volname, options, userid, groupid, &mount_handle);
    handle_api_return(funcToTest, err, exp_status);
    if (err != exp_status) {
        return -1;
    }
    if (err == 0) {
        // Save away mount uid and gid
        set_mount_uid_gid(userid, groupid);
    }
}

void test_mkdir(file_id_t id, uint64_t parent_inode, uid_t uid, gid_t gid, mode_t mode, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[MKDIR];
    file_info_t* fi = &file_info[id];

    TLOG("Calling %s for %s (%d), expect status %d\n", funcToTest, fi->basename, (int)id, exp_status);
    int err = proxyfs_mkdir(mount_id(), parent_inode, fi->basename, uid, gid, mode, &fi->inode);
    update_perms(err, mode, id, uid, gid); // If success, update file_info
    update_parent(err, id, parent_inode); // If success, update file_info
    handle_api_return_with_inode(funcToTest, err, exp_status, fi->inode);
}

void test_mkdir_path(file_id_t id, uid_t uid, gid_t gid, mode_t mode, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[MKDIR_PATH];
    file_info_t* fi = &file_info[id];

    TLOG("Calling %s for %s (%d), expect status %d\n", funcToTest, fi->fullpath, (int)id, exp_status);
    int err = proxyfs_mkdir_path(mount_id(), fi->fullpath, uid, gid, mode);
    update_perms(err, mode, id, uid, gid); // If success, update file_info
    handle_api_return(funcToTest, err, exp_status);
}

void test_chmod(file_id_t id, mode_t mode, mode_t exp_mode, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    // Since we'll be using this file's inode, make sure it is valid
    if (get_inode(id) == 0) { TLOG("%s: ERROR, %s inode=0! Skipping test.\n\n",__FUNCTION__,get_name(id)); return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[CHMOD];
    file_info_t* fi = &file_info[id];

    TLOG("Calling %s for inode %" PRIu64 " with mode 0x%x, expect status %d\n", funcToTest, fi->inode, mode, exp_status);
    int err = proxyfs_chmod(mount_id(), fi->inode, mode);
    update_mode(err, id, exp_mode); // If success, update file_info
    handle_api_return(funcToTest, err, exp_status);
}

void test_chmod_path(file_id_t id, mode_t mode, mode_t exp_mode, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[CHMOD_PATH];
    file_info_t* fi = &file_info[id];

    TLOG("Calling %s for fullpath %s with mode 0x%x, expect status %d\n", funcToTest, fi->fullpath, mode, exp_status);
    int err = proxyfs_chmod_path(mount_id(), fi->fullpath, mode);
    update_mode(err, id, exp_mode); // If success, update file_info
    handle_api_return(funcToTest, err, exp_status);
}

void test_chown(file_id_t id, uid_t uid, gid_t gid, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    // Since we'll be using this file's inode, make sure it is valid
    if (get_inode(id) == 0) { TLOG("%s: ERROR, %s inode=0! Skipping test.\n\n",__FUNCTION__,get_name(id)); return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[CHOWN];
    file_info_t* fi = &file_info[id];

    TLOG("Calling %s for inode %" PRIu64 " with uid %d gid %d, expect status %d\n", funcToTest, fi->inode, uid, gid, exp_status);
    int err = proxyfs_chown(mount_id(), fi->inode, uid, gid);
    update_uid_gid(err, id, uid, gid); // If success, update file_info
    handle_api_return(funcToTest, err, exp_status);
}

void test_chown_path(file_id_t id, uid_t uid, gid_t gid, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[CHOWN_PATH];
    file_info_t* fi = &file_info[id];

    TLOG("Calling %s for fullpath %s with uid %d gid %d, expect status %d\n", funcToTest, fi->fullpath, uid, gid, exp_status);
    int err = proxyfs_chown_path(mount_id(), fi->fullpath, uid, gid);
    update_uid_gid(err, id, uid, gid); // If success, update file_info
    handle_api_return(funcToTest, err, exp_status);
}

void test_create(file_id_t id, uint64_t parent_inode, uid_t uid, gid_t gid, mode_t mode, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    // Since we'll be using parent_inode, make sure it is valid
    if (parent_inode == 0) { TLOG("%s: ERROR, %s parent_inode=0! Skipping test.\n\n",__FUNCTION__,get_name(id)); return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[CREATE];
    file_info_t* fi = &file_info[id];

    // Update parent inode, unless this is a non-success test
    if (exp_status == 0) {
        fi->parent_inode = parent_inode;
    }

    TLOG("Calling %s for %s (%d) with mode 0x%x, expect status %d\n", funcToTest, fi->basename, (int)id, mode, exp_status);
    int err = proxyfs_create(mount_id(), parent_inode, fi->basename, uid, gid, mode, &fi->inode);
    update_perms(err, id, mode, uid, gid); // If success, update file_info
    handle_api_return_with_inode(funcToTest, err, exp_status, fi->inode);
}

void test_create_path(file_id_t id, uid_t uid, gid_t gid, mode_t mode, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[CREATE_PATH];
    file_info_t* fi = &file_info[id];

    TLOG("Calling %s for %s (%d) with mode 0x%x, expect status %d\n", funcToTest, fi->fullpath, (int)id, mode, exp_status);
    int err = proxyfs_create_path(mount_id(), fi->fullpath, uid, gid, mode, &fi->inode);
    update_perms(err, id, mode, uid, gid); // If success, update file_info
    handle_api_return_with_inode(funcToTest, err, exp_status, fi->inode);
}

void test_link(file_id_t id, uint64_t parent_inode, uint64_t target_inode, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    // Since we'll be using parent_inode, make sure it is valid
    if (parent_inode == 0) { TLOG("%s: ERROR, %s parent_inode=0! Skipping test.\n\n",__FUNCTION__,get_name(id)); return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[LINK];
    file_info_t* fi = &file_info[id];

    // Update parent inode, unless this is a non-success test
    if (exp_status == 0) {
        fi->parent_inode = parent_inode;
    }

    TLOG("Calling %s for basename %s and target inode %" PRIu64 ", expect status %d.\n",funcToTest, fi->basename, target_inode, exp_status);
    int err = proxyfs_link(mount_id(), parent_inode, fi->basename, target_inode);
    handle_api_return(funcToTest, err, exp_status);
}

void test_link_path(file_id_t id, char* tgt_fullpath, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[LINK_PATH];
    file_info_t* fi = &file_info[id];

    TLOG("Calling %s for source %s target %s, expect status %d\n", funcToTest, fi->fullpath, tgt_fullpath, exp_status);
    int err = proxyfs_link_path(mount_id(), fi->fullpath, tgt_fullpath);
    handle_api_return(funcToTest, err, exp_status);
}

void test_symlink(file_id_t id, uint64_t parent_inode, char* target, uid_t uid, gid_t gid, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    // Since we'll be using parent_inode, make sure it is valid
    if (parent_inode == 0) { TLOG("%s: ERROR, %s parent_inode=0! Skipping test.\n\n",__FUNCTION__,get_name(id)); return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[SYMLINK];
    file_info_t* fi = &file_info[id];

    // Update parent inode, unless this is a non-success test
    if (exp_status == 0) {
        fi->parent_inode = parent_inode;
    }

    TLOG("Calling %s for basename %s and target %s, expect status %d.\n",funcToTest, fi->basename, target, exp_status);
    int err = proxyfs_symlink(mount_id(), parent_inode, fi->basename, target, uid, gid);
    update_perms(err, id, 0777, uid, gid); // If success, update file_info
    handle_api_return(funcToTest, err, exp_status);
}

void test_symlink_path(file_id_t id, char* tgt_fullpath, uid_t uid, gid_t gid, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[SYMLINK_PATH];
    file_info_t* fi = &file_info[id];

    TLOG("Calling %s for source %s target %s, expect status %d\n", funcToTest, fi->fullpath, tgt_fullpath, exp_status);
    int err = proxyfs_symlink_path(mount_id(), fi->fullpath, tgt_fullpath, uid, gid);
    update_perms(err, id, 0777, uid, gid); // If success, update file_info
    handle_api_return(funcToTest, err, exp_status);
}

// Do lookup and store the resulting inode in the data structure
void do_lookup(file_id_t id, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[LOOKUP];
    file_info_t* fi = &file_info[id];

    TLOG("Calling %s for basename %s and dir inode %" PRIu64 ", expect status %d.\n",funcToTest, fi->basename, fi->parent_inode, exp_status);
    int err = proxyfs_lookup(mount_id(), fi->parent_inode, fi->basename, &fi->inode);
    handle_api_return_with_inode(funcToTest, err, exp_status, fi->inode);
}

// Do lookup and compare the resulting inode to what is in the data structure
void test_lookup(file_id_t id, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[LOOKUP];
    file_info_t* fi = &file_info[id];

    TLOG("Calling %s for basename %s and dir inode %" PRIu64 ", expect status %d.\n",funcToTest, fi->basename, fi->parent_inode, exp_status);
    uint64_t rtn_inode = 0;
    int err = proxyfs_lookup(mount_id(), fi->parent_inode, fi->basename, &rtn_inode);
    handle_api_return_with_exp_inode(funcToTest, err, exp_status, rtn_inode, fi->inode);
}

void test_lookup_path(char* fullpath, int exp_status, uint64_t exp_inode) {
    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[LOOKUP_PATH];
    TLOG("Calling %s for %s, expect status %d\n", funcToTest, fullpath, exp_status);
    uint64_t rtn_inode = 0;
    int err = proxyfs_lookup_path(mount_id(), fullpath, &rtn_inode);
    handle_api_return_with_exp_inode(funcToTest, err, exp_status, rtn_inode, exp_inode);
}

void test_write(file_id_t id, uint64_t offset, uint64_t length, uint8_t* buf, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    // Since we'll be using this file's inode, make sure it is valid
    if (get_inode(id) == 0) { TLOG("%s: ERROR, %s inode=0! Skipping test.\n\n",__FUNCTION__,get_name(id)); return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[WRITE];
    file_info_t* fi = &file_info[id];

    TLOG("Calling %s for inode %" PRIu64 ", with offset %" PRIu64 " length %" PRIu64 ", expect status %d.\n",funcToTest, fi->inode, offset, length, exp_status);
    uint64_t bytesWritten = 0;
    int err = proxyfs_write(mount_id(), fi->inode, offset, buf, length, &bytesWritten);
    handle_api_return_with_exp_len(funcToTest, err, exp_status, bytesWritten, length);
}

void test_flush(file_id_t id, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    // Since we'll be using this file's inode, make sure it is valid
    if (get_inode(id) == 0) { TLOG("%s: ERROR, %s inode=0! Skipping test.\n\n",__FUNCTION__,get_name(id)); return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[FLUSH];
    file_info_t* fi = &file_info[id];

    TLOG("Calling %s for inode %" PRIu64 ", expect status %d.\n",funcToTest, fi->inode, exp_status);
    int err = proxyfs_flush(mount_id(), fi->inode);
    handle_api_return(funcToTest, err, exp_status);
}

// This API allows read attempts beyond EOF, by allowing the caller to specify the
// requested read length as well as the expected read length.
void test_read_past_eof(file_id_t id, uint64_t offset, uint64_t length, uint8_t* exp_buf, uint64_t exp_len, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    // Since we'll be using this file's inode, make sure it is valid
    if (get_inode(id) == 0) { TLOG("%s: ERROR, %s inode=0! Skipping test.\n\n",__FUNCTION__,get_name(id)); return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[READ];
    file_info_t* fi = &file_info[id];

    size_t   readBufSize = 1024+length;
    uint8_t* readBufPtr  = malloc(readBufSize);
    size_t   bytesRead   = 0;

    TLOG("Calling %s for inode %" PRIu64 " offset %" PRIu64 " length %" PRIu64 ", expect status %d and length %zu.\n",funcToTest, fi->inode, offset, length, exp_status, exp_len);
    int err = proxyfs_read(mount_id(), fi->inode, offset, length, readBufPtr, readBufSize, &bytesRead);
    handle_api_return_with_exp_len_and_buf(funcToTest, err, exp_status, bytesRead, exp_len, readBufPtr, exp_buf);

    // Free the read data now that we're done with it
    if (readBufPtr != NULL) {
        free(readBufPtr);
        readBufPtr = NULL;
    }
}

void test_read(file_id_t id, uint64_t offset, uint64_t length, uint8_t* exp_buf, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }


    // Call more generic function; expect to read same length as what was requested
    test_read_past_eof(id, offset, length, exp_buf, length, exp_status);
}

// user callback context
typedef struct {
    pthread_mutex_t      cb_lock;
    proxyfs_io_request_t req;

} test_callback_info_t;

test_callback_info_t* alloc_cb_info(uint64_t in_inode_number,
                                    uint8_t* in_bufptr,
                                    size_t   in_bufsize,
                                    uint64_t in_offset,
                                    uint64_t in_length)
{
    // allocate struct
    test_callback_info_t* cb_info = malloc(sizeof(test_callback_info_t));

    //pthread_mutex_t cb_lock;
    if (pthread_mutex_init(&cb_info->cb_lock, NULL) != 0)
    {
        printf("ERROR, mutex init failed!\n");
        free(cb_info);
        return NULL;
    }

    cb_info->req.op           = IO_NONE;
    cb_info->req.mount_handle = mount_id();
    cb_info->req.inode_number = in_inode_number;
    cb_info->req.offset       = in_offset;
    cb_info->req.length       = in_length;
    cb_info->req.data         = in_bufptr;
    cb_info->req.error        = 9999;
    cb_info->req.out_size     = 0;
    cb_info->req.done_cb      = NULL;
    cb_info->req.done_cb_arg  = NULL;
    cb_info->req.done_cb_fd   = 0;

    return cb_info;
}

void free_cb_info(test_callback_info_t* cb_info)
{
    if (cb_info != NULL) {
        free(cb_info);
    }
}

void set_response_info(test_callback_info_t* cb_info,
                       int                   rsp_status,
                       size_t                out_bufsize)
{
    cb_info->req.error    = rsp_status;
    cb_info->req.out_size = out_bufsize;
}


void do_lock(test_callback_info_t* cb_info, const char* funcName) {
    //TLOG("%s: locking %p.\n", funcName, &cb_info->cb_lock);
    pthread_mutex_lock(&cb_info->cb_lock);
}

void do_unlock(test_callback_info_t* cb_info, const char* funcName) {
    //TLOG("%s: unlocking %p.\n", funcName, &cb_info->cb_lock);
    pthread_mutex_unlock(&cb_info->cb_lock);
}

void cb_wait(test_callback_info_t* cb_info, const char* funcName) {
    //TLOG("%s: locking/unlocking %p.\n", funcName, &cb_info->cb_lock);
    pthread_mutex_lock(&cb_info->cb_lock);
    pthread_mutex_unlock(&cb_info->cb_lock);
}

void test_read_callback(proxyfs_io_request_t* req)
{
    TLOG("%s called with cookie=%p.\n",__FUNCTION__,req->done_cb_arg);

    // Extract cb_info from request
    test_callback_info_t* cb_info = (test_callback_info_t*)req->done_cb_arg;

    do_unlock(cb_info, __FUNCTION__);
}

void test_read_async(file_id_t id, uint64_t offset, uint64_t length, uint8_t* exp_buf, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    // Since we'll be using this file's inode, make sure it is valid
    if (get_inode(id) == 0) { TLOG("%s: ERROR, %s inode=0! Skipping test.\n\n",__FUNCTION__,get_name(id)); return; }


    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    // Do the async read
    char* funcToTest = funcs[AIO_SEND];
    file_info_t* fi = &file_info[id];

    size_t   readBufSize = 1024+length;
    uint8_t* readBufPtr  = malloc(readBufSize);
    size_t   bytesRead   = 0;

    // Save away cb context prior to calling the API
    test_callback_info_t* cb_info = NULL;
    cb_info = alloc_cb_info(fi->inode, readBufPtr, readBufSize, offset, length);
    if (cb_info == NULL) {
        TLOG("Error allocating cb context!\n");
        goto done;
    }

    // Lock the mutex prior to calling the API
    do_lock(cb_info, __FUNCTION__);

    TLOG("Calling %s for inode %" PRIu64 " offset %" PRIu64 ", expect status %d and length %zu.\n",funcToTest, fi->inode, offset, exp_status, length);

    cb_info->req.op          = IO_READ;
    cb_info->req.done_cb     = test_read_callback;
    cb_info->req.done_cb_arg = (void*)cb_info;

    int err = proxyfs_async_io_send(&cb_info->req);

    if (err == 0) {
        // Success; now wait for the callback

        // Wait after calling the API. We should only proceed
        // from here after the callback has completed.
        cb_wait(cb_info, __FUNCTION__);

    } else {
        TLOG("Error %d from %s; no need to wait for callback.\n", err, funcToTest);
        // set returned status in cb_info structure
        cb_info->req.error = err;
        goto check;
    }

    if (err == 0) {
        TLOG("Success from %s; req.error=%d req.out_size=%ld.\n", funcToTest, cb_info->req.error, cb_info->req.out_size);
    } else {
        TLOG("Error %d from %s.\n", err, funcToTest);
    }

check:
    // check stuff
    handle_api_return_with_exp_len_and_buf(funcToTest, cb_info->req.error, exp_status, cb_info->req.out_size, length, cb_info->req.data, cb_info->req.data);

done:
    // Free stuff now that we're done with it
    if (cb_info != NULL) {
        free_cb_info(cb_info);
        cb_info = NULL;
    }
    if (readBufPtr != NULL) {
        free(readBufPtr);
        readBufPtr = NULL;
    }
}

void test_write_callback(proxyfs_io_request_t* req)
{
    TLOG("%s called with cookie=%p.\n",__FUNCTION__, req->done_cb_arg);

    // Extract cb_info from request
    test_callback_info_t* cb_info = (test_callback_info_t*)req->done_cb_arg;

    do_unlock(cb_info, __FUNCTION__);
}

void test_write_async(file_id_t id, uint64_t offset, uint64_t length, uint8_t* buf, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    // Since we'll be using this file's inode, make sure it is valid
    if (get_inode(id) == 0) { TLOG("%s: ERROR, %s inode=0! Skipping test.\n\n",__FUNCTION__,get_name(id)); return; }


    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    // Do the async write
    char* funcToTest = funcs[AIO_SEND];
    file_info_t* fi = &file_info[id];

    // Save away cb context prior to calling the API
    test_callback_info_t* cb_info = NULL;
    cb_info = alloc_cb_info(fi->inode, buf, length, offset, length);
    if (cb_info == NULL) {
        TLOG("Error allocating cb context!\n");
        goto done;
    }

    // Lock the mutex prior to calling the API
    do_lock(cb_info, __FUNCTION__);

    TLOG("Calling %s for inode %" PRIu64 " offset %" PRIu64 " length %" PRIu64 ", expect status %d.\n",funcToTest, fi->inode, offset, length, exp_status);

    cb_info->req.op          = IO_WRITE;
    cb_info->req.done_cb     = test_write_callback;
    cb_info->req.done_cb_arg = (void*)cb_info;

    int err = proxyfs_async_io_send(&cb_info->req);
    if (err == 0) {
        // Success; now wait for the callback

        // Wait after calling the API. We should only proceed
        // from here after the callback has completed.
        cb_wait(cb_info, __FUNCTION__);

    } else {
        TLOG("Error %d from %s; no need to wait for callback.\n", err, funcToTest);
        // set returned status in cb_info structure
        cb_info->req.error = err;
        goto check;
    }

    if (err == 0) {
        TLOG("Success from %s; req.error=%d req.out_size=%ld.\n", funcToTest, cb_info->req.error, cb_info->req.out_size);
    } else {
        TLOG("Error %d from %s.\n", err, funcToTest);
    }

check:
    // check stuff
    handle_api_return_with_exp_len(funcToTest, cb_info->req.error, exp_status, cb_info->req.out_size, length);

done:
    // Free stuff now that we're done with it
    if (cb_info != NULL) {
        free_cb_info(cb_info);
        cb_info = NULL;
    }
}

void test_read_symlink(file_id_t id, char* exp_target, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    // Since we'll be using this file's inode, make sure it is valid
    if (get_inode(id) == 0) { TLOG("%s: ERROR, %s inode=0! Skipping test.\n\n",__FUNCTION__,get_name(id)); return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[READ_SYMLINK];
    file_info_t* fi = &file_info[id];

    TLOG("Calling %s on inode %" PRIu64 ", expect status %d.\n",funcToTest, fi->inode, exp_status);
    const char* target = NULL;
    int err = proxyfs_read_symlink(mount_id(), fi->inode, &target);
    handle_api_return_with_exp_target(funcToTest, err, exp_status, target, exp_target);
    if (target != NULL) {
        free((void*)target);
        target = NULL;
    }
}

void test_read_symlink_path(file_id_t id, char* exp_target, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[READ_SYMLINK_PATH];
    file_info_t* fi = &file_info[id];

    TLOG("Calling %s for fullpath %s, expect status %d\n", funcToTest, fi->fullpath, exp_status);
    const char* target = NULL;
    int err = proxyfs_read_symlink_path(mount_id(), fi->fullpath, &target);
    handle_api_return_with_exp_target(funcToTest, err, exp_status, target, exp_target);
    if (target != NULL) {
        free((void*)target);
        target = NULL;
    }
}

void test_readdir(file_id_t id, int prevDirLoc, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    // Since we'll be using this file's inode, make sure it is valid
    if (get_inode(id) == 0) { TLOG("%s: ERROR, %s inode=0! Skipping test.\n\n",__FUNCTION__,get_name(id)); return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[READDIR];
    file_info_t* fi = &file_info[id];

    TLOG("Calling %s on inode %" PRIu64 " with previous location %d, expect status %d.\n", funcToTest, fi->inode, prevDirLoc, exp_status);

    struct dirent* dir_ent = NULL;
    int err = proxyfs_readdir(mount_id(), fi->inode, prevDirLoc, &dir_ent);
    handle_api_return_with_dirent(funcToTest, err, fi->inode, dir_ent, prevDirLoc, exp_status);

    if (dir_ent != NULL) {
        dir_ent = NULL;
    }
}

// Read all dir entries for a given id
void test_readdir_all(file_id_t id, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    // Since we'll be using this file's inode, make sure it is valid
    if (get_inode(id) == 0) { TLOG("%s: ERROR, %s inode=0! Skipping test.\n\n",__FUNCTION__,get_name(id)); return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[READDIR];
    file_info_t* fi = &file_info[id];

    int64_t prevDirLoc = -1;
    int     err        = 0;
    do {
        // Do a readdir. If we get success, keep doing it again until we get ENOENT
        TLOG("Calling %s on inode %" PRIu64 " with previous location %" PRId64 ", expect status %d.\n", funcToTest, fi->inode, prevDirLoc, exp_status);

        struct dirent* dir_ent = NULL;
        err = proxyfs_readdir(mount_id(), fi->inode, prevDirLoc, &dir_ent);
        handle_api_return_with_dirent(funcToTest, err, fi->inode, dir_ent, prevDirLoc, exp_status);

        if (dir_ent != NULL) {
            prevDirLoc = dir_ent->d_off;
            dir_ent = NULL;
        }
    } while (err != ENOENT);
}

void test_readdir_plus(file_id_t id, int prevDirLoc, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    // Since we'll be using this file's inode, make sure it is valid
    if (get_inode(id) == 0) { TLOG("%s: ERROR, %s inode=0! Skipping test.\n\n",__FUNCTION__,get_name(id)); return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[READDIR_PLUS];
    file_info_t* fi = &file_info[id];

    TLOG("Calling %s on inode %" PRIu64 " with previous location %d, expect status %d.\n", funcToTest, fi->inode, prevDirLoc, exp_status);

    struct dirent* dir_ent = NULL;
    proxyfs_stat_t* stat = NULL;
    int err = proxyfs_readdir_plus(mount_id(), fi->inode, prevDirLoc, &dir_ent, &stat);
    handle_api_return_with_dirent_stat(funcToTest, err, fi->inode, dir_ent, stat, prevDirLoc, exp_status);

    if (dir_ent != NULL) {
        dir_ent = NULL;
    }
    if (stat != NULL) {
        free(stat);
        stat = NULL;
    }
}

// Read all dir entries for a given id
void test_readdir_plus_all(file_id_t id, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    // Since we'll be using this file's inode, make sure it is valid
    if (get_inode(id) == 0) { TLOG("%s: ERROR, %s inode=0! Skipping test.\n\n",__FUNCTION__,get_name(id)); return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[READDIR_PLUS];
    file_info_t* fi = &file_info[id];

    int64_t prevDirLoc = -1;
    int     err        = 0;
    do {
        // Do a readdir. If we get success, keep doing it again until we get ENOENT
        TLOG("Calling %s on inode %" PRIu64 " with previous location %" PRId64 ", expect status %d.\n", funcToTest, fi->inode, prevDirLoc, exp_status);

        struct dirent* dir_ent = NULL;
        proxyfs_stat_t* stat = NULL;
        err = proxyfs_readdir_plus(mount_id(), fi->inode, prevDirLoc, &dir_ent, &stat);
        handle_api_return_with_dirent_stat(funcToTest, err, fi->inode, dir_ent, stat, prevDirLoc, exp_status);

        if (dir_ent != NULL) {
            prevDirLoc = dir_ent->d_off;
            dir_ent = NULL;
        }
        if (stat != NULL) {
            free(stat);
            stat = NULL;
        }

    } while (err != ENOENT);
}

void test_get_stat(file_id_t id, int expectedSize, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    // Since we'll be using this file's inode, make sure it is valid
    if (get_inode(id) == 0) { TLOG("%s: ERROR, %s inode=0! Skipping test.\n\n",__FUNCTION__,get_name(id)); return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[GETSTAT];
    file_info_t* fi = &file_info[id];

    TLOG("Calling %s on inode %" PRIu64 ", expect status %d.\n",funcToTest, fi->inode, exp_status);

    proxyfs_stat_t* stat = NULL;
    int err = proxyfs_get_stat(mount_id(), fi->inode, &stat);
    handle_api_return_with_stat(funcToTest, err, exp_status, fi->inode, fi->mode, fi->uid, fi->gid, stat, expectedSize);

    if (stat != NULL) {
        free(stat);
        stat = NULL;
    }
}

void test_get_stat_path(file_id_t id, int expectedSize, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    // Since we'll be using this file's inode, make sure it is valid
    if (get_inode(id) == 0) { TLOG("%s: ERROR, %s inode=0! Skipping test.\n\n",__FUNCTION__,get_name(id)); return; }


    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[GETSTAT_PATH];
    file_info_t* fi = &file_info[id];

    TLOG("Calling %s on fullpath %s, expect status %d.\n",funcToTest, fi->fullpath, exp_status);

    proxyfs_stat_t* stat = NULL;
    int err = proxyfs_get_stat_path(mount_id(), fi->fullpath, &stat);
    handle_api_return_with_stat(funcToTest, err, exp_status, fi->inode, fi->mode, fi->uid, fi->gid, stat, expectedSize);

    if (stat != NULL) {
        free(stat);
        stat = NULL;
    }
}

void test_get_stat_atime_mtime(file_id_t id, int exp_status, proxyfs_timespec_t* exp_atime, proxyfs_timespec_t* exp_mtime) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    // Since we'll be using this file's inode, make sure it is valid
    if (get_inode(id) == 0) { TLOG("%s: ERROR, %s inode=0! Skipping test.\n\n",__FUNCTION__,get_name(id)); return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[GETSTAT];
    file_info_t* fi = &file_info[id];

    TLOG("Calling %s on inode %" PRIu64 ", expect status %d.\n",funcToTest, fi->inode, exp_status);

    proxyfs_stat_t* stat = NULL;
    int err = proxyfs_get_stat(mount_id(), fi->inode, &stat);
    handle_api_return_with_atime_mtime(funcToTest, err, exp_status, stat, exp_atime, exp_mtime);

    if (stat != NULL) {
        free(stat);
        stat = NULL;
    }
}

void test_set_time(file_id_t id, proxyfs_timespec_t* atime, proxyfs_timespec_t* mtime, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    // Since we'll be using this file's inode, make sure it is valid
    if (get_inode(id) == 0) { TLOG("%s: ERROR, %s inode=0! Skipping test.\n\n",__FUNCTION__,get_name(id)); return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[SET_TIME];
    file_info_t* fi = &file_info[id];

    TLOG("Calling %s on inode %" PRIu64 ", expect status %d.\n",funcToTest, fi->inode, exp_status);

    int err = proxyfs_settime(mount_id(), fi->inode, atime, mtime);
    handle_api_return(funcToTest, err, exp_status);
}

void test_set_time_path(file_id_t id, proxyfs_timespec_t* atime, proxyfs_timespec_t* mtime, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[SET_TIME_PATH];
    file_info_t* fi = &file_info[id];

    TLOG("Calling %s on fullpath %s, expect status %d.\n",funcToTest, fi->fullpath, exp_status);

    int err = proxyfs_settime_path(mount_id(), fi->fullpath, atime, mtime);
    handle_api_return(funcToTest, err, exp_status);
}

void test_statvfs(int expectedSize, int exp_status) {
    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[STATVFS];

    TLOG("Calling %s, expect status %d.\n", funcToTest, exp_status);

    struct statvfs* out_statvfs = NULL;
    int err = proxyfs_statvfs(mount_id(), &out_statvfs);
    handle_api_return_with_statvfs(funcToTest, err, exp_status, expectedSize, out_statvfs);

    if (out_statvfs != NULL) {
        free(out_statvfs);
        out_statvfs = NULL;
    }
}

void test_resize(file_id_t id, uint64_t newSize, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    // Since we'll be using this file's inode, make sure it is valid
    if (get_inode(id) == 0) { TLOG("%s: ERROR, %s inode=0! Skipping test.\n\n",__FUNCTION__,get_name(id)); return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[RESIZE];
    file_info_t* fi = &file_info[id];

    TLOG("Calling %s on inode %" PRIu64 " with size %" PRIu64 ", expect status %d.\n",funcToTest, fi->inode, newSize, exp_status);

    int err = proxyfs_resize(mount_id(), fi->inode, newSize);
    handle_api_return(funcToTest, err, exp_status);
}

void test_type(file_id_t id, uint16_t exp_type, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    // Since we'll be using this file's inode, make sure it is valid
    if (get_inode(id) == 0) { TLOG("%s: ERROR, %s inode=0! Skipping test.\n\n",__FUNCTION__,get_name(id)); return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[TYPE];
    file_info_t* fi = &file_info[id];

    TLOG("Calling %s on inode %" PRIu64 ", expect status %d.\n",funcToTest, fi->inode, exp_status);

    uint16_t file_type = 0;
    int err = proxyfs_type(mount_id(), fi->inode, &file_type);
    handle_api_return_type(funcToTest, err, exp_status, file_type, exp_type);
}

void test_rmdir(file_id_t id, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    // Since we'll be using this file's inode, make sure it is valid
    if (get_inode(id) == 0) { TLOG("%s: ERROR, %s inode=0! Skipping test.\n\n",__FUNCTION__,get_name(id)); return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[RMDIR];
    file_info_t* fi = &file_info[id];

    TLOG("Calling %s for %s (%d), expect status %d\n", funcToTest, fi->basename, (int)id, exp_status);
    int err = proxyfs_rmdir(mount_id(), fi->parent_inode, fi->basename);
    handle_api_return(funcToTest, err, exp_status);
}

void test_rmdir_path(file_id_t id, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[RMDIR_PATH];
    file_info_t* fi = &file_info[id];

    //print_one_file_info(fi);

    TLOG("Calling %s for %s (%d), expect status %d\n", funcToTest, fi->fullpath, (int)id, exp_status);
    int err = proxyfs_rmdir_path(mount_id(), fi->fullpath);
    handle_api_return(funcToTest, err, exp_status);
}

void test_unlink(file_id_t id, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[UNLINK];
    file_info_t* fi = &file_info[id];

    TLOG("Calling %s for %s, expect status %d\n", funcToTest, fi->basename, exp_status);
    int err = proxyfs_unlink(mount_id(), fi->parent_inode, fi->basename);
    handle_api_return(funcToTest, err, exp_status);
}

void test_unlink_path(file_id_t id, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[UNLINK_PATH];
    file_info_t* fi = &file_info[id];

    TLOG("Calling %s for %s, expect status %d\n", funcToTest, fi->fullpath, exp_status);
    int err = proxyfs_unlink_path(mount_id(), fi->fullpath);
    handle_api_return(funcToTest, err, exp_status);
}

void test_rename_path(file_id_t id, char* new_fullpath, int exp_status) {
    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    if (!isEnabled(ERROR_TESTS) && (exp_status != 0)) {
        // We're not running error tests this time
        return;
    }

    char* funcToTest = funcs[RENAME_PATH];
    file_info_t* fi = &file_info[id];

    TLOG("Calling %s for %s, expect status %d\n", funcToTest, fi->fullpath, exp_status);
    int err = proxyfs_rename_path(mount_id(), fi->fullpath, new_fullpath);
    handle_api_return(funcToTest, err, exp_status);

    // Update what we have stored for this record in our test framework
    if (err == 0) {
        update_fullpath(id, new_fullpath);
    }
}


void chown_chmod_tests()
{
    uid_t  uid = 123;
    gid_t  gid = 456;
    mode_t mode = 0744;

    // CHOWN OF FILE

    // Inode-based API: Set a file's mode to something other than what it was
    test_chown(FILE1, uid, gid, 0);
    // do get_stat to check that uid/gid is what we just set it to
    test_get_stat(FILE1, -1, 0);

    // Path-based API: Set a file's uid/gid to something other than what it was
    uid++; gid++;
    test_chown_path(FILE2, uid, gid, 0);
    // do get_stat to check that uid/gid is what we just set it to
    test_get_stat_path(FILE2, -1, 0);

    // CHOWN OF DIRECTORY

    // Inode-based API: Set a dir's mode to something other than what it was
    uid++; gid++;
    test_chown(SUBDIR1, uid, gid, 0);
    // do get_stat to check that uid/gid is what we just set it to
    test_get_stat(SUBDIR1, -1, 0);

    // Path-based API: Set a dir's mode to something other than what it was
    uid++; gid++;
    test_chown_path(SUBSUBDIR1, uid, gid, 0);
    // do get_stat to check that uid/gid is what we just set it to
    test_get_stat_path(SUBSUBDIR1, -1, 0);

    // CHOWN OF SYMLIMK

    // Inode-based API: Set a symlink's uid/gid to something other than what it was
    uid++; gid++;
    test_chown(SYMLINK1, uid, gid, 0);
    // do get_stat to check that uid/gid is what we just set it to
    test_get_stat(SYMLINK1, -1, 0);

    // Path-based API: Set a symlink's uid/gid to something other than what it was
    uid++; gid++;
    test_chown_path(SYMLINK2, uid, gid, 0);
    // do get_stat to check that uid/gid is what we just set it to
    test_get_stat_path(SYMLINK2, -1, 0);

    // chown only uid
    uid++; gid++;
    test_chown(FILE1, uid, -1, 0);
    // do get_stat to check that uid/gid is what we just set it to
    test_get_stat(FILE1, -1, 0);

    // chown only gid
    uid++; gid++;
    test_chown(FILE1, -1, gid, 0);
    // do get_stat to check that uid/gid is what we just set it to
    test_get_stat(FILE1, -1, 0);

    // chown and set uid to zero, should work
    uid++; gid++;
    test_chown(FILE1, 0, gid, 0);
    // do get_stat to check that uid/gid is what we just set it to
    test_get_stat(FILE1, -1, 0);

    // chown and set gid to zero, should work
    uid++; gid++;
    test_chown(FILE1, uid, 0, 0);
    // do get_stat to check that uid/gid is what we just set it to
    test_get_stat(FILE1, -1, 0);

    // chown and set uid or gid to > 65535, should succeed
    uid = 65535 + 2000;
   	gid = 65535 + 10000;
    test_chown(FILE1, uid, gid, 0);
    // do get_stat to check that uid/gid is what we just set it to
    test_get_stat(FILE1, -1, 0);

    // chown neither uid or gid, should return an error
    test_chown(FILE1, -1, -1, EINVAL);
    // do get_stat to check that uid/gid didn't change
    test_get_stat(FILE1, -1, 0);

    // CHMOD OF FILE

    // Inode-based API: Set a file's mode to something other than what it was
    mode = 0513;
    test_chmod(FILE1, mode, mode, 0);
    // do get_stat to check that mode is what we just set it to
    test_get_stat(FILE1, -1, 0);

    // Path-based API: Set a file's mode to something other than what it was
    mode = 0751;
    test_chmod_path(FILE2, mode, mode, 0);
    // do get_stat to check that mode is what we just set it to
    test_get_stat_path(FILE2, -1, 0);

    // CHMOD OF DIRECTORY

    // Inode-based API: Set a dir's mode to something other than what it was
    test_chmod(SUBDIR1, mode, mode, 0);
    // do get_stat to check that mode is what we just set it to
    test_get_stat(SUBDIR1, -1, 0);

    // Path-based API: Set a dir's mode to something other than what it was
    mode = 0456;
    test_chmod_path(SUBSUBDIR1, mode, mode, 0);
    // do get_stat to check that mode is what we just set it to
    test_get_stat_path(SUBSUBDIR1, -1, 0);

    // CHMOD OF SYMLINK

    // Inode-based API: Set a symlink's mode to something other than what it was
    mode = 0603;
    test_chmod(SYMLINK1, mode, mode, 0);
    // do get_stat *on the symlink target* to check that its mode has not changed
    test_get_stat(FILE1, -1, 0);

    // Path-based API: Set a symlink's mode to something other than what it was
    mode = 0176;
    test_chmod_path(SYMLINK2, mode, mode, 0);
    // do get_stat *on the symlink target* to check that mode has not changed
    test_get_stat(FILE1, -1, 0);

    // Try setting an invalid mode (can't be greater than 0x1ff)
    //
    // NOTE: We used to return EINVAL for this case, but now we now silently mask off
    //       the bits outside the valid range.
    mode = 0xfff;
    test_chmod(FILE1, mode, 0x1ff, 0);
    test_chmod_path(FILE1, mode, 0x1ff, 0);
    // Make sure mode hasn't changed
    test_get_stat(FILE1, -1, 0);
}

int mount_tests()
{
    int rc = 0;
    uid_t userid  = 8888;
    gid_t groupid = 1000;

    // Test ENOENT returned for bad volume name
    test_mount(badVolName, 0, userid, groupid, ENOENT);

    // Test being unable to connect to the far end
    set_fault(RPC_CONNECT_FAULT);
    test_mount(volName, 0, userid, groupid, ENODEV);
    clear_fault(RPC_CONNECT_FAULT);

    // Mount A : mount the specified test Volume (must be empty)
    rc = test_mount(volName, 0, userid, groupid, 0);
    if (rc == -1) {
        TLOG("FAILURE, mount failed. Abandoning test suite.\n\n");

        // If we have a null mount handle, there's not much point in trying to continue.
        return -1;
    }

    // Update the ROOT_DIR inode
    update_file_info(ROOT_DIR, 0, root_inode());

    // Save away parts of the real mount handle into a test one
    bad_id_mount_handle.rpc_handle = mount_handle->rpc_handle;

    return 0;
}

void mkdir_create_tests()
{
    mode_t mode = 0774;

    // Mkdir A/B/  : create a subdirectory within Volume
    test_mkdir(SUBDIR1, root_inode(), mount_uid(), mount_gid(), mode, 0);
    // do get_stat to check that mode is what we just set it to
    test_get_stat(SUBDIR1, 0, 0);

    // Try to create the same dir again. Should get EEXIST.
    test_mkdir(SUBDIR1, root_inode(), mount_uid(), mount_gid(), mode, EEXIST);

    // Create #1 A/C   : create and open a normal file within Volume directory
    test_create(FILE1, root_inode(), mount_uid(), mount_gid(), mode, 0);
    // do get_stat to check that mode is what we just set it to
    test_get_stat(FILE1, 0, 0);

    // Try to create the same file again. Should get EEXIST.
    test_create(FILE1, root_inode(), mount_uid(), mount_gid(), mode, EEXIST);

    // Try to create a file, providing bogus dir inode. Should get ENOENT.
    test_create(FILE1, 8888, mount_uid(), mount_gid(), mode, ENOENT);

    // Try to create a file, providing basename that is too big. Should get ENAMETOOLONG.
    test_create(BAD_BASENAME, root_inode(), mount_uid(), mount_gid(), mode, ENAMETOOLONG);

    // Test EINVAL returned for bad mount ID
    set_fault(BAD_MOUNT_ID);
    test_create(FILE1, root_inode(), mount_uid(), mount_gid(), mode, EINVAL);
    clear_fault(BAD_MOUNT_ID);

    // Try to create a dir with the name of an existing file. Should get EEXIST.
    test_mkdir(FILE1, root_inode(), mount_uid(), mount_gid(), mode, EEXIST);

    // Remove file and recreate
    test_unlink(FILE1, 0);
    test_create(FILE1, root_inode(), mount_uid(), mount_gid(), mode, 0);

    // Try to mkdir with an bogus parent inode. Should get ENOENT.
    test_mkdir(SUBDIR2, 1234567890, mount_uid(), mount_gid(), mode, ENOENT);

    // Try to mkdir with a non-dir parent inode. Should get ENOTDIR.
    test_mkdir(SUBDIR1, get_inode(FILE1), mount_uid(), mount_gid(), mode, ENOTDIR);

    // Try to create a directory, providing basename that is too big. Should get ENAMETOOLONG.
    test_mkdir(BAD_BASENAME, root_inode(), mount_uid(), mount_gid(), mode, ENAMETOOLONG);

    // Mkdir A/B/C : create another subdirectory within the previous one
    test_mkdir_path(SUBSUBDIR1, mount_uid(), mount_gid(), mode, 0);

    // Try to mkdir with an invalid parent dir. Should get ENOENT.
    test_mkdir_path(BAD_PARENT_SUBDIR, mount_uid(), mount_gid(), mode, ENOENT);

    // Try to create the same dir again. Should get EEXIST.
    test_mkdir_path(SUBSUBDIR1, mount_uid(), mount_gid(), mode, EEXIST);

    // Try to create a directory, providing basename that is too big. Should get ENAMETOOLONG.
    test_mkdir_path(BAD_BASENAME, mount_uid(), mount_gid(), mode, ENAMETOOLONG);

    // Try to create a directory, providing fullpath that is too big. Should get ENAMETOOLONG.
    test_mkdir_path(BAD_FULLPATH, mount_uid(), mount_gid(), mode, ENAMETOOLONG);

    // Mkdir A/B/  : create a subdirectory within Volume
    test_mkdir(SUBDIR2, root_inode(), mount_uid(), mount_gid(), mode, 0);
    // do get_stat to check that mode is what we just set it to
    test_get_stat(SUBDIR2, 0, 0);

    // Test EINVAL returned for bad mount ID
    set_fault(BAD_MOUNT_ID);
    test_mkdir(SUBDIR1, root_inode(), mount_uid(), mount_gid(), mode, EINVAL);
    clear_fault(BAD_MOUNT_ID);

    // Test EINVAL returned for bad mount ID
    set_fault(BAD_MOUNT_ID);
    test_mkdir_path(SUBSUBDIR1, mount_uid(), mount_gid(), mode, EINVAL);
    clear_fault(BAD_MOUNT_ID);

    // Create another file, with path-based API
    test_create_path(FILE2, mount_uid(), mount_gid(), mode, 0);
    // do get_stat to check that mode is what we just set it to
    test_get_stat_path(FILE2, 0, 0);

    // Test EINVAL returned for bad mount ID
    set_fault(BAD_MOUNT_ID);
    test_create_path(FILE2, mount_uid(), mount_gid(), mode, EINVAL);
    clear_fault(BAD_MOUNT_ID);

    // Try to create a file, providing basename that is too big. Should get ENAMETOOLONG.
    test_create_path(BAD_BASENAME, mount_uid(), mount_gid(), mode, ENAMETOOLONG);

    // Try to create a file, providing fullpath that is too big. Should get ENAMETOOLONG.
    test_create_path(BAD_FULLPATH, mount_uid(), mount_gid(), mode, ENAMETOOLONG);

    // Create      #2 A/B/E : create a normal file within subdirectory
    test_create(SUBDIR_FILE1, get_inode(SUBDIR1), mount_uid(), mount_gid(), mode, 0);
    // do get_stat to check that mode is what we just set it to
    test_get_stat(SUBDIR_FILE1, 0, 0);

    // Create a normal file within subdirectory
    test_create(SUBDIR_FILE2, get_inode(SUBDIR1), mount_uid(), mount_gid(), mode, 0);
    // do get_stat to check that mode is what we just set it to
    test_get_stat(SUBDIR_FILE2, 0, 0);
}

void link_tests()
{
    // Link (inode-based)
    test_link(LINK1, root_inode(), get_inode(FILE1), 0);

    // Try to create the same link again. Should get EEXIST.
    test_link(LINK1, root_inode(), get_inode(FILE1), EEXIST);

    // Try to link a directory. Should get EPERM.
    test_link(LINK2, root_inode(), get_inode(SUBDIR1), EPERM);

    // Try to link with a non-dir parent directory. Should get ENOTDIR.
    test_link(LINK2, get_inode(FILE2), get_inode(SUBDIR_FILE1), ENOTDIR);

    // Try to create a link, providing bogus dir inode. Should get ENOENT.
    test_link(LINK1, 123456789, get_inode(FILE1), ENOENT);

    // Try to create a link, providing bogus target inode. Should get ENOENT.
    test_link(LINK1, root_inode(), 98765432100000, ENOENT);

    // Test EINVAL returned for bad mount ID
    set_fault(BAD_MOUNT_ID);
    test_link(LINK1, root_inode(), get_inode(FILE1), EINVAL);
    clear_fault(BAD_MOUNT_ID);

    // LinkPath
    test_link_path(LINK2, subdirfile1Path, 0);

    // Try to create the same link again. Should get EEXIST.
    test_link_path(LINK2, subdirfile1Path, EEXIST);

    // Try to link a directory. Should get EPERM.
    test_link_path(LINK2, subSubDir1, EPERM);

    // Try to link a non-existent source path. Should get ENOENT.
    test_link_path(BAD_LINK, subSubDir1, ENOENT);

    // Try to link a non-existent target path. Should get ENOENT.
    test_link_path(LINK2, get_fullpath(BAD_INODE), ENOENT);

    // Test EINVAL returned for bad mount ID
    set_fault(BAD_MOUNT_ID);
    test_link_path(LINK2, subdirfile1Path, EINVAL);
    clear_fault(BAD_MOUNT_ID);

    // Symlink A/D->A/C  : create a symlink to the normal file
    test_symlink(SYMLINK1, root_inode(), get_name(FILE1), 0, 0, 0);

    // Get the symlink's inode
    do_lookup(SYMLINK1, 0);

    // do get_stat to check that mode is what we just set it to
    mode_t mode = 0777;
    test_get_stat(SYMLINK1, 0, 0);

    // Try to create the same symlink again. Should get EEXIST.
    test_symlink(SYMLINK1, root_inode(), get_name(FILE1), 0, 0, EEXIST);

    // Try to create a symlink, providing bogus dir inode. Should get ENOENT.
    test_symlink(SYMLINK1, 999888777666, get_name(FILE1), 0, 0, ENOENT);

    // try the path version of the API
    test_symlink_path(SYMLINK2, get_fullpath(FILE1), 0, 0, 0);

    // Try to link a non-existent source path. Should get ENOENT.
    test_symlink_path(BAD_LINK, subSubDir1, 0, 0, ENOENT);

    // Try to link a non-existent target path. Should get ENOENT.
    test_link_path(SYMLINK2, get_fullpath(BAD_INODE), ENOENT);

    // Test EINVAL returned for bad mount ID
    set_fault(BAD_MOUNT_ID);
    test_symlink(SYMLINK1, root_inode(), get_name(FILE1), 0, 0, EINVAL);
    clear_fault(BAD_MOUNT_ID);
}

void lookup_tests()
{
    // Use LookupPath to find the root dir inode
    test_lookup_path(get_name(ROOT_DIR), 0, root_inode());

    // Try LookupPath with "/"
    test_lookup_path(get_fullpath(ROOT_DIR), 0, root_inode());

    // Try LookupPath on the subdirectory, prefixed with "/"
    test_lookup_path(subDir1Slash, 0, get_inode(SUBDIR1));

    // Lookup  #1 A/C   : fetch the inode name of the just created normal file
    test_lookup_path(get_fullpath(FILE1), 0, get_inode(FILE1));

    // proxyfs_lookup_path
    test_lookup_path(get_fullpath(FILE2), 0, get_inode(FILE2));

    // Lookup      #2 A/D   : fetch the inode name of the just created symlink
    do_lookup(SYMLINK1, 0);

    // Lookup      #3 A/B/  : fetch the inode name of the subdirectory
    test_lookup(SUBDIR1, 0);

    // Try to lookup a nonexistent inode, should get ENOENT
    test_lookup(BAD_INODE, ENOENT);

    // Test EINVAL returned for bad mount ID
    set_fault(BAD_MOUNT_ID);
    test_lookup(SUBDIR1, EINVAL);
    clear_fault(BAD_MOUNT_ID);

    // Try to lookup a nonexistent path, should get ENOENT
    test_lookup_path(get_fullpath(BAD_INODE), ENOENT, get_inode(FILE1));

    // Test EINVAL returned for bad mount ID
    set_fault(BAD_MOUNT_ID);
    test_lookup_path(get_name(ROOT_DIR), EINVAL, root_inode());
    clear_fault(BAD_MOUNT_ID);
}

void async_read_write_tests1()
{
    // Write then read back to normal file
    test_write_async(FILE1, 0, sizeof(bufToWrite), (uint8_t*)bufToWrite, 0);
    test_flush(FILE1, 0); // Flush our write so that we can read it
    test_read_async(FILE1, 0, sizeof(bufToWrite), bufToWrite, 0);

    // Try to write an invalid inode. Should get EBADF.
    test_write_async(BAD_INODE, 0, sizeof(bufToWrite), (uint8_t*)bufToWrite, EBADF);

    // Test EINVAL returned for bad mount ID - write
    set_fault(BAD_MOUNT_ID);
    test_write_async(FILE1, 0, sizeof(bufToWrite), (uint8_t*)bufToWrite, EINVAL);
    clear_fault(BAD_MOUNT_ID);

    // Fake out a far end not responding error - write
    set_fault(WRITE_BROKEN_PIPE_FAULT);
    test_write_async(FILE1, 0, sizeof(bufToWrite), (uint8_t*)bufToWrite, ENODEV);
    clear_fault(WRITE_BROKEN_PIPE_FAULT);

    // Test EINVAL returned for bad mount ID - read
    set_fault(BAD_MOUNT_ID);
    test_read_async(FILE1, 0, sizeof(bufToWrite), bufToWrite, EINVAL);
    clear_fault(BAD_MOUNT_ID);

    // Try to read a directory. Should get EISDIR.
    test_read_async(SUBDIR1, 0, sizeof(bufToWrite), bufToWrite, EISDIR);

    // Try to read an invalid inode. Should get EBADF.
    test_read_async(BAD_INODE, 0, sizeof(bufToWrite), bufToWrite, EBADF);

    // Try to call read on a symlink. Should get EISDIR.
    test_read_async(SYMLINK1, 0, sizeof(bufToWrite), bufToWrite, EISDIR);

    // Fake out a far end not responding error - read
    set_fault(WRITE_BROKEN_PIPE_FAULT);
    test_read_async(SYMLINK1, 0, sizeof(bufToWrite), bufToWrite, ENODEV);
    clear_fault(WRITE_BROKEN_PIPE_FAULT);
}

void* async_read_thread(void* work_info)
{
    test_callback_info_t* twork_info = (test_callback_info_t*)work_info;
    TLOG("Spawned read thread, inode %" PRIu64 " offset %" PRIu64 ", length %zu.\n", twork_info->req.inode_number, twork_info->req.offset, twork_info->req.length);

    test_read_async(FILE2, twork_info->req.offset, twork_info->req.length, twork_info->req.data + twork_info->req.offset, 0);

    TLOG("Returning from read thread, inode %" PRIu64 " offset %" PRIu64 ", length %zu.\n", twork_info->req.inode_number, twork_info->req.offset, twork_info->req.length);
}

#define MAX_READ_THREADS 124

void parallel_read_tests(file_id_t id, uint64_t offset, uint64_t length, uint8_t* buf, uint8_t num_threads, bool small_reads)
{
    file_info_t* fi = &file_info[id];
    int          rc = 0;
    int          t  = 0;
    uint64_t     thread_read_offset = 0;
    uint64_t     thread_read_length = 0;

    // Confusing cheat here - use test_callback_info_t to pass info to the threads
    test_callback_info_t* thread_work_info[MAX_READ_THREADS] = { NULL };
    pthread_t             work_thread[MAX_READ_THREADS]      = { 0 };

    // Check that operations on this file are enabled
    if (!get_enabled(id)) { return; }

    // Check test parameters to make sure this is going to work out
    //
    if (num_threads == 0) {
        TLOG("num_threads=0, nothing to do!\n");
        return;
    } else if (num_threads > MAX_READ_THREADS) {
        TLOG("num_threads=%d, is greater than max=%d!\n", num_threads, MAX_READ_THREADS);
        return;
    }

    // Figure out what our read size will be, based on the number of threads
    // and the fact that we don't wan't overlapping reads
    //
    uint64_t max_read_length = length / num_threads;
    if (small_reads) {
        // Small reads; 1k or smaller
        thread_read_length = (max_read_length < 1024) ? max_read_length : 1024;
    } else {
        // Else do the biggest reads we can
        thread_read_length = max_read_length;
    }

    // Spawn num_threads test_read_async's, each to read different offsets of the file
    for (t=0; t < num_threads; t++) {

        thread_work_info[t] = alloc_cb_info(fi->inode, buf, 0, thread_read_offset, thread_read_length);
        if (thread_work_info[t] == NULL) {
            TLOG("Error allocating thread %d context!\n", t);
            goto done;
        }

        // Increment offset so that the next thread's is different and the reads don't overlap
        thread_read_offset += thread_read_length;

        rc = pthread_create(&work_thread[t], NULL, &async_read_thread, (void*)thread_work_info[t]);
        if (rc != 0) {
            TLOG("Error %d spawning thread[%d] to get the response\n", rc, t);
        }
        TLOG("Spawned thread[%d]=%p to read the response\n", t, (void*)work_thread[t]);
    }


    // pthread_join to block here till the threads are done
    for (t=0; t < num_threads; t++) {
        if (work_thread[t] == 0) {
            continue; // We didn't successfully create the thread, skip it
        }
        void* thread_rtn_val = (void*)8888;
        TLOG("Joining thread[%d]=%p to wait for the response\n", t, (void*)work_thread[t]);
        rc = pthread_join(work_thread[t], &thread_rtn_val);
        if (rc != 0) {
            // error joining thread
            TLOG("Error %d joining thread %p\n",rc, (void*)work_thread[t]);
        }
    }

done:
    // Free stuff now that we're done with it
    for (t=0; t < num_threads; t++) {
        if (thread_work_info[t] != NULL) {
            free_cb_info(thread_work_info[t]);
            thread_work_info[t] = NULL;
        }
    }
}

int read_write_tests1()
{
    // Write A/C   : write something to normal file
    test_write(FILE1, 0, sizeof(bufToWrite), (uint8_t*)bufToWrite, 0);
    test_read(FILE1, 0, sizeof(bufToWrite), bufToWrite, 0);

    // Try to write an invalid inode. Should get EBADF.
    test_write(BAD_INODE, 0, sizeof(bufToWrite), (uint8_t*)bufToWrite, EBADF);

    // Test EINVAL returned for bad mount ID
    set_fault(BAD_MOUNT_ID);
    test_write(FILE1, 0, sizeof(bufToWrite), (uint8_t*)bufToWrite, EINVAL);
    clear_fault(BAD_MOUNT_ID);

    // Flush our write so that we can read it
    test_flush(FILE1, 0);

    // Test EINVAL returned for bad mount ID
    set_fault(BAD_MOUNT_ID);
    test_flush(FILE1, EINVAL);
    clear_fault(BAD_MOUNT_ID);

    // Read A/C   : read back what was just written to normal file
    test_read(FILE1, 0, sizeof(bufToWrite), bufToWrite, 0);

    // Test EINVAL returned for bad mount ID
    set_fault(BAD_MOUNT_ID);
    test_read(FILE1, 0, sizeof(bufToWrite), bufToWrite, EINVAL);
    clear_fault(BAD_MOUNT_ID);

    // Try to flush an invalid inode. Should get EBADF.
    test_flush(BAD_INODE, EBADF);

    // Try to read a directory. Should get EISDIR.
    test_read(SUBDIR1, 0, sizeof(bufToWrite), bufToWrite, EISDIR);

    // Try to read an invalid inode. Should get EBADF.
    test_read(BAD_INODE, 0, sizeof(bufToWrite), bufToWrite, EBADF);

    // Try to call read on a symlink. Should get EISDIR.
    test_read(SYMLINK1, 0, sizeof(bufToWrite), bufToWrite, EISDIR);

    // Fake out a far end not responding error
    set_fault(WRITE_BROKEN_PIPE_FAULT);
    test_write(FILE1, 0, sizeof(bufToWrite), (uint8_t*)bufToWrite, ENODEV);
    clear_fault(WRITE_BROKEN_PIPE_FAULT);

    return 0;
}

int read_write_tests2()
{
    char     readBuf[4096+1];
    size_t   totalSize   = 0;
    size_t   ioSize      = 0;
    uint8_t* bigReadBuf  = NULL;
    uint8_t* bufCurrPtr  = NULL;
    uint8_t* bufStartPtr = NULL;
    uint64_t offset      = 0;

    // Read data from file
    FILE* fp = fopen("./randfile", "r");
    if (fp == NULL) {
        TLOG("Error opening randfile, errno=%s\n",strerror(errno));
        return -1;
    }

    // In case we are running this test in a loop, first truncate
    // the file by setting its size to zero.
    test_resize(FILE2, 0, 0);

    // This is the 4k write/read test (bug #127027749, fixed).
    if (isEnabled(WRITEREAD4K_TEST)) {
        ioSize = 4096;
        bufStartPtr = (uint8_t*)readBuf;
        bufCurrPtr  = (uint8_t*)readBuf;

        // Read 4k from randfile and write it to FILE2
        size_t readSize = fread(bufStartPtr, 1, ioSize, fp);
        if (readSize != ioSize) {
            TLOG("Error reading randfile, got %zu bytes, expected %zu\n", readSize, totalSize);
            fclose(fp);
            return -1;
        }
        //fgets(readBuf, ioSize, fp);
        test_write(FILE2, offset, ioSize, bufCurrPtr, 0);

        // Flush our write so that we can read it right away
        test_flush(FILE2, 0);

        // read back what was just written to normal file
        test_read(FILE2, offset, ioSize, bufStartPtr, 0);

        // Check the size of the file
        test_get_stat(FILE2, ioSize, 0);
    }

    // This is the 65k write/read test (bug #126184919, fixed).
    if (isEnabled(WRITEREAD65K_TEST)) {
        totalSize = 65*1024;    // 65k
        ioSize    = 64*1024;    // 64k
        bigReadBuf = malloc(totalSize);
        bufStartPtr = bigReadBuf;
        bufCurrPtr  = bigReadBuf;

        // Read 65k from randfile
        rewind(fp);
        size_t readSize = fread(bufStartPtr, 1, totalSize, fp);
        if (readSize != totalSize) {
            TLOG("Error reading randfile, got %zu bytes, expected %zu\n", readSize, totalSize);
            fclose(fp);
            return -1;
        }

        // Write the first 64k to FILE2
        test_write(FILE2, offset, ioSize, bufCurrPtr, 0);

        // Write the next 1k to FILE2
        offset = ioSize;
        ioSize = 1024;
        bufCurrPtr += offset;
        test_write(FILE2, offset, ioSize, bufCurrPtr, 0);

        // Flush our write so that we can read it right away
        test_flush(FILE2, 0);

        // read back whole file
        test_read(FILE2, 0, totalSize, bufStartPtr, 0);

        // Check the size of the file
        test_get_stat(FILE2, totalSize, 0);

        // NOTE: this test was the one responsible for the jrpcclient_test hang
        //       in regression_test.py. The issue is fixed so the test is enabled
        //       again. Leaving this note here for now just in case.

        // Make just these tests verbose
        if (quiet) {
            proxyfs_set_verbose();
        }

        // Kick off parallel reads of this file and wait till they are done
        int num_threads = 4;

        // Big reads (as big as possible given totalSize and num threads)
        parallel_read_tests(FILE2, 0, totalSize, bigReadBuf, num_threads, false);

        // Small reads (1k or less)
        parallel_read_tests(FILE2, 0, totalSize, bigReadBuf, num_threads, true);

        // Make just these tests verbose
        if (quiet) {
            proxyfs_unset_verbose();
        }

        // Free the buffer
        free(bigReadBuf);
    }

    // Close the file we're reading from
    fclose(fp);
    return 0;
}

int rw_holes_tests()
{
    char     readBuf[4096+1];
    char     zeroBuf[4096+1];
    size_t   totalSize   = 0;
    size_t   ioSize      = 0;
    uint8_t* bigReadBuf  = NULL;
    uint8_t* bufCurrPtr  = NULL;
    uint8_t* bufStartPtr = NULL;
    uint64_t offset      = 0;

    // Read test data from randfile
    ioSize = 4096;
    FILE* fp = fopen("./randfile", "r");
    if (fp == NULL) {
        TLOG("Error opening randfile, errno=%s\n",strerror(errno));
        return -1;
    }
    bufStartPtr = (uint8_t*)readBuf;
    bufCurrPtr  = (uint8_t*)readBuf;
    size_t readSize = fread(bufStartPtr, 1, ioSize, fp);
    if (readSize != ioSize) {
        TLOG("Error reading randfile, got %zu bytes, expected %zu\n", readSize, totalSize);
        fclose(fp);
        return -1;
    }
    fclose(fp);

    // Read zero data from zerofile
    fp = fopen("./zerofile", "r");
    if (fp == NULL) {
        TLOG("Error opening zerofile, errno=%s\n",strerror(errno));
        return -1;
    }
    readSize = fread(zeroBuf, 1, ioSize, fp);
    if (readSize != ioSize) {
        TLOG("Error reading randfile, got %zu bytes, expected %zu\n", readSize, totalSize);
        fclose(fp);
        return -1;
    }
    fclose(fp);


    // Write to file, starting at an offset other than zero
    offset = 2048;
    ioSize = 1024;
    test_write(SUBDIR_FILE1, offset, ioSize, bufCurrPtr, 0);

    // Flush our write so that we can read it right away
    test_flush(SUBDIR_FILE1, 0);

    // Check the size of the file
    test_get_stat(SUBDIR_FILE1, offset+ioSize, 0);

    // Read from an offset less than what we wrote; should get zeros
    offset = 256;
    ioSize = 678;
    test_read(SUBDIR_FILE1, offset, ioSize, zeroBuf, 0);

    if (isEnabled(READPASTEOF_TEST)) {
        // Read more bytes out of file than we wrote, expect to read file size bytes
        offset = 2048+256;
        ioSize = 1024;
        test_read_past_eof(SUBDIR_FILE1, offset, ioSize, bufCurrPtr+256, (1024-256), 0);

        // Attempt to read beyond the end of the file. Expect 0 bytes back.
        offset = 4096;
        ioSize = 1024;
        test_read_past_eof(SUBDIR_FILE1, offset, ioSize, zeroBuf, 0, 0);
    }

    // XXX TODO: Do a read that spans what we wrote and what we didn't

    // XXX TODO: Set the size of an empty file to some length. We should then be able
    //           to read zeros up to that length.

    return 0;
}

// This test is intended to simulate a syslog, where new information is periodically
// appended to the end of the file.
int rw_syslog_tests()
{
    if (!isEnabled(SYSLOGWRITE_TEST)) {
        return 0;
    }

    size_t   totalSize = 65*1024;    // 65k
    uint8_t* wbuf      = malloc(totalSize);
    uint64_t done      = 0;
    int      rtnVal    = 0;

    // Read test data from randfile
    FILE* fp = fopen("./randfile", "r");
    if (fp == NULL) {
        TLOG("Error opening randfile, errno=%s\n",strerror(errno));
        rtnVal = -1;
        goto done;
    }
    size_t readSize = fread(wbuf, 1, totalSize, fp);
    if (readSize != totalSize) {
        TLOG("Error reading randfile, got %zu bytes, expected %zu\n", readSize, totalSize);
        fclose(fp);
        rtnVal = -1;
        goto done;
    }
    fclose(fp);

    while (done < totalSize) {
        int count = 1024;
        if (done + count > totalSize) {
            count -= 1024;
            count = totalSize - done;
        }

        // Write to file
        //int ret = write(fd, buf, count);
        test_write(SUBDIR_FILE2, done, count, wbuf, 0);

        // read back what was just written
        //ret = read(fd, rbuf, count);
        test_read(SUBDIR_FILE2, done, count, wbuf, 0);

        done += count;
    }

done:
    free(wbuf);
    return rtnVal;
}

void read_symlink_tests()
{
    // Readsymlink    A/D   : read the symlink to ensure it points to the normal file
    test_read_symlink(SYMLINK1, file1, 0);

    // Do the same thing again using the path-based API
    test_read_symlink_path(SYMLINK1, file1, 0);

    // Call Readsymlink on a file inode, expect EINVAL
    test_read_symlink(FILE1, file1, EINVAL);

    // Call Readsymlink on a dir inode, expect EINVAL
    test_read_symlink(SUBDIR1, file1, EINVAL);

    // Try to read_symlink on an invalid inode. Should get ENOENT.
    test_read_symlink(BAD_INODE, file1, ENOENT);

    // Try to read_symlink on a non-existent path, expect ENOENT.
    test_read_symlink_path(BAD_INODE, file1, ENOENT);

    // Test EINVAL returned for bad mount ID
    set_fault(BAD_MOUNT_ID);
    test_read_symlink_path(SYMLINK1, file1, EINVAL);
    clear_fault(BAD_MOUNT_ID);
}

void readdir_tests()
{
    // Readdir     #1 A/B/ (prev == "",  max_entries == 0) : ensure we get only ".", "..", and "E"
    test_readdir_all(ROOT_DIR, -1);
    test_readdir(ROOT_DIR, 5, 0);
    test_readdir(ROOT_DIR, 99, ENOENT);

    // Try to readdir a regular file. Should get ENOTDIR.
    test_readdir(SUBDIR_FILE1, 0, ENOTDIR);

    // Test EINVAL returned for bad mount ID
    set_fault(BAD_MOUNT_ID);
    test_readdir(ROOT_DIR, 0, EINVAL);
    clear_fault(BAD_MOUNT_ID);

    // Readdir     #2 A/   (prev == "",  max_entries == 3) : ensure we get only ".", ".." & "B"
    test_readdir_plus_all(ROOT_DIR, -1);
    test_readdir_plus(ROOT_DIR, 5, 0);
    test_readdir_plus(ROOT_DIR, 99, ENOENT);

    // Try to readdir a symlink. Should get ENOTDIR.
    test_readdir_plus(SYMLINK1, 0, ENOTDIR);

    // Test EINVAL returned for bad mount ID
    set_fault(BAD_MOUNT_ID);
    test_readdir_plus(ROOT_DIR, 0, EINVAL);
    clear_fault(BAD_MOUNT_ID);

    // Readdir     #3 A/   (prev == "B", max_entries == 3) : ensure we get only "C" & "D"
}

void stat_size_type_tests()
{
    // Getstat     #1 A/C  : check the current size of the normal file
    test_get_stat(FILE1, sizeof(bufToWrite), 0);

    // Getstat on a file that doesn't exist
    test_get_stat(BAD_INODE, -1, ENOENT);

    // Test EINVAL returned for bad mount ID
    set_fault(BAD_MOUNT_ID);
    test_get_stat(FILE1, sizeof(bufToWrite), EINVAL);
    clear_fault(BAD_MOUNT_ID);

    // Resize         A/C  : truncate the file
    test_resize(FILE1, 0, 0);

    // Truncate, something other than zero
    test_resize(FILE2, 2048, 0);
    test_get_stat(FILE2, 2048, 0);

    // Attempt resize on a directory inode, expect EISDIR
    test_resize(SUBDIR1, 0, EISDIR);

    // Test EINVAL returned for bad mount ID
    set_fault(BAD_MOUNT_ID);
    test_resize(FILE1, 0, EINVAL);
    clear_fault(BAD_MOUNT_ID);

    // Getstat     #2 A/C  : verify the size of the normal file is now zero
    test_get_stat_path(FILE1, 0, 0);

    // Test EINVAL returned for bad mount ID
    set_fault(BAD_MOUNT_ID);
    test_get_stat_path(FILE1, 0, EINVAL);
    clear_fault(BAD_MOUNT_ID);

    // Type
    test_type(FILE1, DT_REG, 0);

    // Test EINVAL returned for bad mount ID
    set_fault(BAD_MOUNT_ID);
    test_type(FILE1, DT_REG, EINVAL);
    clear_fault(BAD_MOUNT_ID);

    // Set atime/mtime
    proxyfs_timespec_t mtime = {999, 77777};
    proxyfs_timespec_t atime = {666, 22222};
    test_set_time(FILE1, &atime, &mtime, 0);

    // Test EINVAL returned for bad mount ID
    set_fault(BAD_MOUNT_ID);
    test_set_time(FILE1, &atime, &mtime, EINVAL);
    clear_fault(BAD_MOUNT_ID);

    // Check the atime/ntime we set
    test_get_stat_atime_mtime(FILE1, 0, &atime, &mtime);

    // Path-based Set atime/mtime
    proxyfs_timespec_t mtime2 = {123, 45678};
    proxyfs_timespec_t atime2 = {888, 77777};
    test_set_time_path(FILE1, &atime2, &mtime2, 0);

    // Test EINVAL returned for bad mount ID
    set_fault(BAD_MOUNT_ID);
    test_set_time_path(FILE1, &atime2, &mtime2, EINVAL);
    clear_fault(BAD_MOUNT_ID);

    // Check the atime/ntime we set
    test_get_stat_atime_mtime(FILE1, 0, &atime2, &mtime2);
}

void rename_tests()
{
    char* oldName = get_fullpath(SUBDIR_FILE1);

    // Rename
    sprintf(newName, "%sWoohoo", get_fullpath(SUBDIR_FILE1));
    test_rename_path(SUBDIR_FILE1, newName, 0);

    // Check that the cache was updated correctly
    if (strcmp(newName, get_fullpath(SUBDIR_FILE1)) != 0) {
        char* funcToTest = funcs[RENAME_PATH];
        test_failed(funcToTest);
        TLOG("FAILURE, after test %s cache was not updated to %s as expected.\n\n",funcToTest,newName);
    }

    // lookup to make sure the rename worked
    test_lookup_path(get_fullpath(SUBDIR_FILE1), 0, get_inode(SUBDIR_FILE1));

    // Test EINVAL returned for bad mount ID
    set_fault(BAD_MOUNT_ID);
    test_rename_path(SUBDIR_FILE1, newName, EINVAL);
    clear_fault(BAD_MOUNT_ID);

    // lookup the old name, should fail
    test_lookup_path(oldName, ENOENT, get_inode(SUBDIR_FILE1));
}

void unlink_rmdir_tests()
{
    // Rmdir => removing subDir2
    test_rmdir(SUBDIR2, 0);

    // Try to rmdir on subdirectory that isn't empty
    test_rmdir(SUBDIR1, ENOTEMPTY);

    // Test EINVAL returned for bad mount ID
    set_fault(BAD_MOUNT_ID);
    test_rmdir(SUBDIR2, EINVAL);
    clear_fault(BAD_MOUNT_ID);

    // Try to rmdir on file
    test_rmdir_path(SUBDIR_FILE1, ENOTDIR);

    // Test EINVAL returned for bad mount ID
    set_fault(BAD_MOUNT_ID);
    test_rmdir_path(SUBDIR_FILE1, EINVAL);
    clear_fault(BAD_MOUNT_ID);

    // Unlink      #1 A/B/E  : delete the normal file within the subdirectory
    test_unlink_path(SUBDIR_FILE1, 0);

    // Unlink
    test_unlink_path(SUBDIR_FILE2, 0);

    // proxyfs_unlink on normal file
    test_unlink(FILE1, 0);

    // Unlink      #3 A/C  : delete the normal file
    test_unlink_path(FILE2, 0);

    if (isEnabled(LINK_TESTS)) {
        // Delete the hard link
        test_unlink(LINK1, 0);

        // Delete the other hard link
        test_unlink_path(LINK2, 0);
    }

    // Try to unlink a subdirectory
    test_unlink_path(SUBSUBDIR1, EISDIR);

    // Test EINVAL returned for bad mount ID
    set_fault(BAD_MOUNT_ID);
    test_unlink_path(LINK2, EINVAL);
    clear_fault(BAD_MOUNT_ID);

    if (isEnabled(LINK_TESTS)) {
        // Unlink      #2 A/D : delete the symlink
        test_unlink(SYMLINK1, 0);
    }

    // Test EINVAL returned for bad mount ID
    set_fault(BAD_MOUNT_ID);
    test_unlink(SYMLINK1, EINVAL);
    clear_fault(BAD_MOUNT_ID);

    if (isEnabled(LINK_TESTS)) {
        // Unlink : delete the other symlink
        test_unlink_path(SYMLINK2, 0);
    }

    // Rmdir => removing subSubDir1
    test_rmdir_path(SUBSUBDIR1, 0);

    // Rmdir => removing subDir1
    test_rmdir(SUBDIR1, 0);
}

void unmount_tests()
{
    // Unmount        A  : unmount the Volume
    char* funcToTest = funcs[UNMOUNT];
    TLOG("Calling %s.\n",funcToTest);
    proxyfs_unmount(mount_id());

}

void statvfs_tests()
{
    test_statvfs(-1, 0);
}

// XXX TODO - Tests to be added:
//
// + 65M read/write
// + truncate tests (both when not yet flushed and when flushed)
// + read beyond end of file
// + write causing holes

void print_usage() {
    printf("Run ProxyFS RPC client-side tests.\n\n");
    printf("Usage: test [-h|-v|-s|-p <jsonrpc-port-number>|-t <test_name>]\n");
    printf("       When called with no parameters, the entire suite of tests is run.\n");
    printf("       -h: print this message.\n");
    printf("       -v: verbose mode; more test output.\n");
    printf("       -s: silent mode; no test output. Intended for performance testing.\n");
    printf("       -o: specify JSON RPC server info as <ipaddr>:<port>/<fast_port> to use.\n");
    printf("       -t: run a subset of tests, intended to reproduce specific issues.\n");
    printf("           Test subsets supported are:\n");
    printf("            4kread\n");
    printf("            65kread\n");
    printf("            read-past-eof\n");
    printf("            syslog-write\n");
    printf("            mount\n");
    printf("            chown\n");
    printf("            mkdir\n");
    printf("            readdir\n");
    printf("            async\n");
    printf("            parallel\n");
    printf("            statvfs\n");
    printf("            fake_hang\n");
}

int main(int argc, char *argv[])
{
    int c = 0;
    char* tvalue = NULL;
    opterr = 0;
    bool testsSuiteAborted = false;
    bool fakeHang          = false;

    // See if the caller passed in any flags
    while ((c = getopt(argc, argv, "hvst:o:")) != -1) {
        switch (c)
        {
            case 'h':
                print_usage();
                return 0;
                break;
            case 'v':
                // Caller  wants debug prints from proxyfs code
                quiet = false;
                break;
            case 's':
                // Caller  silent mode, no prints at all
                silent = true;
                quiet  = true;
                disable_fault_prints();
                break;
            case 'o':
                // Caller wants to override config file
                rpc_config_override(optarg);
                break;
            case 't':
                // Caller wants to run a specific test
                //
                tvalue = optarg;

                if (strcmp(tvalue,"4kread") == 0) {
                    enableTest(WRITEREAD4K_TEST);

                    // Disable some of the other tests to get to the one we want quicker
                    disableTest(ERROR_TESTS);
                    disableTest(LOOKUP_TESTS);
                    disableTest(READSYMLINK_TESTS);
                    disableTest(READDIR_TESTS);
                    disableTest(STATSIZE_TESTS);

                } else if ((strcmp(tvalue,"65kread") == 0) || (strcmp(tvalue,"parallel") == 0)) {
                    disableAllTests();

                    enableTest(MOUNT_TESTS);
                    enableTest(WRITEREAD65K_TEST);
                    enableTest(MKDIRCREATE_TESTS);
                    enableTest(UNLINKRMDIR_TESTS);

                    disable_all_files();
                    enable_file(FILE2);

                } else if (strcmp(tvalue,"read-past-eof") == 0) {
                    disableAllTests();

                    enableTest(READWRITE_TESTS);
                    enableTest(READPASTEOF_TEST);
                    enableTest(MOUNT_TESTS);
                    enableTest(ERROR_TESTS);
                    enableTest(MKDIRCREATE_TESTS);
                    enableTest(UNLINKRMDIR_TESTS);

                    disable_all_files();
                    enable_file(FILE1);
                    enable_file(SUBDIR1);
                    enable_file(SUBDIR_FILE1);

                } else if (strcmp(tvalue,"syslog-write") == 0) {
                    enableTest(SYSLOGWRITE_TEST);

                    // Disable some of the other tests to get to the one we want quicker
                    disableTest(ERROR_TESTS);
                    disableTest(LOOKUP_TESTS);
                    disableTest(READSYMLINK_TESTS);
                    disableTest(READDIR_TESTS);
                    disableTest(STATSIZE_TESTS);

                } else if (strcmp(tvalue,"mount") == 0) {
                    disableAllTests();
                    enableTest(MOUNT_TESTS);
                    enableTest(ERROR_TESTS);

                    disable_all_files();

                } else if (strcmp(tvalue,"chown") == 0) {
                    disableAllTests();
                    enableTest(CHOWNCHMOD_TESTS);
                    enableTest(ERROR_TESTS);
                    enableTest(MOUNT_TESTS);
                    enableTest(MKDIRCREATE_TESTS);
                    enableTest(LINK_TESTS);
                    enableTest(UNLINKRMDIR_TESTS);

                } else if (strcmp(tvalue,"mkdir") == 0) {
                    disableAllTests();
                    enableTest(MOUNT_TESTS);
                    enableTest(ERROR_TESTS);
                    enableTest(MKDIRCREATE_TESTS);
                    enableTest(UNLINKRMDIR_TESTS);

                } else if (strcmp(tvalue,"readdir") == 0) {
                    disableAllTests();
                    enableTest(READDIR_TESTS);
                    enableTest(MOUNT_TESTS);
                    enableTest(ERROR_TESTS);
                    enableTest(MKDIRCREATE_TESTS);
                    enableTest(LINK_TESTS);
                    enableTest(UNLINKRMDIR_TESTS);

                } else if (strcmp(tvalue,"async") == 0) {
                    disableAllTests();
                    enableTest(ASYNC_READWRITE_TESTS);
                    enableTest(MOUNT_TESTS);
                    enableTest(ERROR_TESTS);
                    enableTest(MKDIRCREATE_TESTS);
                    enableTest(LINK_TESTS);
                    enableTest(UNLINKRMDIR_TESTS);

                    disable_all_files();
                    enable_file(FILE1);
                    enable_file(SUBDIR1);
                    enable_file(SUBDIR_FILE1);
                    enable_file(SYMLINK1);
                    enable_file(BAD_INODE);

                } else if (strcmp(tvalue,"statvfs") == 0) {
                    disableAllTests();
                    enableTest(MOUNT_TESTS);
                    //enableTest(ERROR_TESTS);
                    enableTest(STATVFS_TESTS);

                } else if (strcmp(tvalue,"fake_hang") == 0) {
                    fakeHang = true;

                } else {
                    printf("Unknown test supplied: %s\n\n",tvalue);
                    print_usage();
                    return -1;
                }
                break;
            case '?':
                if ((optopt == 't') || (optopt == 'o')) {
                    printf("Option -%c requires an argument.\n", optopt);
                } else if (isprint(optopt)) {
                    printf("Unknown option `-%c'.\n", optopt);
                } else {
                    printf("Unknown option character `\\x%x'.\n", optopt);
                }
                return 1;
            default:
                abort ();
        }
    }

    // Caller doesn't want debug prints from proxyfs code
    if (quiet) {
        proxyfs_unset_verbose();
    } else {
        proxyfs_set_verbose();
    }

    // Initialize string stuff
    init_globals();

    // XXX TODO: Add a test for ENODEV

    // Run mount tests
    if (isEnabled(MOUNT_TESTS)) {
        int err = mount_tests();
        if (err != 0) {
            // If we have a null mount handle, there's not much point in trying to continue.
            testsSuiteAborted = true;
            goto done;
        }
    }

    // Generate test begin log on proxyfs
    // NOTE: For now, has to be after mount because that's where our socket handle is stored
    test_log("JRPCCLIENT TEST START");

    // Run file/dir/symlink create tests
    if (isEnabled(MKDIRCREATE_TESTS)) {
        mkdir_create_tests();
        if (isEnabled(LINK_TESTS)) {
            link_tests();
        }
    }

    // Optionally dump out file info
    //  print_file_info();

    // Run lookup tests
    if (isEnabled(LOOKUP_TESTS)) {
        lookup_tests();
    }

    // Test read/write/flush
    if (isEnabled(READWRITE_TESTS)) {
        if (read_write_tests1() != 0) {
            TLOG("ERROR in read/write tests. Abandoning test suite.\n\n");
            testsSuiteAborted = true;
            goto done;
        }
        if (rw_holes_tests() != 0) {
            TLOG("ERROR in read/write tests. Abandoning test suite.\n\n");
            testsSuiteAborted = true;
            goto done;
        }
    }
    // Test 4k read/write, 65k read/write, parallel read/write
    int i = 0;
    for (i=0; i<10; i++) {
        if (read_write_tests2() != 0) {
            TLOG("ERROR in read/write tests. Abandoning test suite.\n\n");
            testsSuiteAborted = true;
            goto done;
        }
    }

    if (rw_syslog_tests() != 0) {
        TLOG("ERROR in read/write tests. Abandoning test suite.\n\n");
        testsSuiteAborted = true;
        goto done;
    }

    // Test async read/write
    if (isEnabled(ASYNC_READWRITE_TESTS)) {
        async_read_write_tests1();
    }

    // Test read_symlink
    if (isEnabled(READSYMLINK_TESTS)) {
        read_symlink_tests();
    }

    // Test readdir
    if (isEnabled(READDIR_TESTS)) {
        readdir_tests();
    }

    // Stat tests
    if (isEnabled(STATSIZE_TESTS)) {
        stat_size_type_tests();
    }

    // Statvfs A : should report A has 4 "files" (normal & symlink) and 1 directory "ideally"
    if (isEnabled(STATVFS_TESTS)) {
        statvfs_tests();
    }

    // Test rename
    if (isEnabled(RENAME_TESTS)) {
        rename_tests();
    }

    // Chown and chmod
    if (isEnabled(CHOWNCHMOD_TESTS)) {
        chown_chmod_tests();
    }

    // cleanup: unlink, rmdir, unmount
    if (isEnabled(UNLINKRMDIR_TESTS)) {
        unlink_rmdir_tests();
    }

    // For test debug purposes: fake a hang
    if (fakeHang) {
        TLOG("Simulating test hang!");
        sleep(30);
    }

    // Generate test end log on proxyfs
    // NOTE: For now, has to be before unmount because that's where our socket handle is stored
    test_log("JRPCCLIENT TEST END");

    if (isEnabled(MOUNT_TESTS)) {
        unmount_tests();
    }

done:
    TLOG("Done.\n\n");
    print_passed_failed();

    if ((num_failed() == 0) && (!testsSuiteAborted)) {
        return 0;
    } else {
        return 1;
    }
}
