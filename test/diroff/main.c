/**
 * Tests to verify that d_off for direction entries from readdir(3)
 * work correctly with seekdir(3)/telldir(3).
 *
 * This test assumes its run on a directory with at least 5 entires
 * (but more would be better).
 */
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <dirent.h>
#include <string.h>
#include <errno.h>
#include <utime.h>
#include <sys/types.h>
#include <sys/stat.h>

char const *    cmdname;
long int        seedval;

void            readdir_stable_tests(char const * dirname);

void
usage()
{
    fprintf(stderr, "Usage: %s <directory_path>\n", cmdname);
    exit(2);
}

int
main(
    int         argc,
    char **     argv)
{
    // strip leading pathname components from cmdname
    cmdname = strrchr(argv[0], '/');
    if (cmdname != NULL) {
        cmdname++;
    } else {
        cmdname = argv[0];
    }

    // only one argument expected
    if (argc != 2) {
        usage();
        /* NOTREACHED */
    }

    // choose a seed for the random number generator so the tests are different
    // each time
    seedval = getpid();
    srand48(seedval);

    readdir_stable_tests(argv[1]);

    // if we get here the tests succeeded
    exit(0);
}

/**
 * @brief Called when one of the readdir_stable_tests fails to bring a message.
 *
 * Given a stat(2) of the directory taken before the test starts it also checks
 * to see if the directory was modified which, if true, could explain the
 * failure.
 *
 * The intention is that this can be run on all the directories in a live system
 * and be somewhat tolerant of directories that change while its running.
 */
void
readdir_stable_fail(
    char const *        dirname,
    struct stat *       dirstat_origp,
    char *              msg,
    ...)
{
    char                msgbuf[1024];
    int                 rv;
    va_list             argp;

    va_start(argp, msg);
    rv = vsnprintf(msgbuf, sizeof(msgbuf), msg, argp);
    va_end(argp);

    if (rv < 0) {
        fprintf(stderr, "%s: FAIL: Bad msg passed to readdir_stable_fail() '%s': %s\n",
                cmdname, msg, strerror(errno));
        exit(3);
    }
    if (rv >= sizeof(msgbuf)) {
        fprintf(stderr, "%s: FAIL: msg passed to readdir_stable_fail() '%s' requires %d bytes to print!\n",
                cmdname, msg, rv);
        exit(3);
    }

    struct stat         dirstat;
    rv = stat(dirname, &dirstat);
    if (rv < 0) {
        // another process might have removed the directory or changed
        // permissions on the path so perhaps this should be a warning?
        fprintf(stderr, "%s: FAIL: readdir_stable_fail() stat of '%s' failed: %s\n",
                cmdname, dirname, strerror(errno));
        exit(3);
    }

    // if the directory changed that may explain the failure so treat it as a
    // warning; otherwise its an error (the size can't change without st_ctim
    // changing but we check that anyway)
    if (dirstat.st_ctim.tv_sec != dirstat_origp->st_ctim.tv_sec ||
        dirstat.st_ctim.tv_nsec != dirstat_origp->st_ctim.tv_nsec ||
        dirstat.st_size != dirstat_origp->st_size) {

        fprintf(stderr, "%s: WARN: readdir_stable directory '%s' changed "
                "while test was running; treating failure as a warning\n",
                cmdname, dirname);
        fprintf(stderr, "%s: WARN: readdir_stable directory '%s': %s\n",
                cmdname, dirname, msgbuf);
    } else {
        fprintf(stderr, "%s: FAIL: readdir_stable directory '%s': %s\n",
                cmdname, dirname, msgbuf);
    }

}

/**
 * @brief Test readdir()/seekdir()/telldir() along with d_off in a directory
 * that is not changing (no changes are made to the directory).
 *
 * Read the contents of the directory collecting all of the struct dirent and
 * then validate their contents, including the ability seek to @a d_off and
 * get the same directory entry.
 */
void
readdir_stable_tests(
    char const *        dirname)
{
    struct stat         dirstat;
    int                 rc;
    DIR *               dirp;

    rc = stat(dirname, &dirstat);
    if (rc < 0) {
        readdir_stable_fail(dirname, &dirstat, "stat(%s) failed: %s", dirname, strerror(errno));
        /* NOTREACHED */
        usage();
    }

    dirp = opendir(dirname);
    if (dirp == NULL) {
        readdir_stable_fail(dirname, &dirstat, "opendir(%s) failed: %s", dirname, strerror(errno));
        /* NOTREACHED */
        usage();
    }

    // count the number of directory entries
    int                 dirent_cnt = 0;
    struct dirent *     direntp = NULL;

    errno = 0;
    while (direntp = readdir(dirp) ) {
        dirent_cnt++;
    }
    if (errno != 0) {
        readdir_stable_fail(dirname, &dirstat, "readdir() failed after %d entries: %s",
                            dirent_cnt, strerror(errno));
        return;
    }

    // create a map of offset, dirent. it is annoying, but the maximum size of
    // d_name is poorly defined and may exceed 256 bytes (NAME_MAX + 1) on some
    // file systems (like NTFS) so we can't rely on dirent.d_name being large
    // enough in all cases!  hence, dm_name.
    struct dirent_map {
        off_t           dm_off;
        struct dirent   dm_dirent;
        char *          dm_name;
    };

    struct dirent_map * dentmap = calloc(sizeof(struct dirent_map), dirent_cnt + 1);
    int                 i;

    rewinddir(dirp);
    for (i = 0; i < dirent_cnt + 1; i++) {

        off_t           diroff = telldir(dirp);
        if (diroff < 0) {
            readdir_stable_fail(dirname, &dirstat, "telldir() before readdir failed after %d entries: %s",
                                dirent_cnt, strerror(errno));
            return;
        }
        dentmap[i].dm_off = diroff;

        errno = 0;
        direntp = readdir(dirp);
        if (direntp == NULL) {
            break;
        }

        // do not assume memcpy() copies all of d_name! the name might exceed sizeof(d_name)
        memcpy(&dentmap[i].dm_dirent, direntp, sizeof(struct dirent));
        dentmap[i].dm_name = strdup(direntp->d_name);

        // validate d_off
        diroff = telldir(dirp);
        if (diroff < 0) {
            readdir_stable_fail(dirname, &dirstat, "telldir() after readdir failed after %d entries: %s",
                                dirent_cnt, strerror(errno));
            return;
        }
        if (diroff != dentmap[i].dm_dirent.d_off) {
            readdir_stable_fail(dirname, &dirstat, "telldir() offset %ld != dirent d_off %ld for entry '%s'",
                                diroff, dentmap[i].dm_dirent.d_off, dentmap[i].dm_name);
            return;
        }
    }
    if (i != dirent_cnt) {
        readdir_stable_fail(dirname, &dirstat,
                            "second readdir expected %d directory entries but read %d entries; possible error: %s",
                            dirent_cnt, i, strerror(errno));
            return;
    }


    // cause the client to dump its cache and Ganesha as well by changing the
    // directory underneath Ganesha (via the FUSE mountpoint) and waiting long
    // enough for it to notice.
    int                 rv;

    rv = utime("/CommonMountPoint/test", NULL);
    if (rv < 0) {
        readdir_stable_fail(dirname, &dirstat, "utime(/CommonMountPoint/test) failed: %s",
                            strerror(errno));
        return;
    }
    sleep(3);

    /*
     * seek to each returned dirent offset and verify we get the correct entry
     */

    // randomly shuffle the entries
    for (i = 0; i < dirent_cnt; i++) {
        int                     j = lrand48() % dirent_cnt;
        struct dirent_map       tmp;

        tmp = dentmap[i];
        dentmap[i] = dentmap[j];
        dentmap[j] = tmp;
    }

    // read the directory entries by offset and verify they match
    for (i = 0; i < dirent_cnt; i++) {
        seekdir(dirp, dentmap[i].dm_off);

        errno = 0;
        direntp = readdir(dirp);
        if (direntp == NULL) {
            readdir_stable_fail(dirname, &dirstat, "telldir() readdir for entry '%s' failed at off %ld",
                                dentmap[i].dm_name, dentmap[i].dm_off, strerror(errno));
            return;
        }


        rv = utime("/CommonMountPoint/test", NULL);
        if (rv < 0) {
            readdir_stable_fail(dirname, &dirstat, "utime(/CommonMountPoint/test) failed: %s",
                                strerror(errno));
            return;
        }
        sleep(3);
    }
}
