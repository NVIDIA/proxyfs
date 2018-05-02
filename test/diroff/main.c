/**
 * Tests to verify that d_off for direction entries from readdir(3)
 * work correctly with seekdir(3)/telldir(3).
 *
 * This test assumes its run on a directory with at least 5 entires
 * (but more would be better).
 */
#include <stdlib.h>
#include <stdio.h>
#include <dirent.h>
#include <string.h>
#include <errno.h>

char const *    cmdname;

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

    char const *        dirname = argv[1];
    DIR *               dirp = opendir(dirname);

    if (dirp == NULL) {
        fprintf(stderr, "%s: FAIL: opendir('%s') failed: %s\n",
                cmdname, dirname, strerror(errno));
        usage();
        /* NOTREACHED */
    }

    // count the number of directory entries
    int                 direntries = 0;
    struct dirent *     direntp = NULL;

    errno = 0;
    while (direntp = readdir(dirp) ) {
        direntries++;
    }
    if (errno != 0) {
        fprintf(stderr, "%s: FAIL: readdir() failed after %d entries: %s\n",
                cmdname, direntries, strerror(errno));
        exit(3);
    }

    // create a map of offset, dirent; it is annoying, but the maximum size of
    // d_name is poorly defined and may exceed 256 bytes (NAME_MAX + 1) on some
    // file systems (like NTFS) so we can't ely on dm_dirent.d_name being large
    // enough in all cases!  hence, dm_name.
    struct dirent_map {
        off_t           dm_off;
        struct dirent   dm_dirent;
        char *          dm_name;
    };

    rewinddir(dirp);

    struct dirent_map * dentmap = calloc(sizeof(struct dirent_map), direntries + 1);
    int                 i;
    for (i = 0; i < direntries + 1; i++) {
        off_t           diroff = telldir(dirp);
        if (diroff < 0) {
            fprintf(stderr, "%s: FAIL: telldir() pre-readdir failed after %d entries: %s\n",
                    cmdname, i, strerror(errno));
            exit(3);
        }
        dentmap[i].dm_off = diroff;

        errno = 0;
        direntp = readdir(dirp);
        if (direntp == NULL) {
            break;
        }

        // do not assume this copies all of d_name!
        memcpy(&dentmap[i].dm_dirent, direntp, sizeof(struct dirent));
        dentmap[i].dm_name = strdup(direntp->d_name);

        // check d_off
        diroff = telldir(dirp);
        if (diroff < 0) {
            fprintf(stderr, "%s: FAIL: telldir() post-readdir failed after %d entries: %s\n",
                    cmdname, i, strerror(errno));
            exit(3);
        }
        if (diroff != dentmap[i].dm_dirent.d_off) {
            fprintf(stderr, "%s: FAIL: telldir offset %ld != dirent d_off %ld for entry '%s'\n",
                    cmdname, diroff, dentmap[i].dm_dirent.d_off, dentmap[i].dm_name);
            exit(3);
        }
    }
    if (i != direntries) {
        fprintf(stderr, "%s: FAIL: expected %d directory entries but read %d entries\n",
                cmdname, direntries, i);
        exit(3);
    }

    // seek to each returned dirent offset and verify we get the correct entry
}
