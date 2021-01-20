# Copyright (c) 2015-2021, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0


"""
Error numbers from ProxyFS. Not guaranteed complete; these are just the
ones that the middleware cares about.


The interface mimics errno's interface, but the error names and values are
different.
"""

errorcode = {
    2: "NotFoundError",
    17: "FileExistsError",
    20: "NotDirError",
    21: "IsDirError",
    22: "InvalidArgError",
    31: "TooManyLinksError",
    39: "NotEmptyError",
}

g = globals()
for errval, errstr in errorcode.items():
    g[errstr] = errval
