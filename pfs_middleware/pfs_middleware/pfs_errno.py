# Copyright (c) 2016 SwiftStack, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
