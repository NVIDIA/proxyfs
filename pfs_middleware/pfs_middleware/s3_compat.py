# Copyright (c) 2017 SwiftStack, Inc.
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
Middleware to provide S3 compatibility functions.

In conjunction with the Swift3 middleware, this allows clients using the S3
API to access data in bimodal ProxyFS accounts.
"""

from . import utils

from swift.common import swob
from swift.common.utils import get_logger


class S3Compat(object):
    def __init__(self, app, conf, logger=None):
        self.logger = logger or get_logger(conf, log_route='pfs_s3_compat')
        self.app = app

    @swob.wsgify
    def __call__(self, req):
        if self._is_s3_slo_manifest_put(req):
            return self.app
        else:
            return self.app

    def _is_s3_slo_manifest_put(self, req):
        ver, acc, con, obj = utils.split_path(req.path)
