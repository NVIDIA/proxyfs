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

from .bimodal_checker import BimodalChecker  # flake8: noqa
from .middleware import PfsMiddleware  # flake8: noqa
from .s3_compat import S3Compat  # flake8: noqa


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def pfs_filter(app):
        return BimodalChecker(S3Compat(PfsMiddleware(app, conf), conf), conf)

    return pfs_filter
