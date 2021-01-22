# Copyright (c) 2015-2021, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0


from .bimodal_checker import BimodalChecker  # flake8: noqa
from .middleware import PfsMiddleware  # flake8: noqa
from .s3_compat import S3Compat  # flake8: noqa
from swift.common.utils import register_swift_info


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    register_swift_info('pfs',
                        bypass_mode=conf.get('bypass_mode', 'off'))

    def pfs_filter(app):
        return BimodalChecker(S3Compat(PfsMiddleware(app, conf), conf), conf)

    return pfs_filter
