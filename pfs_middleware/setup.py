# Copyright (c) 2015-2021, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0


from setuptools import setup


try:
    import multiprocessing.util  # noqa
except ImportError:
    pass


setup(
    name="pfs_middleware",
    version="0.0.1",
    author="SwiftStack Inc.",
    description="WSGI middleware component of ProxyFS",
    license="Apache",
    packages=["pfs_middleware"],

    test_suite="tests",
    tests_require=['mock'],

    # TODO: classifiers and such
    entry_points={
        'paste.filter_factory': [
            'pfs = pfs_middleware:filter_factory',
        ],
    },
)
