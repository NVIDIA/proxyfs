# Copyright (c) 2015-2021, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0


from setuptools import setup


setup(
    name="meta_middleware",
    version="0.0.1",
    author="SwiftStack Inc.",
    description="WSGI middleware component of ProxyFS",
    license="Apache",
    packages=["meta_middleware"],

    test_suite="nose.collector",
    tests_require=['nose'],

    # TODO: classifiers and such
    entry_points={
        'paste.filter_factory': [
            'meta = meta_middleware.middleware:filter_factory',
        ],
    },
)
