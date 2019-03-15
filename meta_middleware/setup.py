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
