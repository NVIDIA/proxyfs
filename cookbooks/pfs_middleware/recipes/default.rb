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

execute "Install PIP" do
  command "curl https://bootstrap.pypa.io/get-pip.py | python"
  not_if { system("pip --version") }
end

execute "Check setuptools is installed" do
  command "pip install setuptools"
end

# install middleware in development mode
execute 'pfs_middleware - setup.py develop' do
  command "python setup.py develop"
  cwd "#{node['source_root']}/src/github.com/swiftstack/ProxyFS/pfs_middleware"
end
