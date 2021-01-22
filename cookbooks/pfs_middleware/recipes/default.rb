# Copyright (c) 2015-2021, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

execute "Check setuptools & tox is installed" do
  command "pip install setuptools tox==3.5.3"
end

# install middleware in development mode
execute 'pfs_middleware - setup.py develop' do
  command "python setup.py develop"
  cwd "#{node['source_root']}/src/github.com/swiftstack/ProxyFS/pfs_middleware"
end
