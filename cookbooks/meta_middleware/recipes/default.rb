# Copyright (c) 2015-2021, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

execute "Check setuptools is installed" do
  command "pip install setuptools"
end

# install middleware in development mode
execute 'meta_middleware - setup.py develop' do
  command "python setup.py develop"
  cwd "#{node['source_root']}/src/github.com/NVIDIA/proxyfs/meta_middleware"
end
