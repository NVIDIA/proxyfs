#!/usr/bin/env python

#
# Copyright (c) 2019 SwiftStack, Inc.
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
#

#
# Exercise the multipart-upload functionality of S3API using the aws
# command from the awscli package and perform some validation of the
# results.
#
# The aws command needs a file ~/.aws/config that contains the
# authentication information for the account(s) being used.
# For the proxyfs runway environment, the file can look like this:
#
# % cat ~/.aws/config
'''
[plugins]
endpoint = awscli_plugin_endpoint

[profile default]
aws_access_key_id = test:tester
aws_secret_access_key = testing
s3 =
     endpoint_url = http://127.0.0.1:8080
     multipart_threshold = 64MB
     multipart_chunksize = 16MB
s3api =
     endpoint_url = http://127.0.0.1:8080
     multipart_threshold = 5MB
     multipart_chunksize = 5MB

[profile swift]
aws_access_key_id = admin:admin
aws_secret_access_key = admin
s3 =
     endpoint_url = http://127.0.0.1:8080
     multipart_threshold = 64MB
     multipart_chunksize = 16MB
s3api =
     endpoint_url = http://127.0.0.1:8080
     multipart_threshold = 5MB
     multipart_chunksize = 5MB
'''
# The above config file defines two profiles, "default" and "swift".
# this, where the admin account is a separate, non-swift, account
# that can be accessed via curl at AUTH_admin.
#
# This command also assumes three files are in /tmp, available for
# uploading, named /tmp/{part01,part02,part03} which contain 5 Mbyte
# of zeros, 5 Mbyte of zeros, and 1 Mbyte of zeros, respectively.
#
# dd if=/dev/zero of=/tmp/part01 bs=1M count=10
# dd if=/dev/zero of=/tmp/part02 bs=1M count=10
# dd if=/dev/zero of=/tmp/part03 bs=1M count=1
#
from __future__ import print_function

import argparse
import os
import Queue
import sys
import subprocess32
import json

def run_s3api_cmd(sub_cmd, profile_name, bucket_name, obj_name, extra_args):
    '''Create and execute an "aws s3api" sub-command using the passed
    arguments (any of which can be None).

    Return the return code, stdout, and stderr as a tuple.
    '''

    cmd = ['aws', 's3api', sub_cmd]
    if profile_name is not None:
        cmd += ['--profile', profile_name]
    if bucket_name is not None:
        cmd += ['--bucket', bucket_name]
    if obj_name is not None:
        cmd += ['--key', obj_name]
    if extra_args is not None:
        cmd += extra_args

    aws_proc = subprocess32.Popen(cmd, bufsize=-1, stdout=subprocess32.PIPE, stderr=subprocess32.PIPE)
    (aws_stdout, aws_stderr) = aws_proc.communicate()
    rc = aws_proc.wait()
    if rc != 0:
        err = ("run_s3api_cmd(): command '%s' returned %d"
               % (' '.join(cmd), rc))
        print("%s\nstderr: %s\n" % (err, aws_stderr), file=sys.stderr)
        return (err, aws_stdout, aws_stderr)

    print("%s\n%s\n" % (' '.join(cmd), aws_stdout))
    return (None, aws_stdout, aws_stderr)


def multipart_upload(profile, bucket, objname, file_parts):
    '''
    Create a new multipart-upload context (uploadId) for the object
    and then upload the files specified in file_parts.

    Return the JSON decoded string from the "complete-multipart-upload"
    and "head-object" subcommands.
    '''

    if bucket is None or objname is None:
        err ="multipart_upload(): bucket '%s' objname '%s' must be specified" % (bucket, objname)
        print("%s\n" % (err), file=sys.stderr)
        return err

    if len(file_parts) < 2:
        err = "multipart_upload(): must specify at least two files to upload, got %d" % (len(file_parts))
        print("%s\n" % (err), file=sys.stderr)
        return err

    for file in file_parts:
        try:
            fp = open(file, 'r')
            fp.close()
        except:
            err = "multipart_upload(): could not open file '%s' for reading" % (file)
            print("%s\n" % (err), file=sys.stderr)
            return err

    # create the upload bucket (ignore failures if already present)
    (err, aws_stdout, aws_stderr) = run_s3api_cmd(
        'create-bucket', profile, bucket, None, None)
    # if err is not None:
    #     err = "'create-bucket' failed: %s" % (err)
    #     print("%s\n" % (err), file=sys.stderr)
    #     return err

    # create the upload context
    (err, aws_stdout, aws_stderr) = run_s3api_cmd(
        'create-multipart-upload', profile, bucket, objname, None)
    if err is not None:
        err = "'create-multipart-upload' failed: %s" % (err)
        print("%s\n" % (err), file=sys.stderr)
        return err
    create_resp = json.loads(aws_stdout)
    upload_id = create_resp['UploadId']

    # upload the parts
    part_num = 0
    etag_set = []
    for file in file_parts:
        part_num += 1

        extra_args = ['--upload-id', upload_id, '--part-number', str(part_num), '--body', file]
        (err, aws_stdout, aws_stderr) = run_s3api_cmd(
            'upload-part', profile, bucket, objname, extra_args)
        if err is not None:
            err = "'upload-part' failed: %s" % (err)
            print("%s\n" % (err), file=sys.stderr)
            return err

        upload_resp = json.loads(aws_stdout)
        etag = { "ETag": upload_resp['ETag'], "PartNumber": part_num }
        etag_set.append(etag)

    mpu_info = { "Parts": etag_set}
    print("%s\n" % json.dumps(mpu_info))

    # complete the multipart-upload (if there are too many parts we won't
    # be able to pass them on the command line and this will break)
    extra_args = ['--upload-id', upload_id, '--multipart-upload', json.dumps(mpu_info)]
    (err, aws_stdout, aws_stderr) = run_s3api_cmd(
        'complete-multipart-upload', profile, bucket, objname, extra_args)
    if err is not None:
        err = "'complete-multipart-upload' failed: %s" % (err)
        print("%s\n" % (err), file=sys.stderr)
        return err

    complete_resp = json.loads(aws_stdout)
    # we should validate ""Last-Modified"" time in the response
    # headers, except they aren't visible via aws s3api!  but there
    # was a bug where the time was incorrect.

    # get the object attributes
    (err, aws_stdout, aws_stderr) = run_s3api_cmd(
        'head-object', profile, bucket, objname, None)
    if err is not None:
        err = "'head-object' failed: %s" % (err)
        print("%s\n" % (err), file=sys.stderr)
        return err

    head_resp = json.loads(aws_stdout)

    # TODO: should validate ""LastModified"" time in the HEAD response
    # TODO: should validate the ACL (use 'get-object-acl')


    # get the object attributes from a list-object commands;
    # they should match
    (err, aws_stdout, aws_stderr) = run_s3api_cmd(
        'list-objects', profile, bucket, None, None)
    if err is not None:
        err = "'list-objects' failed: %s" % (err)
        print("%s\n" % (err), file=sys.stderr)
        return err

    list_resp = json.loads(aws_stdout)

    s3_etag = complete_resp['ETag']
    if s3_etag != head_resp['ETag']:
        print("ERROR: ETag from complete-multipart-upload '%s' does not match ETag from head-object '%s'\n" %
              (s3_etag, head_resp['ETag']))

    for obj_info in list_resp['Contents']:
        if obj_info['Key'] == 'testobj':
            if s3_etag != obj_info['ETag']:
                print("ERROR: ETag from complete-multipart-upload '%s' does not match ETag from list-objects '%s'\n" %
                      (s3_etag, obj_info['ETag']))
            break

    return (None, complete_resp, head_resp)

(err, proxyfs_complete_resp, proxyfs_head_resp) = multipart_upload(
    "default", "multipart-upload", "testobj", ['/tmp/part01', '/tmp/part02', '/tmp/part03'])

(err, swift_complete_resp, swift_head_resp) = multipart_upload(
    "swift", "multipart-upload", "testobj", ['/tmp/part01', '/tmp/part02', '/tmp/part03'])

if proxyfs_complete_resp['ETag'] != swift_complete_resp['ETag']:
    print("ERROR: ETag from proxyfs complete-multipart-upload '%s' does not match ETag from swift '%s'\n" %
          (proxyfs_complete_resp['ETag'], swift_complete_resp['ETag']))

sys.exit(0)
