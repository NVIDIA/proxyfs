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

"""
This module contains functions for creating and parsing JSON-RPC
requests and replies for calling remote procedures in proxyfsd.

These functions do not perform any network IO.
"""

import base64
import uuid


allow_read_only = {
    "Server.RpcFetchExtentMapChunk",
    "Server.RpcGetAccount",
    "Server.RpcGetContainer",
    "Server.RpcGetObject",
    "Server.RpcGetStat",
    "Server.RpcGetXAttr",
    "Server.RpcListXAttr",
    "Server.RpcLookup",
    "Server.RpcMountByAccountName",
    "Server.RpcMountByVolumeName",
    "Server.RpcPing",
    "Server.RpcReadSymlink",
    "Server.RpcReaddirByLoc",
    "Server.RpcReaddirPlusByLoc",
    "Server.RpcReaddirPlus",
    "Server.RpcReaddir",
    "Server.RpcStatVFS",
    "Server.RpcType",
}

allow_read_write = {
    "Server.RpcChmod",
    "Server.RpcChown",
    "Server.RpcCreate",
    "Server.RpcDelete",
    "Server.RpcFetchExtentMapChunk",
    "Server.RpcFlock",
    "Server.RpcFlush",
    "Server.RpcGetAccount",
    "Server.RpcGetContainer",
    "Server.RpcGetObject",
    "Server.RpcGetStat",
    "Server.RpcGetXAttr",
    "Server.RpcLink",
    "Server.RpcListXAttr",
    "Server.RpcLog",
    "Server.RpcLookup",
    "Server.RpcMiddlewareMkdir",
    "Server.RpcMiddlewarePost",
    "Server.RpcMkdir",
    "Server.RpcMountByAccountName",
    "Server.RpcMountByVolumeName",
    "Server.RpcPing",
    "Server.RpcProvisionObject",
    "Server.RpcPutLocation",
    "Server.RpcReadSymlink",
    "Server.RpcReaddirByLoc",
    "Server.RpcReaddirPlusByLoc",
    "Server.RpcReaddirPlus",
    "Server.RpcReaddir",
    "Server.RpcReleaseLease",
    "Server.RpcRemoveXAttr",
    "Server.RpcRename",
    "Server.RpcRenewLease",
    "Server.RpcResize",
    "Server.RpcRmdir",
    "Server.RpcSetTime",
    "Server.RpcSetXAttr",
    "Server.RpcSnapShotCreate",
    "Server.RpcSnapShotDelete",
    "Server.RpcSnapShotListByID",
    "Server.RpcSnapShotListByName",
    "Server.RpcSnapShotListByTime",
    "Server.RpcSnapShotLookupByName",
    "Server.RpcStatVFS",
    "Server.RpcSymlink",
    "Server.RpcType",
    "Server.RpcUnlink",
    "Server.RpcWrote",
}


def _encode_binary(data):
    if data:
        return base64.b64encode(data.encode('ascii')).decode('ascii')
    else:
        return ""


def _decode_binary(bindata):
    if bindata:
        return base64.b64decode(bindata).decode('ascii')
    else:
        # None (happens sometimes) or empty-string (also happens sometimes)
        return ""


def _ctime_or_mtime(res):
    '''
    Get the last-modified time from a response.

    Ideally, this will be the AttrChangeTime (ctime), but older versions of
    proxyfsd only returned the user-settable ModificationTime (mtime).
    '''
    return res.get("AttrChangeTime", res["ModificationTime"])


def jsonrpc_request(method, params, call_id=None):
    """
    Marshal an RPC request in JSON-RPC 2.0 format.

    See http://www.jsonrpc.org/specification for details.

    :param method: remote method to invoke
    :param params: parameters to remote method
    :param call_id: value of JSON-RPC 'id' parameter

    :returns: dictionary suitable for passing to json.dumps() to get a
              serialized JSON-RPC request
    """
    # JSON-RPC requires an 'id' parameter in order to receive a response,
    # and that parameter has to be unique for every call made by that
    # client.
    if call_id is None:
        call_id = str(uuid.uuid4())

    return {'jsonrpc': '2.0',  # magic value
            'method': method,
            'params': params,
            'id': call_id}


def is_account_bimodal_request(account_name):
    """
    Return a JSON-RPC request to ask if an account is bimodal (proxyfs
    middleware is responsible for it) or not (not).

    :param account_name: name of the account. Note that this is the bare
    account name: no /v1, no container, and no object.
    """
    return jsonrpc_request("Server.RpcIsAccountBimodal",
                           [{"AccountName": account_name}])


def parse_is_account_bimodal_response(resp):
    """
    Parse a response from RpcIsAccountBimodal.

    Returns (True, peer-addr) if the account is bimodal, (False, None)
    otherwise.
    """
    is_bimodal = resp["IsBimodal"]
    ip = resp["ActivePeerPrivateIPAddr"]

    if ip.startswith("[") and ip.endswith("]"):
        # IPv6 addresses come with brackets around them, IPv4 addresses do
        # not
        ip = ip[1:-1]

    return (is_bimodal, ip)


def get_object_request(path, http_ranges):
    """
    Return a JSON-RPC request to get a read plan and other info for a
    particular object.

    :param path: URL path component for the object, e.g. "/v1/acc/con/obj"
    :param ranges: HTTP byte-ranges from the HTTP request's Range header
    """
    # This RPC method takes one positional argument, which is a JSON object
    # with two fields: the path and the ranges.
    #
    # The ranges are a bit weird: if both ends are specified, then we have
    # to convert from HTTP-style start/end byte indices to offset/length.
    # It's slightly annoying, but just arithmetic.
    #
    # However, if it's a half-open range (e.g. bytes=M- or bytes=-N), then
    # we send the left endpoint in Offset or the right one in Len; the other
    # is None.
    rpc_ranges = []
    for start, end in http_ranges:
        if start is None or end is None:
            rpc_range = {"Offset": start, "Len": end}
        else:
            rpc_range = {"Offset": start, "Len": end - start + 1}
        rpc_ranges.append(rpc_range)

    return jsonrpc_request("Server.RpcGetObject",
                           [{"VirtPath": path,
                             "ReadEntsIn": rpc_ranges}])


def parse_get_object_response(read_plan_response):
    """
    Parse a response from RpcGetObject.

    Returns (read_plan, metadata, file_size, inode number, number of writes,
    lease ID).

    The read plan is a list of dictionaries with keys "Length", "Offset",
    and "ObjectPath". One gets the object data by reading <Length> bytes
    from <ObjectPath> starting at byte <Offset>.
    """
    return (read_plan_response["ReadEntsOut"],
            _decode_binary(read_plan_response["Metadata"]),
            read_plan_response["FileSize"],
            _ctime_or_mtime(read_plan_response),
            read_plan_response["InodeNumber"],
            read_plan_response["NumWrites"],
            read_plan_response["LeaseId"])


def coalesce_object_request(destination, elements):
    """
    Return a JSON-RPC request to get a read plan and other info for a
    particular object.

    :param destination: full path for the destination object, e.g. /v1/a/c/o

    :param elements: list of account-relative paths for the files to
        coalesce, e.g. ["c1/o1", "c2/o2"]
    """
    return jsonrpc_request("Server.RpcCoalesce",
                           [{"VirtPath": destination,
                             "ElementAccountRelativePaths": elements}])


def parse_coalesce_object_response(coalesce_object_response):
    """
    Parse a response from RpcGetObject.

    Returns (modification time, inode no., no. writes).
    """
    return (_ctime_or_mtime(coalesce_object_response),
            coalesce_object_response["InodeNumber"],
            coalesce_object_response["NumWrites"])


def put_location_request(path):
    """
    Return a JSON-RPC request to get a segment path for an incoming object.

    This is used in handling an object PUT; the middleware asks proxyfsd
    where to store the data (this request), then tells proxyfsd what it's
    done (RpcPutComplete).

    :param path: URL path component for the object, e.g. "/v1/acc/con/obj"
    """
    return jsonrpc_request("Server.RpcPutLocation", [{"VirtPath": path}])


def parse_put_location_response(put_location_response):
    """
    Parse a response from RpcPutLocation.

    :returns: physical path (an object path)
    """
    return put_location_response["PhysPath"]


def put_complete_request(virtual_path, log_segments, obj_metadata):
    """
    Return a JSON-RPC request to notify proxyfsd that an object PUT has
    completed.

    This is used in handling an object PUT; the middleware asks proxyfsd
    where to store the data (RpcPutLocation), then tells proxyfsd what it's
    done (this request).

    :param virtual_path: user-visible path component for the object, e.g.
        "/v1/acc/con/obj". This is what the user specified in their HTTP PUT
        request.

    :param log_segments: the log segments containing the data and their
        sizes. Comes as a list of 2-tuples (segment-name, segment-size).

    :param obj_metadata: serialized object metadata
    """
    return jsonrpc_request("Server.RpcPutComplete", [{
        "VirtPath": virtual_path,
        "PhysPaths": [ls[0] for ls in log_segments],
        "PhysLengths": [ls[1] for ls in log_segments],
        "Metadata": _encode_binary(obj_metadata)}])


def parse_put_complete_response(put_complete_response):
    """
    Parse a response from RpcPutComplete.

    :returns: 3-tuple. The first element is the mtime of the new file, in
    nanoseconds since the epoch; the second is the file's inode number, and
    the third is the number of writes the file has seen since its creation.
    """
    return (_ctime_or_mtime(put_complete_response),
            put_complete_response["InodeNumber"],
            put_complete_response["NumWrites"])


def get_account_request(path, marker, end_marker, limit):
    """
    Return a JSON-RPC request to get a account listing for a given
    account.

    :param path: URL path component for the account, e.g. "/v1/acc"

    :param marker: marker query param, e.g. "animals/fish/carp.png". Comes
                   from the ?marker=X part of the query string sent by the
                   client.

    :param end_marker: end_marker query param, e.g. "animals/zebra.png". Comes
                       from the ?end_marker=X part of the query string sent by
                       the client.

    :param limit: maximum number of entries to return
    """
    # This RPC method takes one positional argument, which is a JSON object
    # with two fields: the path and the ranges.
    return jsonrpc_request("Server.RpcGetAccount",
                           [{"VirtPath": path, "Marker": marker,
                             "EndMarker": end_marker, "MaxEntries": limit}])


def parse_get_account_response(get_account_response):
    """
    Parse a response from RpcGetAccount.

    Returns: (account mtime, account entries).

    The account mtime is in nanoseconds since the epoch.

    The account entries are a list of dictionaries with keys:

        Basename: the container name

        ModificationTime: container mtime, in nanoseconds since the epoch
    """
    mtime = _ctime_or_mtime(get_account_response)
    account_entries = get_account_response["AccountEntries"]
    if account_entries is None:
        # seems to happen when it's an empty list
        return (mtime, [])
    else:
        return (mtime, account_entries)


def get_container_request(path, marker, end_marker, limit, prefix, delimiter):
    """
    Return a JSON-RPC request to get a container listing for a given
    container.

    :param path: URL path component for the container, e.g. "/v1/acc/con"

    :param marker: marker query param, e.g. "animals/fish/carp.png". Comes
                   from the ?marker=X part of the query string sent by the
                   client.

    :param end_marker: end_marker query param, e.g. "animals/zebra.png". Comes
                       from the ?end_marker=X part of the query string sent by
                       the client.

    :param limit: maximum number of entries to return

    :param prefix: prefix of all returned entries' filenames

    :param delimiter: delimiter value, which returns the object names that are
                      nested in the container
    """
    # This RPC method takes one positional argument, which is a JSON object
    # with two fields: the path and the ranges.
    return jsonrpc_request("Server.RpcGetContainer",
                           [{"VirtPath": path, "Marker": marker,
                             "EndMarker": end_marker,
                             "MaxEntries": limit, "Prefix": prefix,
                             "Delimiter": delimiter}])


def parse_get_container_response(get_container_response):
    """
    Parse a response from RpcGetContainer.

    Returns: (container entries, container metadata, container mtime).

    The container entries are a list of dictionaries with keys:

        Basename: the object name

        FileSize: the object's size in bytes

        ModificationTime: object mtime, in nanoseconds since the epoch

        IsDir: True if object is a directory, False otherwise

        InodeNumber: object's inode number

        Metadata: object's serialized metadata, if any

    The container's metadata is just a string. Presumably it's some
    JSON-serialized dictionary that this middleware previously set, but it
    could really be anything.

    The container's mtime is the modification time of the directory, in
    nanoseconds since the epoch.
    """
    res = get_container_response

    ents = []
    for ent in res["ContainerEntries"]:
        ent = ent.copy()
        ent["Metadata"] = _decode_binary(ent["Metadata"])
        ents.append(ent)

    container_meta = _decode_binary(res["Metadata"])
    container_mtime = _ctime_or_mtime(res)
    return ents, container_meta, container_mtime


def middleware_mkdir_request(path, obj_metadata):
    """
    :param path: URL path component for the dir, e.g. "/v1/acc/con/obj".
                 Must refer to an object, not a container (use
                 get_container_request() for containers).

    :param metadata: serialized object metadata
    """
    # There's also an RpcMkdir, but it's not suitable for our use.
    return jsonrpc_request("Server.RpcMiddlewareMkdir", [{
        "VirtPath": path,
        "Metadata": _encode_binary(obj_metadata)}])


def parse_middleware_mkdir_response(mkdir_resp):
    """
    Parse a response from RpcMiddlewareMkdir.

    Returns (mtime in nanoseconds, inode number, number of writes)
    """
    return (_ctime_or_mtime(mkdir_resp),
            mkdir_resp["InodeNumber"],
            mkdir_resp["NumWrites"])


def head_request(path):
    """
    Return a JSON-RPC request to HEAD a container or object

    :param path: URL path component for the container or object,
    e.g. "/v1/acc/con"
    """
    return jsonrpc_request("Server.RpcHead", [{"VirtPath": path}])


def parse_head_response(head_response):
    """
    Parse a response from RpcHead.

    Returns: 6-tuple (serialized metadata, modification time in nanoseconds,
        file size, is-directory, inode number, number of writes)

    The metadata is whatever was sent to proxyfsd. Presumably it's
    serialized somehow, but it's not guaranteed by proxyfsd. This will be
    empty for a container or object created via Samba/FUSE and never
    modified via HTTP.
    """
    return (_decode_binary(head_response["Metadata"]),
            _ctime_or_mtime(head_response),
            head_response["FileSize"], head_response["IsDir"],
            head_response["InodeNumber"], head_response["NumWrites"])


def delete_request(path):
    """
    Return a JSON-RPC request to delete a file or directory.

    :param path: path to the object or container, e.g. "/v1/a/c/o"
    """
    return jsonrpc_request("Server.RpcDelete", [{"VirtPath": path}])


# NB: there is no parse_delete_response since a successful response to
# RpcDelete contains no useful information.


def post_request(path, old_metadata, new_metadata):
    return jsonrpc_request("Server.RpcPost", [{
        "VirtPath": path,
        "OldMetaData": _encode_binary(old_metadata),
        "NewMetaData": _encode_binary(new_metadata)}])


def put_container_request(path, old_metadata, new_metadata):
    return jsonrpc_request("Server.RpcPutContainer", [{
        "VirtPath": path,
        "OldMetadata": _encode_binary(old_metadata),
        "NewMetadata": _encode_binary(new_metadata)}])


def renew_lease_request(lease_id):
    """
    Return a JSON-RPC request to renew a lease. We do this while serving an
    object GET response to make sure that the file's log segments don't get
    deleted while we're still using them.

    :param lease: ID of the lease, probably returned as part of a response
        from Server.RpcGetObject
    """
    return jsonrpc_request("Server.RpcRenewLease", [{
        "LeaseId": lease_id}])


def release_lease_request(lease_id):
    """
    Return a JSON-RPC request to release a lease. We do this when we're done
    serving an object GET response to unblock log-segment deletion for the
    file we're serving.

    :param lease: ID of the lease, probably returned as part of a response
        from Server.RpcGetObject
    """
    return jsonrpc_request("Server.RpcReleaseLease", [{
        "LeaseId": lease_id}])
