# Copyright (c) 2015-2021, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

import json
import requests
import socket

# This version works as well, leaving here as a reference
#def main():
#    args = {'VolumeName' : "CommonVolume", 'MountOptions': 0, 'AuthUser': "balajirao"}
#
#    payload = {
#        "method": "Server.RpcMount",
#        "params": [args],
#        "id": 1,
#    }
#
#    s = socket.create_connection(("localhost", 12345))
#    s.sendall(json.dumps((payload)))
#    rdata = s.recv(1024)
#
#    print "received data:", rdata
#    print "decoded received data:", json.loads(rdata)
#
#    s.close

def main():
    args = {'VolumeName' : "CommonVolume", 'MountOptions': 0, 'AuthUser': "balajirao"}

    id = 0
    payload = {
        "method": "Server.RpcMount",
        "params": [args],
        "jsonrpc": "2.0",
        "id": id,
    }

    s = socket.create_connection(("localhost", 12345))
    data = json.dumps((payload))
    print "sending data:", data
    s.sendall(data)

    # This will actually have to loop if resp is bigger
    rdata = s.recv(1024)

    #print "received data:", rdata
    resp = json.loads(rdata)
    #print "decoded received data:", resp

    if resp["id"] != id:
        raise Exception("expected id=%s, received id=%s: %s" %(id, resp["id"], resp["error"]))

    if resp["error"] is not None:
        raise Exception(resp["error"])


    print "Returned MountID:", resp["result"]["MountID"]

    s.close


if __name__ == "__main__":
    main()
