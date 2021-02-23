# OpenStack Swift Authorization PlugIn

To provide a workable solution for those following standard Swift Authentication,
this plug-in instance may be employed. Otherwise, consider this implementation a
template for the desired instantiation of whatever Authentication mechanism is
employed.

For the standard Swift Authentication instantiation, the value of `authInJSON`
is required to be a UTF-8 encoded JSON Document:

```
{
    "AuthURL"  : "<e.g. https://<domain-name>/auth/v1.0>",
    "AuthUser" : "<e.g. test:tester>",
    "AuthKey"  : "<e.g. testing>",
    "Account"  : "<e.g. AUTH_test>"
}
```

There are three modifications to the Storage URL normally returned by a
standard Swift Authentication operation:

* The `scheme` used to authenticate may be either `http` or `https`. In the
  case of `https`, it is likely that some form of TLS termination <b>prior</b>
  to reaching the Swift Proxy has rewritten the `scheme` to be `http`. In such
  a case, the Storage URL returned will specify `http` as its scheme. Since
  the client must continue to use `https` to reach the Swift Proxy for each
  authenticated subsequent request, the plug-in will rewrite the scheme to
  be `https`. Note that this is an incomplete solution in cases where standard
  port numbers (i.e. `80` for `http` and `443` for `https`) are not assumed
  (i.e. port numbers are specified in the URL).

* The path portion of the Storage URL returned by the Swift Proxy will start
  with `v1` to indicate the version of the OpenStack Swift protocol being used.
  No `v2` has ever been defined, so this is a constant in normal requests.
  To indicate the client, in the case of GETs and PUTs, has specified the
  `physical` path (i.e. to LogSegments of a FileInode) rather than the
  `virtual` path (i.e. the full path of the file in the file system), this
  protocol version will be changed to `proxyfs`. This updated protocol verion
  will also be used with the `PROXYFS` method used to pass a `mount` request,
  via JSON RPC, over to the ProxyFS process serving the volume to be mounted.

* The final element of the path portion of the Storage URL returned by the
  Swift Proxy will typically be the Account associated with the specified
  AuthUser (e.g. AuthUser `test` typically has a corresponding Account named
  `AUTH_test`). The volume being accessed may, however be stored in a different
  Account than this. As such, the account element of the path will be replaced
  with the `Account` as requested.
