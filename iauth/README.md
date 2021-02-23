# Authentication Plug-In

Both ProxyFS itself and each PFSAgent client access Swift Accounts,
Containers, and Objects directly. PFSAgent, potentially residing
outside a Swift cluster's `trust domain`, will use normal
OpenStack Swift methods for such access. This access must be authorized
by means of obtaining an `AuthToken`. ProxyFS, if not configured
alongside a so-called `NoAuth` Swift Proxy, will also need the same.
Even in Swift clusters have a `NoAuth` Swift Proxy configured, ProxyFS
will, from time to time, validate PFSAgent client access by testing their
AuthToken locally as well.

While many Swift clusters implement authorization via Swift Proxy
pipeline filters that honor the OpenStack Swift convention, some clusters
may require alternate authorization mechanisms. To support any such
authorization solution, a Golang Plug-In mechanism is employed.

A standard OpenStack Swift Plug-In is provided (see subdirectory `iauth-swift`)
that may either be used for clusters honoring the OpenStack Swift authorization
convention or as a template for development of any particular authorization
solution. The only requirements are that:

* The plug-in's location is provided this `iauth` package
* Credentials to be authorized are provided in a single string (possibly a JSON document)
* The plug-in returns both a Swift AuthToken and StorageURL (or an error)
* The StorageURL has been properly modified as necessary to ensure
    * the proper transport is used (i.e. either "http" or "https")
    * the version portion of the path has been set to "proxyfs" (rather than "v1")
