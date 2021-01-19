// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package main

// To use: fmt.Sprintf(indexDotHTMLTemplate, globals.ipAddrTCPPort)
const indexDotHTMLTemplate string = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>PFSAgent - %[1]v</title>
  </head>
  <body>
  <a href="/config">config</a>
  <br/>
  <a href="/leases">leases</a>
  <br/>
  <a href="/metrics">metrics</a>
  <br/>
  <a href="/stats">stats</a>
  <br/>
  <a href="/version">version</a>
  </body>
</html>
`
