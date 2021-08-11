// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

// To use: fmt.Sprintf(indexDotHTMLTemplate, proxyfsVersion)
const indexDotHTMLTemplate string = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="/bootstrap.min.css">
    <link rel="stylesheet" href="/styles.css">
    <title>imgr</title>
  </head>
  <body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark fixed-top">
      <button class="navbar-toggler" type="button" data-toggle="collapse" data-target="#navbarNavDropdown" aria-controls="navbarNavDropdown" aria-expanded="false" aria-label="Toggle navigation">
        <span class="navbar-toggler-icon"></span>
      </button>
      <div class="collapse navbar-collapse" id="navbarNavDropdown">
        <ul class="navbar-nav mr-auto">
          <li class="nav-item active">
            <a class="nav-link" href="/">Home <span class="sr-only">(current)</span></a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/config">Config</a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/stats">Stats</a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/volume">Volumes</a>
          </li>
        </ul>
        <span class="navbar-text">Version %[1]v</span>
      </div>
    </nav>
    <div class="container">
      <nav aria-label="breadcrumb">
        <ol class="breadcrumb">
          <li class="breadcrumb-item active" aria-current="page">Home</li>
        </ol>
      </nav>
      <h1 class="display-4">
        ProxyFS imgr
      </h1>
      <div class="card-deck">
        <div class="card mb-4">
          <div class="card-body">
            <h5 class="card-title">Configuration parameters</h5>
            <p class="card-text">Diplays a JSON representation of the active configuration.</p>
          </div>
          <ul class="list-group list-group-flush">
            <li class="list-group-item">
              <a href="/config" class="card-link">Configuration Parameters</a>
          </ul>
        </div>
        <div class="w-100 d-none d-sm-block d-md-none"><!-- wrap every 1 on sm--></div>
        <div class="card mb-4">
          <div class="card-body">
            <h5 class="card-title">Stats</h5>
            <p class="card-text">Displays current statistics.</p>
          </div>
          <ul class="list-group list-group-flush">
            <li class="list-group-item">
              <a href="/stats" class="card-link">Stats Page</a>
          </ul>
        </div>
        <div class="w-100 d-none d-sm-block d-md-none"><!-- wrap every 1 on sm--></div>
        <div class="w-100 d-none d-md-block d-lg-none"><!-- wrap every 2 on md--></div>
        <div class="w-100 d-none d-lg-block d-xl-none"><!-- wrap every 2 on lg--></div>
        <div class="w-100 d-none d-xl-block"><!-- wrap every 3 on xl--></div>
        <div class="w-100 d-none d-sm-block d-md-none"><!-- wrap every 1 on sm--></div>
        <div class="card mb-4">
          <div class="card-body">
            <h5 class="card-title">Volumes</h5>
            <p class="card-text">Examine volumes currently active on this imgr node.</p>
          </div>
          <ul class="list-group list-group-flush">
            <li class="list-group-item">
              <a href="/volume" class="card-link">Volume Page</a>
          </ul>
        </div>
      </div>
    </div>
    <script src="/jquery.min.js"></script>
    <script src="/popper.min.js"></script>
    <script src="/bootstrap.min.js"></script>
  </body>
</html>
`

// To use: fmt.Sprintf(configTemplate, proxyfsVersion, confMapJSONString)
const configTemplate string = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="/bootstrap.min.css">
    <link rel="stylesheet" href="/styles.css">
    <title>Config</title>
  </head>
  <body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark fixed-top">
      <button class="navbar-toggler" type="button" data-toggle="collapse" data-target="#navbarNavDropdown" aria-controls="navbarNavDropdown" aria-expanded="false" aria-label="Toggle navigation">
        <span class="navbar-toggler-icon"></span>
      </button>
      <div class="collapse navbar-collapse" id="navbarNavDropdown">
        <ul class="navbar-nav mr-auto">
          <li class="nav-item">
            <a class="nav-link" href="/">Home</a>
          </li>
          <li class="nav-item active">
            <a class="nav-link" href="/config">Config <span class="sr-only">(current)</span></a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/stats">Stats</a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/volume">Volumes</a>
          </li>
        </ul>
        <span class="navbar-text">Version %[1]v</span>
      </div>
    </nav>
    <div class="container">
      <nav aria-label="breadcrumb">
        <ol class="breadcrumb">
          <li class="breadcrumb-item"><a href="/">Home</a></li>
          <li class="breadcrumb-item active" aria-current="page">Config</li>
        </ol>
      </nav>
      <h1 class="display-4">
        Config
      </h1>
      <pre class="code" id="json_data"></pre>
    </div>
    <script src="/jquery.min.js"></script>
    <script src="/popper.min.js"></script>
    <script src="/bootstrap.min.js"></script>
    <script src="/jsontree.js"></script>
    <script type="text/javascript">
      var json_data = %[2]v;
      document.getElementById("json_data").innerHTML = JSONTree.create(json_data, null, 1);
      JSONTree.collapse();
    </script>
  </body>
</html>
`

// To use: fmt.Sprintf(volumeListTopTemplate, proxyfsVersion)
const volumeListTopTemplate string = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="/bootstrap.min.css">
    <link rel="stylesheet" href="/styles.css">
    <title>Volumes</title>
  </head>
  <body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark fixed-top">
      <button class="navbar-toggler" type="button" data-toggle="collapse" data-target="#navbarNavDropdown" aria-controls="navbarNavDropdown" aria-expanded="false" aria-label="Toggle navigation">
        <span class="navbar-toggler-icon"></span>
      </button>
      <div class="collapse navbar-collapse" id="navbarNavDropdown">
        <ul class="navbar-nav mr-auto">
          <li class="nav-item">
            <a class="nav-link" href="/">Home</a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/config">Config</a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/stats">Stats</a>
          </li>
          <li class="nav-item active">
            <a class="nav-link" href="/volume">Volumes <span class="sr-only">(current)</span></a>
          </li>
        </ul>
        <span class="navbar-text">Version %[1]v</span>
      </div>
    </nav>
    <div class="container">
      <nav aria-label="breadcrumb">
        <ol class="breadcrumb">
          <li class="breadcrumb-item"><a href="/">Home</a></li>
          <li class="breadcrumb-item active" aria-current="page">Volumes</li>
        </ol>
      </nav>

      <h1 class="display-4">Volumes</h1>
      <table class="table table-sm table-striped table-hover">
        <thead>
          <tr>
            <th scope="col">Volume Name</th>
            <th class="fit">&nbsp;</th>
            <th class="fit">&nbsp;</th>
            <th class="fit">&nbsp;</th>
            <th class="fit">&nbsp;</th>
            <th class="fit">&nbsp;</th>
          </tr>
        </thead>
        <tbody>
`

// To use: fmt.Sprintf(volumeListPerVolumeTemplate, volumeName)
const volumeListPerVolumeTemplate string = `          <tr>
            <td>%[1]v</td>
            <td class="fit"><a href="/volume/%[1]v/snapshot" class="btn btn-sm btn-primary">SnapShots</a></td>
            <td class="fit"><a href="/volume/%[1]v/fsck-job" class="btn btn-sm btn-primary">FSCK jobs</a></td>
            <td class="fit"><a href="/volume/%[1]v/scrub-job" class="btn btn-sm btn-primary">SCRUB jobs</a></td>
            <td class="fit"><a href="/volume/%[1]v/layout-report" class="btn btn-sm btn-primary">Layout Report</a></td>
            <td class="fit"><a href="/volume/%[1]v/extent-map" class="btn btn-sm btn-primary">Extent Map</a></td>
          </tr>
`

const volumeListBottom string = `        </tbody>
      </table>
    </div>
    <script src="/jquery.min.js"></script>
    <script src="/popper.min.js"></script>
    <script src="/bootstrap.min.js"></script>
  </body>
</html>
`

// To use: fmt.Sprintf(layoutReportTopTemplate, proxyfsVersion, volumeName)
const layoutReportTopTemplate string = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="/bootstrap.min.css">
    <link rel="stylesheet" href="/styles.css">
    <title>Layout Report %[2]v</title>
  </head>
  <body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark fixed-top">
      <button class="navbar-toggler" type="button" data-toggle="collapse" data-target="#navbarNavDropdown" aria-controls="navbarNavDropdown" aria-expanded="false" aria-label="Toggle navigation">
        <span class="navbar-toggler-icon"></span>
      </button>
      <div class="collapse navbar-collapse" id="navbarNavDropdown">
        <ul class="navbar-nav mr-auto">
          <li class="nav-item">
            <a class="nav-link" href="/">Home</a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/config">Config</a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/stats">Stats</a>
          </li>
          <li class="nav-item active">
            <a class="nav-link" href="/volume">Volumes <span class="sr-only">(current)</span></a>
          </li>
        </ul>
        <span class="navbar-text">Version %[1]v</span>
      </div>
    </nav>
    <div class="container">
      <nav aria-label="breadcrumb">
        <ol class="breadcrumb">
          <li class="breadcrumb-item"><a href="/">Home</a></li>
          <li class="breadcrumb-item"><a href="/volume">Volumes</a></li>
          <li class="breadcrumb-item active" aria-current="page">Layout Report %[2]v</li>
        </ol>
      </nav>
      <h1 class="display-4">
        Layout Report
        <small class="text-muted">%[2]v</small>
      </h1>
      <a id="validate-button" class="btn btn-primary float-right" href="#">Show validated version</a><br><br>
`

// To use: fmt.Sprintf(layoutReportTableTopTemplate, TreeName, NumDiscrepencies, {"success"|"danger"})
const layoutReportTableTopTemplate string = `      <br>
      <div class="d-flex justify-content-between">
        <h3>%[1]v</h3><h4><span class="badge badge-%[3]v d-none">%[2]v discrepancies</span></h4>
      </div>
	    <table class="table table-sm table-striped table-hover">
        <thead>
          <tr>
            <th scope="col" class="w-50">ObjectName</th>
            <th scope="col" class="w-50">ObjectBytes</th>
          </tr>
        </thead>
        <tbody>
`

// To use: fmt.Sprintf(layoutReportTableRowTemplate, ObjectName, ObjectBytes)
const layoutReportTableRowTemplate string = `          <tr>
            <td><pre class="no-margin">%016[1]X</pre></td>
			      <td><pre class="no-margin">%[2]v</pre></td>
          </tr>
`

const layoutReportTableBottom string = `        </tbody>
      </table>
`

const layoutReportBottom string = `    <div>
    <script src="/jquery.min.js"></script>
    <script src="/popper.min.js"></script>
    <script src="/bootstrap.min.js"></script>
    <script type="text/javascript">
      var url_params = new URLSearchParams(window.location.search);
      var validate = false;
      var validate_button = document.getElementById('validate-button');
      if (
        url_params.has("validate") &&
        url_params.get("validate") != "0" &&
        url_params.get("validate").toLowerCase() != "false"
      ) {
        validate = true;
      }
      if (validate) {
        validate_button.innerHTML = 'Show non-validated version';
        validate_button.href = window.location.origin + window.location.pathname + "?validate=0";
        $("h4 .badge").removeClass("d-none");
      } else {
        validate_button.innerHTML = 'Show validated version';
        validate_button.href = window.location.origin + window.location.pathname + "?validate=1";
      }
    </script>
  </body>
</html>
`

// To use: fmt.Sprintf(extentMapTemplate, proxyfsVersion, volumeName, extentMapJSONString, pathDoubleQuotedString, serverErrorBoolString)
const extentMapTemplate string = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="/bootstrap.min.css">
    <link rel="stylesheet" href="/styles.css">
    <title>Extent Map %[2]v</title>
  </head>
  <body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark fixed-top">
      <button class="navbar-toggler" type="button" data-toggle="collapse" data-target="#navbarNavDropdown" aria-controls="navbarNavDropdown" aria-expanded="false" aria-label="Toggle navigation">
        <span class="navbar-toggler-icon"></span>
      </button>
      <div class="collapse navbar-collapse" id="navbarNavDropdown">
        <ul class="navbar-nav mr-auto">
          <li class="nav-item">
            <a class="nav-link" href="/">Home</a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/config">Config</a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/stats">Stats</a>
          </li>
          <li class="nav-item active">
            <a class="nav-link" href="/volume">Volumes <span class="sr-only">(current)</span></a>
          </li>
        </ul>
        <span class="navbar-text">Version %[1]v</span>
      </div>
    </nav>
    <div class="container">
      <nav aria-label="breadcrumb">
        <ol class="breadcrumb">
          <li class="breadcrumb-item"><a href="/">Home</a></li>
          <li class="breadcrumb-item"><a href="/volume">Volumes</a></li>
          <li class="breadcrumb-item active" aria-current="page">Extent Map %[2]v</li>
        </ol>
      </nav>

      <h1 class="display-4">
        Extent Map
        <small class="text-muted">%[2]v</small>
      </h1>

      <div class="alert alert-danger" id="error-message" role="alert"></div>

      <form id="new-path-form">
        <div class="input-group mb-3">
          <input type="text" id="path-text-box" class="form-control path-text-box" placeholder="path/to/check" aria-label="Path to check">
          <div class="input-group-append">
            <input type="submit" class="btn btn-primary" value="Search">
          </div>
        </div>
      </form>

      <br>
      <table class="table table-sm table-striped table-hover" id="extent-map-table">
        <thead>
          <tr>
            <th scope="col">File Offset</th>
            <th scope="col" class="w-50">Container/Object</th>
            <th scope="col">Object Offset</th>
            <th scope="col">Length</th>
          </tr>
        </thead>
        <tbody id="extent-map-data"></tbody>
      </table>
    </div>
    <script src="/jquery.min.js"></script>
    <script src="popper.min.js"></script>
    <script src="/bootstrap.min.js"></script>
    <script type="text/javascript">
      var json_data = %[3]v
      var path = %[4]v;
      var volume = "%[2]v";
      var server_error = %[5]v;

      $("#new-path-form").submit(function(e){
        e.preventDefault();
        var new_path = $("#path-text-box").val().trim();
        if (new_path != "" && !new_path.startsWith("/")) {
          new_path = "/" + new_path;
        }
        var new_url = "/volume/" + volume + "/extent-map" + new_path;
        window.location = new_url;
      });

      var hideError = function() {
        document.getElementById("error-message").style.display = "none";
      };

      var showPathError = function(path) {
        var msg_to_print = "";
        if (path !== null) {
          msg_to_print = "<p>There was an error getting extent map for path:</p><pre>" + path + "</pre>";
        } else {
          msg_to_print = "<p>There was an error getting extent map for path:</p><pre>(error retrieving input path)</pre>";
        }
        showCustomError(msg_to_print);
      };

      var showCustomError = function(text) {
        document.getElementById("error-message").innerHTML = text;
      };

      var getTableMarkupWithData = function(data) {
        var table_markup = "";
        for (var key in data) {
          table_markup += "          <tr>\n";
          table_markup += "            <td><pre class=\"no-margin\">" + data[key]["file_offset"] + "</pre></td>\n";
          table_markup += "            <td><pre class=\"no-margin\">" + data[key]["container_name"] + "/" + data[key]["object_name"] + "</pre></td>\n";
          table_markup += "            <td><pre class=\"no-margin\">" + data[key]["object_offset"] + "</pre></td>\n";
          table_markup += "            <td><pre class=\"no-margin\">" + data[key]["length"] + "</pre></td>\n";
          table_markup += "          </tr>\n";
        }
        return table_markup;
      };

      var buildTable = function(data) {
        document.getElementById("extent-map-data").innerHTML = getTableMarkupWithData(data);
      };

      var hideTable = function() {
        document.getElementById("extent-map-table").style.display = "none";
      };

      var fillInTextBox = function(path) {
        document.getElementById("path-text-box").value = path;
      };

      var selectSearchText = function() {
        var pat_text_box = document.getElementById("path-text-box");
        pat_text_box.setSelectionRange(0, pat_text_box.value.length)
      };

      if (server_error) {
        // Error finding path
        hideTable();
        fillInTextBox(path);
        showPathError(path);
        selectSearchText();
      } else if (json_data === null && path === null) {
        // Empty form
        hideTable();
        hideError();
        selectSearchText();
      } else if (json_data === null || path === null) {
        // This should never happen!
        hideTable();
        var msg_to_print = "<p>Oops, that's embarrassing... Something went wrong server side: ";
        if (json_data === null) {
          msg_to_print += "'json_data' is null, but 'path' is not:</p><pre>" + path + "</pre>";
          fillInTextBox(path);
        } else {
          msg_to_print += "'path' is null, but 'json_data' is not:</p><pre>" + JSON.stringify(json_data, null, 2) + "</pre>";
        }
        showCustomError(msg_to_print);
        selectSearchText();
      } else {
        // Path has been found
        hideError();
        fillInTextBox(path);
        buildTable(json_data);
        selectSearchText();
      }
    </script>
  </body>
</html>
`
