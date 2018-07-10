package httpserver

// To use: fmt.Sprintf(indexDotHTMLTemplate, gitDescribeOutput, globals.ipAddrTCPPort)
// TODO: Incorporate %[1]v (gitDescribeOutput) into navigation code below
const indexDotHTMLTemplate string = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="/bootstrap.min.css">
    <link rel="stylesheet" href="/styles.css">
    <title>ProxyFS Management - %[2]v</title>
  </head>
  <body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark fixed-top">
      <a class="navbar-brand" href="#">%[2]v</a>
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
            <a class="nav-link" href="/metrics">StatsD/Prometheus</a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/trigger">Triggers</a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/volume">Volumes</a>
          </li>
        </ul>
      </div>
    </nav>
    <div class="container">
      <nav aria-label="breadcrumb">
        <ol class="breadcrumb">
          <li class="breadcrumb-item active" aria-current="page">Home</li>
        </ol>
      </nav>
      <h1 class="display-4">
        ProxyFS Management
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
            <h5 class="card-title">StatsD/Prometheus</h5>
            <p class="card-text">Displays current statistics.</p>
          </div>
          <ul class="list-group list-group-flush">
            <li class="list-group-item">
              <a href="/metrics" class="card-link">StatsD/Prometheus Page</a>
          </ul>
        </div>
        <div class="w-100 d-none d-sm-block d-md-none"><!-- wrap every 1 on sm--></div>
        <div class="w-100 d-none d-md-block d-lg-none"><!-- wrap every 2 on md--></div>
        <div class="w-100 d-none d-lg-block d-xl-none"><!-- wrap every 2 on lg--></div>
        <div class="w-100 d-none d-xl-block"><!-- wrap every 3 on xl--></div>
        <div class="card mb-4">
          <div class="card-body">
            <h5 class="card-title">Triggers</h5>
            <p class="card-text">Manage triggers for simulating failures.</p>
          </div>
          <ul class="list-group list-group-flush">
            <li class="list-group-item">
              <a class="card-link" href="/trigger">Triggers Page</a>
            </li>
          </ul>
        </div>
        <div class="w-100 d-none d-sm-block d-md-none"><!-- wrap every 1 on sm--></div>
        <div class="card mb-4">
          <div class="card-body">
            <h5 class="card-title">Volumes</h5>
            <p class="card-text">Examine volumes currently active on this ProxyFS node.</p>
          </div>
          <ul class="list-group list-group-flush">
            <li class="list-group-item">
              <a href="/volume" class="card-link">Volume Page</a>
          </ul>
        </div>
      </div>
    </div>
    <script src="/jquery-3.2.1.min.js"></script>
    <script src="/popper.min.js"></script>
    <script src="/bootstrap.min.js"></script>
  </body>
</html>
`

// To use: fmt.Sprintf(configTemplate, gitDescribeOutput, globals.ipAddrTCPPort, confMapJSONString)
// TODO: Incorporate %[1]v (gitDescribeOutput) into navigation code below
const configTemplate string = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="/bootstrap.min.css">
    <link rel="stylesheet" href="/styles.css">
    <title>Config - %[2]v</title>
  </head>
  <body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark fixed-top">
      <a class="navbar-brand" href="#">%[2]v</a>
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
            <a class="nav-link" href="/metrics">StatsD/Prometheus</a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/trigger">Triggers</a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/volume">Volumes</a>
          </li>
        </ul>
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
    <script src="/jquery-3.2.1.min.js"></script>
    <script src="/popper.min.js"></script>
    <script src="/bootstrap.min.js"></script>
    <script src="/jsontree.js"></script>
    <script type="text/javascript">
      var json_data = %[3]v;
      document.getElementById("json_data").innerHTML = JSONTree.create(json_data, null, 1);
      JSONTree.collapse();
    </script>
  </body>
</html>
`

// To use: fmt.Sprintf(metricsTemplate, gitDescribeOutput, globals.ipAddrTCPPort, metricsJSONString)
// TODO: Incorporate %[1]v (gitDescribeOutput) into navigation code below
const metricsTemplate string = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="/bootstrap.min.css">
    <link rel="stylesheet" href="/styles.css">
    <title>Metrics - %[2]v</title>
  </head>
  <body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark fixed-top">
      <a class="navbar-brand" href="#">%[2]v</a>
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
          <li class="nav-item active">
            <a class="nav-link" href="/metrics">StatsD/Prometheus <span class="sr-only">(current)</span></a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/trigger">Triggers</a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/volume">Volumes</a>
          </li>
        </ul>
      </div>
    </nav>
    <div class="container">
      <nav aria-label="breadcrumb">
        <ol class="breadcrumb">
          <li class="breadcrumb-item"><a href="/">Home</a></li>
          <li class="breadcrumb-item active" aria-current="page">StatsD/Prometheus</li>
        </ol>
      </nav>
      <h1 class="display-4">StatsD/Prometheus</h1>
      <div class="text-center">
        <div class="btn-group btn-group-toggle" data-toggle="buttons" id="tab-bar"></div>
      </div>
      <br>
      <table class="table table-sm table-striped table-hover">
        <tbody id="metrics-data"></tbody>
      </table>
    </div>
    <script src="/jquery-3.2.1.min.js"></script>
    <script src="/popper.min.js"></script>
    <script src="/bootstrap.min.js"></script>
    <script type="text/javascript">
      var json_data = %[3]v;
      var getPrefixes = function(data, levels) {
        var prefixes = new Set();
        for (var key in data) {
          prefixes.add(key.split("_", levels).join("_"));
        }
        return Array.from(prefixes);
      };
      var getTabBarButtonMarkup = function(prefix, text, active) {
        var button_markup = "";
        button_markup += "          <label class=\"btn btn-sm btn-primary" + (active ? " active" : "") + "\" onclick=\"newPrefixSelected('" + prefix + "');\">\n";
        button_markup += "            <input type=\"radio\" name=\"options\" id=\"option-" + prefix + "\" autocomplete=\"off\" checked> " + text + "\n";
        button_markup += "          </label>\n";
        return button_markup;
      };
      var buildTabBarWithPrefixes = function(tab_bar_id, prefixes, selected_prefix) {
        var tab_bar_markup = "";
        tab_bar_markup += getTabBarButtonMarkup("", "All", selected_prefix == "");
        var prefixes_length = prefixes.length;
        for (var i = 0; i < prefixes_length; i++) {
          var prefix = prefixes[i];
          tab_bar_markup += getTabBarButtonMarkup(prefix, prefix, selected_prefix == prefix);
        }
        document.getElementById(tab_bar_id).innerHTML = tab_bar_markup;
      };
      var filterDataByPrefix = function(data, prefix) {
        var prefix_to_search = prefix;
        if (prefix_to_search !== "") {
          prefix_to_search += "_";
        }
        var filtered = {};
        for (var key in data) {
          if (key.startsWith(prefix_to_search)) {
            filtered[key] = data[key];
          }
        }
        return filtered;
      };
      var getTableMarkupWithData = function(data) {
        var table_markup = "";
        for (var key in data) {
          table_markup += "          <tr>\n";
          table_markup += "            <th scope=\"row\">" + key + "</th>\n";
          table_markup += "            <td class=\"text-right\"><pre class=\"no-margin\">" + data[key] + "</pre></td>\n";
          table_markup += "          </tr>\n";
        }
        return table_markup;
      };
      var newPrefixSelected = function(prefix) {
        var filteredData = filterDataByPrefix(json_data, prefix);
        document.getElementById("metrics-data").innerHTML = getTableMarkupWithData(filteredData);
        window.location.hash = prefix;
      };
      var prefixes = getPrefixes(json_data, 2);
      var anchor = window.location.hash;
      if (anchor !== "") {
        anchor = anchor.substr(1);
        if (prefixes.indexOf(anchor) == -1) {
          anchor = "";
        }
      }
      buildTabBarWithPrefixes("tab-bar", prefixes, anchor);
      newPrefixSelected(anchor);
    </script>
  </body>
</html>
`

// To use: fmt.Sprintf(volumeListTopTemplate, gitDescribeOutput, globals.ipAddrTCPPort)
// TODO: Incorporate %[1]v (gitDescribeOutput) into navigation code below
const volumeListTopTemplate string = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="/bootstrap.min.css">
    <link rel="stylesheet" href="/styles.css">
    <title>Volumes - %[2]v</title>
  </head>
  <body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark fixed-top">
      <a class="navbar-brand" href="#">%[2]v</a>
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
            <a class="nav-link" href="/metrics">StatsD/Prometheus</a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/trigger">Triggers</a>
          </li>
          <li class="nav-item active">
            <a class="nav-link" href="/volume">Volumes <span class="sr-only">(current)</span></a>
          </li>
        </ul>
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
    <script src="/jquery-3.2.1.min.js"></script>
    <script src="/popper.min.js"></script>
    <script src="/bootstrap.min.js"></script>
  </body>
</html>
`

// To use: fmt.Sprintf(snapShotsTopTemplate, gitDescribeOutput, globals.ipAddrTCPPort, volumeName)
// TODO: Incorporate %[1]v (gitDescribeOutput) into navigation code below
const snapShotsTopTemplate string = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="/bootstrap.min.css">
    <link rel="stylesheet" href="/styles.css">
    <link href="/open-iconic/font/css/open-iconic-bootstrap.min.css" rel="stylesheet">
    <title>%[2]v SnapShots - %[2]v</title>
  </head>
  <body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark fixed-top">
      <a class="navbar-brand" href="#">%[2]v</a>
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
            <a class="nav-link" href="/metrics">StatsD/Prometheus</a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/trigger">Triggers</a>
          </li>
          <li class="nav-item active">
            <a class="nav-link" href="/volume">Volumes <span class="sr-only">(current)</span></a>
          </li>
        </ul>
      </div>
    </nav>
    <div class="container">
      <nav aria-label="breadcrumb">
        <ol class="breadcrumb">
          <li class="breadcrumb-item"><a href="/">Home</a></li>
          <li class="breadcrumb-item"><a href="/volume">Volumes</a></li>
          <li class="breadcrumb-item active" aria-current="page">SnapShots %[3]v</li>
        </ol>
      </nav>
      <div id="alert-area"></div>
      <h1 class="display-4">
        SnapShots
        <small class="text-muted">%[3]v</small>
      </h1>
      <form class="float-right" onsubmit="return createSnapShot();">
        <div class="input-group mb-3">
          <input type="text" name="name" class="form-control form-control-sm mb-2" id="new-snapshot-name" placeholder="New snapshot name" aria-label="Name" autofocus="autofocus">
          <div class="input-group-append">
            <button type="submit" class="btn btn-sm btn-primary mb-2">Create snapshot</span></button>
          </div>
        </div>
      </form>
      <table class="table table-sm table-striped table-hover">
        <thead>
          <tr>
            <th scope="col" id="header-id" class="fit clickable">ID</th>
            <th scope="col" id="header-timestamp" class="w-25 clickable">Time</th>
            <th scope="col" id="header-name" class="clickable">Name</th>
            <th class="fit">&nbsp;</th>
          </tr>
        </thead>
        <tbody>
`

// To use: fmt.Sprintf(snapShotsPerSnapShotTemplate, id, timeStamp.Format(time.RFC3339), name)
const snapShotsPerSnapShotTemplate string = `          <tr>
            <td>%[1]v</td>
            <td>%[2]v</td>
            <td>%[3]v</td>
            <td class="fit"><a href="#" class="btn btn-sm btn-danger" onclick="deleteSnapShot(%[1]v);"><span class="oi oi-trash" title="Delete" aria-hidden="true"></a></td>
          </tr>
`

// To use: fmt.Sprintf(snapShotsBottomTemplate, volumeName)
const snapShotsBottomTemplate string = `        </tbody>
      </table>
      <br />
    </div>
    <script src="/jquery-3.2.1.min.js"></script>
    <script src="/popper.min.js"></script>
    <script src="/bootstrap.min.js"></script>
    <script type="text/javascript">
      volumeName = "%[1]v";
      hideAlert = function() {
        document.getElementById('alert-area').innerHTML = '';
      };
      showAlertWithMsg = function(msg) {
        var html = '<div class="alert alert-danger alert-dismissible fade show" role="alert">\n';
        html += '  ' + msg + '\n';
        html += '  <button type="button" class="close" data-dismiss="alert" aria-label="Close">';
        html += '    <span aria-hidden="true">&times;</span>\n';
        html += '  </button>\n';
        html += '</div>\n';
        document.getElementById('alert-area').innerHTML = html;
      };
      showDeleteError = function(id, jqXHR, textStatus, errorThrown) {
        var msg = 'Error deleting snapshot with ID <em>' + id + '</em>: ' + jqXHR.status + ' ' + jqXHR.statusText;
        showAlertWithMsg(msg);
      };
      showCreateError = function(name, jqXHR, textStatus, errorThrown) {
        var msg = 'Error creating snapshot with name <em>' + name + '</em>: ' + jqXHR.status + ' ' + jqXHR.statusText;
        showAlertWithMsg(msg);
      };
      deleteSnapShot = function(id) {
        hideAlert();
        var url = '/volume/' + volumeName + '/snapshot/' + id;
        $.ajax({
          url: url,
          method: 'DELETE',
          dataType: 'json',
          success: function(data, textStatus, jqXHR) {
            location.reload();
          },
          error: function(jqXHR, textStatus, errorThrown) {
            showDeleteError(id, jqXHR, textStatus, errorThrown);
          }
        });
      };
      createSnapShot = function() {
        hideAlert();
        document.getElementById('new-snapshot-name').select();
        var new_snapshot_name = $.trim($('#new-snapshot-name').val());
        if (new_snapshot_name == "") {
          showAlertWithMsg("SnapShot name can't be blank.");
          return false;
        }
        var url = '/volume/' + volumeName + '/snapshot/';
        $.ajax({
          url: url,
          method: 'POST',
          data: {'name': new_snapshot_name},
          success: function(data, textStatus, jqXHR) {
            location.reload();
          },
          error: function(jqXHR, textStatus, errorThrown) {
            showCreateError(new_snapshot_name, jqXHR, textStatus, errorThrown);
          }
        });
        return false;
      };
      getQueryVariable = function(variable) {
        var query = window.location.search.substring(1);
        var vars = query.split('&');
        for (var i = 0; i < vars.length; i++) {
          var pair = vars[i].split('=');
          if (decodeURIComponent(pair[0]) == variable) {
            return decodeURIComponent(pair[1]);
          }
        }
        return null;
      }
      getQueryVariableOrDefault = function(variable, default_value, to_lower, allowed_values) {
        var value = getQueryVariable(variable);
        if (value === null) {
          value = default_value;
        } else if (to_lower) {
          value = value.toLowerCase();
        }
        // allowed_values is optional
        if (typeof allowed_values !== 'undefined' && allowed_values.indexOf(value) == -1) {
          value = default_value;
        }
        return value;
      };
      getHeaderIdForField = function(field) {
        return 'header-' + field;
      };
      addCaretToElement = function(element, direction) {
        if (direction == 'desc') {
          element.insertAdjacentHTML('beforeend', ' <span class="oi oi-chevron-bottom"></span>');
        } else if (direction == 'asc') {
          element.insertAdjacentHTML('beforeend', ' <span class="oi oi-chevron-top"></span>');
        }
      };
      var orderby_default = 'timestamp';
      var orderby_allowed_values = ['id', 'timestamp', 'name'];
      var direction_default = 'asc';
      var direction_allowed_values = ['asc', 'desc'];
      displaySortingCaret = function() {
        var orderby = getQueryVariableOrDefault('orderby', orderby_default, true, orderby_allowed_values);
        var direction = getQueryVariableOrDefault('direction', direction_default, true, direction_allowed_values);
        var header = document.getElementById(getHeaderIdForField(orderby));
        if (header === null) {
          console.error("Could not get element by id: " + getHeaderIdForField(orderby));
          return {'orderby': null, 'direction': null};
        }
        addCaretToElement(header, direction);
        return {'orderby': orderby, 'direction': direction};
      };
      var current_sorting = displaySortingCaret();
      for (let i = orderby_allowed_values.length - 1; i >= 0; i--) {
        var id = getHeaderIdForField(orderby_allowed_values[i]);
        $("#" + id).on("click", function(){
          var new_order_by = orderby_allowed_values[i];
          var current_order_by = current_sorting['orderby'];
          if (new_order_by == current_order_by) {
            if (current_sorting['direction'] == 'asc') {
              var new_direction = 'desc';
            } else {
              var new_direction = 'asc';
            }
          } else {
            var new_direction = direction_default;
          }
          window.location = window.location.origin + window.location.pathname + "?orderby=" + new_order_by + "&direction=" + new_direction;
        });
      }
    </script>
  </body>
</html>
`

// To use: fmt.Sprintf(jobsTopTemplate, gitDescribeOutput, globals.ipAddrTCPPort, volumeName, {"FSCK"|"SCRUB"})
// TODO: Incorporate %[1]v (gitDescribeOutput) into navigation code below
const jobsTopTemplate string = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="/bootstrap.min.css">
    <link rel="stylesheet" href="/styles.css">
    <title>%[4]v Jobs %[3]v - %[2]v</title>
  </head>
  <body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark fixed-top">
      <a class="navbar-brand" href="#">%[2]v</a>
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
            <a class="nav-link" href="/metrics">StatsD/Prometheus</a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/trigger">Triggers</a>
          </li>
          <li class="nav-item active">
            <a class="nav-link" href="/volume">Volumes <span class="sr-only">(current)</span></a>
          </li>
        </ul>
      </div>
    </nav>
    <div class="container">
      <nav aria-label="breadcrumb">
        <ol class="breadcrumb">
          <li class="breadcrumb-item"><a href="/">Home</a></li>
          <li class="breadcrumb-item"><a href="/volume">Volumes</a></li>
          <li class="breadcrumb-item active" aria-current="page">%[4]v Jobs %[3]v</li>
        </ol>
      </nav>
      <h1 class="display-4">
        %[4]v Jobs
        <small class="text-muted">%[3]v</small>
      </h1>
      <table class="table table-sm table-striped table-hover">
        <thead>
          <tr>
            <th scope="col">Job ID</th>
            <th>Start Time</th>
            <th>End Time</th>
            <th>Status</th>
            <th class="fit">&nbsp;</th>
          </tr>
        </thead>
        <tbody>
`

// To use: fmt.Sprintf(jobsPerRunningJobTemplate, jobID, job.startTime.Format(time.RFC3339), volumeName, {"fsck"|"scrub"})
const jobsPerRunningJobTemplate string = `          <tr>
            <td>%[1]v</td>
            <td>%[2]v</td>
            <td></td>
            <td>Running</td>
            <td class="fit"><a href="/volume/%[3]v/%[4]v-job/%[1]v" class="btn btn-sm btn-primary">View</a></td>
          </tr>
`

// To use: fmt.Sprintf(jobsPerHaltedJobTemplate, jobID, job.startTime.Format(time.RFC3339), job.endTime.Format(time.RFC3339), volumeName, {"fsck"|"scrub"})
const jobsPerHaltedJobTemplate string = `          <tr class="table-info">
            <td>%[1]v</td>
            <td>%[2]v</td>
            <td>%[3]v</td>
            <td>Halted</td>
            <td class="fit"><a href="/volume/%[4]v/%[5]v-job/%[1]v" class="btn btn-sm btn-primary">View</a></td>
          </tr>
`

// To use: fmt.Sprintf(jobsPerSuccessfulJobTemplate, jobID, job.startTime.Format(time.RFC3339), job.endTime.Format(time.RFC3339), volumeName, {"fsck"|"scrub"})
const jobsPerSuccessfulJobTemplate string = `          <tr class="table-success">
            <td>%[1]v</td>
            <td>%[2]v</td>
            <td>%[3]v</td>
            <td>Successful</td>
            <td class="fit"><a href="/volume/%[4]v/%[5]v-job/%[1]v" class="btn btn-sm btn-primary">View</a></td>
          </tr>
`

// To use: fmt.Sprintf(jobsPerFailedJobTemplate, jobID, job.startTime.Format(time.RFC3339), job.endTime.Format(time.RFC3339), volumeName, {"fsck"|"scrub"})
const jobsPerFailedJobTemplate string = `          <tr class="table-danger">
            <td>%[1]v</td>
            <td>%[2]v</td>
            <td>%[3]v</td>
            <td>Failed</td>
            <td class="fit"><a href="/volume/%[4]v/%[5]v-job/%[1]v" class="btn btn-sm btn-primary">View</a></td>
          </tr>
`

const jobsListBottom string = `        </tbody>
      </table>
    <br />
`

// To use: fmt.Sprintf(jobsStartJobButtonTemplate, volumeName, {"fsck"|"scrub"})
const jobsStartJobButtonTemplate string = `    <form method="post" action="/volume/%[1]v/%[2]v-job">
      <input type="submit" value="Start new job" class="btn btn-sm btn-primary">
    </form>
`

const jobsBottom string = `    <script src="/jquery-3.2.1.min.js"></script>
    <script src="/popper.min.js"></script>
    <script src="/bootstrap.min.js"></script>
  </body>
</html>
`

// To use: fmt.Sprintf(jobTemplate, gitDescribeOutput, globals.ipAddrTCPPort, volumeName, {"FSCK"|"SCRUB"}, {"fsck"|"scrub"}, jobID, jobStatusJSONString)
// TODO: Incorporate %[1]v (gitDescribeOutput) into navigation code below
const jobTemplate string = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="/bootstrap.min.css">
    <link rel="stylesheet" href="/styles.css">
    <title>%[6]v %[4]v Job - %[2]v</title>
  </head>
  <body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark fixed-top">
      <a class="navbar-brand" href="#">%[2]v</a>
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
            <a class="nav-link" href="/metrics">StatsD/Prometheus</a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/trigger">Triggers</a>
          </li>
          <li class="nav-item active">
            <a class="nav-link" href="/volume">Volumes <span class="sr-only">(current)</span></a>
          </li>
        </ul>
      </div>
    </nav>
    <div class="container">
      <nav aria-label="breadcrumb">
        <ol class="breadcrumb">
          <li class="breadcrumb-item"><a href="/">Home</a></li>
          <li class="breadcrumb-item"><a href="/volume">Volumes</a></li>
          <li class="breadcrumb-item"><a href="/volume/%[3]v/%[5]v-job">%[4]v Jobs %[3]v</a></li>
          <li class="breadcrumb-item active" aria-current="page">%[6]v</li>
        </ol>
      </nav>
      <h1 class="display-4">
        %[4]v Job
        <small class="text-muted">%[6]v</small>
      </h1>
      <br>
      <dl class="row" id="job-info"></dl>
    </div>
    <script src="/jquery-3.2.1.min.js"></script>
    <script src="/popper.min.js"></script>
    <script src="/bootstrap.min.js"></script>
    <script type="text/javascript">
      var json_data = %[7]v;
      var getDescriptionListEntryMarkup = function(dt, dd) {
        var markup = "";
        markup += "         <dt class=\"col-sm-2\">\n";
        markup += "           " + dt + "\n";
        markup += "         </dt>\n";
        markup += "         <dd class=\"col-sm-10\">\n";
        markup += "           " + dd + "\n";
        markup += "         </dd>\n";
        return markup;
      };
      var getLogEntryContentsMarkup = function(log_entries, entries_type) {
        var log_entries_length = log_entries.length;
        if (log_entries_length == 0) {
          return "No " + entries_type;
        }
        var markup = "";
        var timestamp = "";
        var description = "";
        var timestamp_end_pos = 0;
        var entry = "";
        markup += "           <table class=\"table table-sm table-striped table-hover\">\n";
        for (var i = 0; i < log_entries_length; i++) {
          entry = log_entries[i];
          timestamp_end_pos = entry.indexOf(" ");
          timestamp = entry.slice(0, timestamp_end_pos);
          description = entry.slice(timestamp_end_pos);
          markup += "             <tr>\n";
          markup += "               <td class=\"fit align-text-top\">\n";
          markup += "                 <nobr>" + timestamp + "&nbsp;</nobr>\n";
          markup += "               </td>\n";
          markup += "               <td class=\"align-text-top\">\n";
          markup += "                 " + description + "\n";
          markup += "               </td>\n";
          markup += "             </tr>\n";
        }
        markup += "           </table>\n";
        return markup;
      };
      var job_info = "";
      if (json_data["halt time"] !== "") {
        var state = "Halted";
      } else if (json_data["done time"] !== "") {
        var state = "Completed";
      } else {
        var state = "Running";
      }
      job_info += getDescriptionListEntryMarkup("State", state);
      job_info += getDescriptionListEntryMarkup("Start time", json_data["start time"]);
      if (json_data["halt time"] !== "") {
        job_info += getDescriptionListEntryMarkup("Halt time", json_data["halt time"]);
      }
      if (json_data["done time"] !== "") {
        job_info += getDescriptionListEntryMarkup("Done time", json_data["done time"]);
      }
      job_info += getDescriptionListEntryMarkup("Errors", getLogEntryContentsMarkup(json_data["error list"], "errors"));
      job_info += getDescriptionListEntryMarkup("Info", getLogEntryContentsMarkup(json_data["info list"], "info"));
      document.getElementById("job-info").innerHTML = job_info;
    </script>
  </body>
</html>
`

// To use: fmt.Sprintf(layoutReportTopTemplate, gitDescribeOutput, globals.ipAddrTCPPort, volumeName)
// TODO: Incorporate %[1]v (gitDescribeOutput) into navigation code below
const layoutReportTopTemplate string = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="/bootstrap.min.css">
    <link rel="stylesheet" href="/styles.css">
    <title>Layout Report %[3]v - %[2]v</title>
  </head>
  <body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark fixed-top">
      <a class="navbar-brand" href="#">%[2]v</a>
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
            <a class="nav-link" href="/metrics">StatsD/Prometheus</a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/trigger">Triggers</a>
          </li>
          <li class="nav-item active">
            <a class="nav-link" href="/volume">Volumes <span class="sr-only">(current)</span></a>
          </li>
        </ul>
      </div>
    </nav>
    <div class="container">
      <nav aria-label="breadcrumb">
        <ol class="breadcrumb">
          <li class="breadcrumb-item"><a href="/">Home</a></li>
          <li class="breadcrumb-item"><a href="/volume">Volumes</a></li>
          <li class="breadcrumb-item active" aria-current="page">Layout Report %[3]v</li>
        </ol>
      </nav>
      <h1 class="display-4">
        Layout Report
        <small class="text-muted">%[3]v</small>
      </h1>
`

// To use: fmt.Sprintf(layoutReportTableTopTemplate, TreeName)
const layoutReportTableTopTemplate string = `      <br>
      <h3>%[1]v</h3>
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
    <script src="/jquery-3.2.1.min.js"></script>
    <script src="/popper.min.js"></script>
    <script src="/bootstrap.min.js"></script>
  </body>
</html>
`

// To use: fmt.Sprintf(extentMapTemplate, gitDescribeOutput, globals.ipAddrTCPPort, volumeName, extentMapJSONString, pathDoubleQuotedString, serverErrorBoolString)
// TODO: Incorporate %[1]v (gitDescribeOutput) into navigation code below
const extentMapTemplate string = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="/bootstrap.min.css">
    <link rel="stylesheet" href="/styles.css">
    <title>Extent Map %[3]v - %[2]v</title>
  </head>
  <body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark fixed-top">
      <a class="navbar-brand" href="#">%[2]v</a>
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
            <a class="nav-link" href="/metrics">StatsD/Prometheus</a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/trigger">Triggers</a>
          </li>
          <li class="nav-item active">
            <a class="nav-link" href="/volume">Volumes <span class="sr-only">(current)</span></a>
          </li>
        </ul>
      </div>
    </nav>
    <div class="container">
      <nav aria-label="breadcrumb">
        <ol class="breadcrumb">
          <li class="breadcrumb-item"><a href="/">Home</a></li>
          <li class="breadcrumb-item"><a href="/volume">Volumes</a></li>
          <li class="breadcrumb-item active" aria-current="page">Extent Map %[3]v</li>
        </ol>
      </nav>

      <h1 class="display-4">
        Extent Map
        <small class="text-muted">%[3]v</small>
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
    <script src="/jquery-3.2.1.min.js"></script>
    <script src="popper.min.js"></script>
    <script src="/bootstrap.min.js"></script>
    <script type="text/javascript">
      var json_data = %[4]v
      var path = %[5]v;
      var volume = "%[3]v";
      var server_error = %[6]v;

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

// To use: fmt.Sprintf(triggerTopTemplate, gitDescribeOutput, globals.ipAddrTCPPort)
// TODO: Incorporate %[1]v (gitDescribeOutput) into navigation code below
const triggerTopTemplate string = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="/bootstrap.min.css">
    <link rel="stylesheet" href="/styles.css">
    <title>Triggers - %[2]v</title>
  </head>
  <body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark fixed-top">
      <a class="navbar-brand" href="#">%[2]v</a>
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
            <a class="nav-link" href="/metrics">StatsD/Prometheus</a>
          </li>
          <li class="nav-item active">
            <a class="nav-link" href="/trigger">Triggers <span class="sr-only">(current)</span></a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="/volume">Volumes</a>
          </li>
        </ul>
      </div>
    </nav>
    <div class="container">
      <nav aria-label="breadcrumb">
        <ol class="breadcrumb">
          <li class="breadcrumb-item"><a href="/">Home</a></li>
          <li class="breadcrumb-item active" aria-current="page">Triggers</li>
        </ol>
      </nav>
      <h1 class="display-4">Triggers</h1>
`

const triggerAllActive string = `      <div class="text-center">
        <div class="btn-group">
          <a href="/trigger" class="btn btn-sm btn-primary active">All</a>
          <a href="/trigger?armed=true" class="btn btn-sm btn-primary">Armed</a>
          <a href="/trigger?armed=false" class="btn btn-sm btn-primary">Disarmed</a>
        </div>
      </div>
`

const triggerArmedActive string = `      <div class="text-center">
        <div class="btn-group">
          <a href="/trigger" class="btn btn-sm btn-primary">All</a>
          <a href="/trigger?armed=true" class="btn btn-sm btn-primary active">Armed</a>
          <a href="/trigger?armed=false" class="btn btn-sm btn-primary">Disarmed</a>
        </div>
      </div>
`

const triggerDisarmedActive string = `      <div class="text-center">
        <div class="btn-group">
          <a href="/trigger" class="btn btn-sm btn-primary">All</a>
          <a href="/trigger?armed=true" class="btn btn-sm btn-primary">Armed</a>
          <a href="/trigger?armed=false" class="btn btn-sm btn-primary active">Disarmed</a>
        </div>
      </div>
`

const triggerTableTop string = `      <br>
      <table class="table table-sm table-striped table-hover">
        <thead>
          <tr>
            <th scope="col">Halt Label</th>
            <th scope="col" class="w-25">Halt After Count</th>
          </tr>
        </thead>
        <tbody>
`

// To use: fmt.Sprintf(triggerTableRowTemplate, haltTriggerString, haltTriggerCount)
const triggerTableRowTemplate string = `          <tr>
            <td class="halt-label">%[1]v</td>
            <td>
              <div class="input-group">
                <input type="number" class="form-control form-control-sm haltTriggerCount" min="0" max="4294967295" value="%[2]v">
                <div class="valid-feedback">
                  New value successfully saved.
                </div>
                <div class="invalid-feedback">
                  There was an error saving the new value.
                </div>
              </div>
            </td>
          </tr>
`

const triggerBottom string = `        </tbody>
      </table>
    </div>
    <script src="/jquery-3.2.1.min.js"></script>
    <script src="/popper.min.js"></script>
    <script src="/bootstrap.min.js"></script>
    <script type="text/javascript">
      markValid = function(elem) {elem.removeClass("is-valid is-invalid").addClass("is-valid");};
      markInvalid = function(elem) {elem.removeClass("is-valid is-invalid").addClass("is-invalid");};
      unmark = function(elem) {elem.removeClass("is-valid is-invalid");};
      updateErrorMsg = function(elem, text) {elem.siblings(".invalid-feedback").html(text);};
      getLabelForCount = function(elem) {return elem.parent().parent().siblings(".halt-label").html();};
      var timeout_unmark = 2000;
      $(".haltTriggerCount").on("change", function(){
        that = $( this );
        $.ajax({
          url: '/trigger/' + getLabelForCount(that),
          method: 'POST',
          data: {'count': that.val()},
          dataType: 'json',
          success: function(data, textStatus, jqXHR) {
            markValid(that);
            window.setTimeout(function(){unmark(that);}, timeout_unmark);
          },
          error: function(jqXHR, textStatus, errorThrown) {
            var msg = "Error: " + jqXHR.status + " " + jqXHR.statusText  // Do we want to use jqXHR.responseText?
            updateErrorMsg(that, msg);
            markInvalid(that);
          }
        });
      });
    </script>
  </body>
</html>
`
