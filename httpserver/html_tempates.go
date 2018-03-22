package httpserver

type staticContentType struct {
	contentType string
	content     []byte
}

var stylesDotCSS = &staticContentType{
	contentType: "text/css",
	content: []byte(`.table td.fit,
.table th.fit {
  white-space: nowrap;
  width: 1%;
}

body { padding-top: 70px; }

.no-margin { margin: 0; }

pre.code {
  background-color: #e9ecef;
  border-radius: .25rem;
  padding: 20px;
}

span.jstExpand, span.jstFold {
  cursor: pointer;
}

.jstValue {
  white-space: pre-wrap;
}
.jstComma {
  white-space: pre-wrap;
}
.jstProperty {
  color: #666;
  word-wrap: break-word;
}
.jstBracket {
  white-space: pre-wrap;;
}
.jstBool {
  color: #2525CC;
}
.jstNum {
  color: #D036D0;
}
.jstNull {
  color: gray;
}
.jstStr {
  color: #2DB669;
}
.jstFold:after {
  content: ' -';
  cursor: pointer;
}
.jstExpand {
  white-space: normal;
}
.jstExpand:after {
  content: ' +';
  cursor: pointer;
}
.jstFolded {
  white-space: normal !important;
}
.jstHiddenBlock {
  display: none;
}
`),
}

var jsontreeDotJS = &staticContentType{
	contentType: "application/javascript",
	content: []byte(`var JSONTree = (function() {

  var escapeMap = {
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    '\'': '&#x27;',
    '/': '&#x2F;'
  };

  var defaultSettings = {
    indent: 2
  };

  var id = 0;
  var instances = 0;
  var current_collapse_level = null;
  var ids_to_collapse = [];

  this.create = function(data, settings, collapse_depth) {
    current_collapse_level = typeof collapse_depth !== 'undefined' ? collapse_depth : -1;
    instances += 1;
    return _span(_jsVal(data, 0, false), {class: 'jstValue'});
  };

  this.collapse = function() {
    var arrayLength = ids_to_collapse.length;
    for (var i = 0; i < arrayLength; i++) {
      JSONTree.toggle(ids_to_collapse[i]);
    }
  };

  var _escape = function(text) {
    return text.replace(/[&<>'"]/g, function(c) {
      return escapeMap[c];
    });
  };

  var _id = function() {
    return instances + '_' + id++;
  };

  var _lastId = function() {
    return instances + '_' + (id - 1);
  };

  var _jsVal = function(value, depth, indent) {
    if (value !== null) {
      var type = typeof value;
      switch (type) {
        case 'boolean':
          return _jsBool(value, indent ? depth : 0);
        case 'number':
          return _jsNum(value, indent ? depth : 0);
        case 'string':
          return _jsStr(value, indent ? depth : 0);
        default:
          if (value instanceof Array) {
            return _jsArr(value, depth, indent);
          } else {
            return _jsObj(value, depth, indent);
          }
      }
    } else {
      return _jsNull(indent ? depth : 0);
    }
  };

  var _jsObj = function(object, depth, indent) {
    var id = _id();
    _decrementCollapseLevel("_jsObj");
    var content = Object.keys(object).map(function(property) {
      return _property(property, object[property], depth + 1, true);
    }).join(_comma());
    var body = [
      _openBracket('{', indent ? depth : 0, id),
      _span(content, {id: id}),
      _closeBracket('}', depth)
    ].join('\n');
    _incrementCollapseLevel("_jsObj");
    return _span(body, {});
  };

  var _jsArr = function(array, depth, indent) {
    var id = _id();
    _decrementCollapseLevel("_jsArr");
    var body = array.map(function(element) {
      return _jsVal(element, depth + 1, true);
    }).join(_comma());
    var arr = [
      _openBracket('[', indent ? depth : 0, id),
      _span(body, {id: id}),
      _closeBracket(']', depth)
    ].join('\n');
    _incrementCollapseLevel("_jsArr");
    return arr;
  };

  var _jsStr = function(value, depth) {
    var jsonString = _escape(JSON.stringify(value));
    return _span(_indent(jsonString, depth), {class: 'jstStr'});
  };

  var _jsNum = function(value, depth) {
    return _span(_indent(value, depth), {class: 'jstNum'});
  };

  var _jsBool = function(value, depth) {
    return _span(_indent(value, depth), {class: 'jstBool'});
  };

  var _jsNull = function(depth) {
    return _span(_indent('null', depth), {class: 'jstNull'});
  };

  var _property = function(name, value, depth) {
    var property = _indent(_escape(JSON.stringify(name)) + ': ', depth);
    var propertyValue = _span(_jsVal(value, depth, false), {});
    return _span(property + propertyValue, {class: 'jstProperty'});
  };

  var _comma = function() {
    return _span(',\n', {class: 'jstComma'});
  };

  var _span = function(value, attrs) {
    return _tag('span', attrs, value);
  };

  var _tag = function(tag, attrs, content) {
    return '<' + tag + Object.keys(attrs).map(function(attr) {
          return ' ' + attr + '="' + attrs[attr] + '"';
        }).join('') + '>' +
        content +
        '</' + tag + '>';
  };

  var _openBracket = function(symbol, depth, id) {
    return (
    _span(_indent(symbol, depth), {class: 'jstBracket'}) +
    _span('', {class: 'jstFold', onclick: 'JSONTree.toggle(\'' + id + '\')'})
    );
  };

  this.toggle = function(id) {
    var element = document.getElementById(id);
    var parent = element.parentNode;
    var toggleButton = element.previousElementSibling;
    if (element.className === '') {
      element.className = 'jstHiddenBlock';
      parent.className = 'jstFolded';
      toggleButton.className = 'jstExpand';
    } else {
      element.className = '';
      parent.className = '';
      toggleButton.className = 'jstFold';
    }
  };

  var _closeBracket = function(symbol, depth) {
    return _span(_indent(symbol, depth), {});
  };

  var _indent = function(value, depth) {
    return Array((depth * 2) + 1).join(' ') + value;
  };

  var _decrementCollapseLevel = function(caller) {
    if (current_collapse_level <= 0) {
      ids_to_collapse.push(_lastId());
    } else {
    }
    current_collapse_level--;
  };

  var _incrementCollapseLevel = function(caller) {
    current_collapse_level++;
  };

  return this;
})();
`),
}

// To use: fmt.Sprintf(indexDotHTMLTemplate, globals.ipAddrTCPPort)
const indexDotHTMLTemplate string = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css" integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm" crossorigin="anonymous">
    <link rel="stylesheet" href="/styles.css">
    <title>ProxyFS Management - %[1]v</title>
  </head>
  <body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark fixed-top">
      <a class="navbar-brand" href="#">%[1]v</a>
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
    <script src="https://code.jquery.com/jquery-3.2.1.slim.min.js" integrity="sha384-KJ3o2DKtIkvYIK3UENzmM7KCkRr/rE9/Qpg6aAZGJwFDMVNA/GpGFF93hXpG5KkN" crossorigin="anonymous"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.12.9/umd/popper.min.js" integrity="sha384-ApNbgh9B+Y1QKtv3Rn7W3mgPxhU9K/ScQsAP7hUibX39j7fakFPskvXusvfa0b4Q" crossorigin="anonymous"></script>
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/js/bootstrap.min.js" integrity="sha384-JZR6Spejh4U02d8jOt6vLEHfe/JQGiRRSQQxSfFWpi1MquVdAyjUar5+76PVCmYl" crossorigin="anonymous"></script>
  </body>
</html>
`

// To use: fmt.Sprintf(configTemplate, globals.ipAddrTCPPort, confMapJSONString)
const configTemplate string = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css" integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm" crossorigin="anonymous">
    <link rel="stylesheet" href="/styles.css">
    <title>Config - %[1]v</title>
  </head>
  <body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark fixed-top">
      <a class="navbar-brand" href="#">%[1]v</a>
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
    <script src="https://code.jquery.com/jquery-3.2.1.slim.min.js" integrity="sha384-KJ3o2DKtIkvYIK3UENzmM7KCkRr/rE9/Qpg6aAZGJwFDMVNA/GpGFF93hXpG5KkN" crossorigin="anonymous"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.12.9/umd/popper.min.js" integrity="sha384-ApNbgh9B+Y1QKtv3Rn7W3mgPxhU9K/ScQsAP7hUibX39j7fakFPskvXusvfa0b4Q" crossorigin="anonymous"></script>
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/js/bootstrap.min.js" integrity="sha384-JZR6Spejh4U02d8jOt6vLEHfe/JQGiRRSQQxSfFWpi1MquVdAyjUar5+76PVCmYl" crossorigin="anonymous"></script>
    <script src="/jsontree.js"></script>
    <script type="text/javascript">
      var json = %[2]v;
      document.getElementById("json_data").innerHTML = JSONTree.create(json, null, 1);
      JSONTree.collapse();
    </script>
  </body>
</html>
`

// To use: fmt.Sprintf(volumeListTopTemplate, globals.ipAddrTCPPort)
const volumeListTopTemplate string = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css" integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm" crossorigin="anonymous">
    <link rel="stylesheet" href="/styles.css">
    <title>Volumes - %[1]v</title>
  </head>
  <body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark fixed-top">
      <a class="navbar-brand" href="#">%[1]v</a>
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
          </tr>
        </thead>
        <tbody>
`

// To use: fmt.Sprintf(volumeListPerVolumeTemplate, volumeName)
const volumeListPerVolumeTemplate string = `          <tr>
            <td>%[1]v</td>
            <td class="fit"><a href="/volume/%[1]v/fsck-job" class="btn btn-sm btn-primary">FSCK jobs</a></td>
            <td class="fit"><a href="/volume/%[1]v/scrub-job" class="btn btn-sm btn-primary">SCRUB jobs</a></td>
            <td class="fit"><a href="/volume/%[1]v/layout-report" class="btn btn-sm btn-primary">Layout Report</a></td>
          </tr>
`

const volumeListBottom string = `        </tbody>
      </table>
    </div>
    <script src="https://code.jquery.com/jquery-3.2.1.slim.min.js" integrity="sha384-KJ3o2DKtIkvYIK3UENzmM7KCkRr/rE9/Qpg6aAZGJwFDMVNA/GpGFF93hXpG5KkN" crossorigin="anonymous"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.12.9/umd/popper.min.js" integrity="sha384-ApNbgh9B+Y1QKtv3Rn7W3mgPxhU9K/ScQsAP7hUibX39j7fakFPskvXusvfa0b4Q" crossorigin="anonymous"></script>
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/js/bootstrap.min.js" integrity="sha384-JZR6Spejh4U02d8jOt6vLEHfe/JQGiRRSQQxSfFWpi1MquVdAyjUar5+76PVCmYl" crossorigin="anonymous"></script>
  </body>
</html>
`

// To use: fmt.Sprintf(jobTopTemplate, globals.ipAddrTCPPort, volumeName, {"FSCK"|"SCRUB"})
const jobTopTemplate string = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css" integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm" crossorigin="anonymous">
    <link rel="stylesheet" href="/styles.css">
    <title>%[3]v Jobs %[2]v - %[1]v</title>
  </head>
  <body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark fixed-top">
      <a class="navbar-brand" href="#">%[1]v</a>
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
          <li class="breadcrumb-item active" aria-current="page">%[3]v Jobs %[2]v</li>
        </ol>
      </nav>
      <h1 class="display-4">
        %[3]v Jobs
        <small class="text-muted">%[2]v</small>
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

// To use: fmt.Sprintf(jobPerRunningJobTemplate, jobID, job.startTime.Format(time.RFC3339), volumeName, {"fsck"|"scrub"})
const jobPerRunningJobTemplate string = `          <tr>
            <td>%[1]v</td>
            <td>%[2]v</td>
            <td></td>
            <td>Running</td>
            <td class="fit"><a href="/volume/%[3]v/%[4]v-job/%[1]v" class="btn btn-sm btn-primary">View</a></td>
          </tr>
`

// To use: fmt.Sprintf(jobPerHaltedJobTemplate, jobID, job.startTime.Format(time.RFC3339), job.endTime.Format(time.RFC3339), volumeName, {"fsck"|"scrub"})
const jobPerHaltedJobTemplate string = `          <tr class="table-info">
            <td>%[1]v</td>
            <td>%[2]v</td>
            <td>%[3]v</td>
            <td>Halted</td>
            <td class="fit"><a href="/volume/%[4]v/%[5]v-job/%[1]v" class="btn btn-sm btn-primary">View</a></td>
          </tr>
`

// To use: fmt.Sprintf(jobPerSuccessfulJobTemplate, jobID, job.startTime.Format(time.RFC3339), job.endTime.Format(time.RFC3339), volumeName, {"fsck"|"scrub"})
const jobPerSuccessfulJobTemplate string = `          <tr class="table-success">
            <td>%[1]v</td>
            <td>%[2]v</td>
            <td>%[3]v</td>
            <td>Successful</td>
            <td class="fit"><a href="/volume/%[4]v/%[5]v-job/%[1]v" class="btn btn-sm btn-primary">View</a></td>
          </tr>
`

// To use: fmt.Sprintf(jobPerFailedJobTemplate, jobID, job.startTime.Format(time.RFC3339), job.endTime.Format(time.RFC3339), volumeName, {"fsck"|"scrub"})
const jobPerFailedJobTemplate string = `          <tr class="table-danger">
            <td>%[1]v</td>
            <td>%[2]v</td>
            <td>%[3]v</td>
            <td>Failed</td>
            <td class="fit"><a href="/volume/%[4]v/%[5]v-job/%[1]v" class="btn btn-sm btn-primary">View</a></td>
          </tr>
`

const jobListBottom string = `        </tbody>
      </table>
    <br />
`

// To use: fmt.Sprintf(fsckJobStartJobButtonTemplate, volumeName, {"fsck"|"scrub"})
const jobStartJobButtonTemplate string = `    <form method="post" action="/volume/%[1]v/%[2]v-job">
      <input type="submit" value="Start new job" class="btn btn-sm btn-primary">
    </form>
`

const jobBottom string = `    <script src="https://code.jquery.com/jquery-3.2.1.slim.min.js" integrity="sha384-KJ3o2DKtIkvYIK3UENzmM7KCkRr/rE9/Qpg6aAZGJwFDMVNA/GpGFF93hXpG5KkN" crossorigin="anonymous"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.12.9/umd/popper.min.js" integrity="sha384-ApNbgh9B+Y1QKtv3Rn7W3mgPxhU9K/ScQsAP7hUibX39j7fakFPskvXusvfa0b4Q" crossorigin="anonymous"></script>
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/js/bootstrap.min.js" integrity="sha384-JZR6Spejh4U02d8jOt6vLEHfe/JQGiRRSQQxSfFWpi1MquVdAyjUar5+76PVCmYl" crossorigin="anonymous"></script>
  </body>
</html>
`

// To use: fmt.Sprintf(layoutReportTopTemplate, globals.ipAddrTCPPort, volumeName)
const layoutReportTopTemplate string = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css" integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm" crossorigin="anonymous">
    <link rel="stylesheet" href="/styles.css">
    <title>Layout Report %[2]v - %[1]v</title>
  </head>
  <body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark fixed-top">
      <a class="navbar-brand" href="#">%[1]v</a>
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
          <li class="breadcrumb-item active" aria-current="page">Layout Report CommonVolume</li>
        </ol>
      </nav>
      <h1 class="display-4">
        Layout Report
        <small class="text-muted">%[2]v</small>
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
    <script src="https://code.jquery.com/jquery-3.2.1.slim.min.js" integrity="sha384-KJ3o2DKtIkvYIK3UENzmM7KCkRr/rE9/Qpg6aAZGJwFDMVNA/GpGFF93hXpG5KkN" crossorigin="anonymous"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.12.9/umd/popper.min.js" integrity="sha384-ApNbgh9B+Y1QKtv3Rn7W3mgPxhU9K/ScQsAP7hUibX39j7fakFPskvXusvfa0b4Q" crossorigin="anonymous"></script>
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/js/bootstrap.min.js" integrity="sha384-JZR6Spejh4U02d8jOt6vLEHfe/JQGiRRSQQxSfFWpi1MquVdAyjUar5+76PVCmYl" crossorigin="anonymous"></script>
  </body>
</html>
`

// To use: fmt.Sprintf(triggerTopTemplate, globals.ipAddrTCPPort)
const triggerTopTemplate string = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css" integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm" crossorigin="anonymous">
    <link rel="stylesheet" href="./styles.css">
    <title>Triggers - %[1]v</title>
  </head>
  <body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark fixed-top">
      <a class="navbar-brand" href="#">%[1]v</a>
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
    <!-- ALERT! Here we're importing a different jQuery version (jquery-3.2.1.min.js instead of jquery-3.2.1.slim.min.js), with more function that we need for Ajax requests. -->
    <script src="https://code.jquery.com/jquery-3.2.1.min.js" integrity="sha384-xBuQ/xzmlsLoJpyjoggmTEz8OWUFM0/RC5BsqQBDX2v5cMvDHcMakNTNrHIW2I5f" crossorigin="anonymous"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.12.9/umd/popper.min.js" integrity="sha384-ApNbgh9B+Y1QKtv3Rn7W3mgPxhU9K/ScQsAP7hUibX39j7fakFPskvXusvfa0b4Q" crossorigin="anonymous"></script>
    <script src="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/js/bootstrap.min.js" integrity="sha384-JZR6Spejh4U02d8jOt6vLEHfe/JQGiRRSQQxSfFWpi1MquVdAyjUar5+76PVCmYl" crossorigin="anonymous"></script>
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
