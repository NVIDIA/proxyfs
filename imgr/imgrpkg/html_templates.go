// Copyright (c) 2015-2021, NVIDIA CORPORATION.
// SPDX-License-Identifier: Apache-2.0

package imgrpkg

// To use: fmt.Sprintf(indexDotHTMLTemplate, proxyfsVersion)
//                                               %[1]v
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
            <p class="card-text">Examine volumes currently active on this imgr instance.</p>
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
//                                          %[1]v            %[2]v
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

// To use: fmt.Sprintf(volumeListTemplate, proxyfsVersion, volumeListJSONString)
//                                             %[1]v              %[2]v
const volumeListTemplate string = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="/bootstrap.min.css">
    <link rel="stylesheet" href="/styles.css">
    <link href="/open-iconic/font/css/open-iconic-bootstrap.min.css" rel="stylesheet">
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
          </tr>
        </thead>
        <tbody id="volumes-data"></tbody>
        </tbody>
      </table>
      <!-- Back to top button -->
      <button type="button" class="btn btn-primary btn-floating btn-lg" id="btn-back-to-top">
        <span class="oi oi-chevron-top"></span>
      </button>
    </div>
    <script src="/jquery.min.js"></script>
    <script src="/popper.min.js"></script>
    <script src="/bootstrap.min.js"></script>
    <script src="/utils.js"></script>
    <script type="text/javascript">
      const json_data = %[2]v;

      const getVolumesTableMarkupWithData = function(data) {
        let table_markup = "";
        data.forEach((volume) => {
          table_markup += "          <tr>\n";
          table_markup += "            <td>" + volume["Name"] + "</td>\n";
          table_markup += "            <td class=\"fit\"><a href=\"/volume/" + volume["Name"] + "\" class=\"btn btn-sm btn-primary\">Details</a></td>\n";
          table_markup += "          </tr>";
        });
        return table_markup;
      };

      // Fill table
      document.getElementById("volumes-data").innerHTML = getVolumesTableMarkupWithData(json_data);

      // Fancy back to top behavior
      addBackToTopBehavior();
    </script>
  </body>
</html>
`

// To use: fmt.Sprintf(volumeTemplate, proxyfsVersion, volumeName, volumeJSONString)
//                                          %[1]v        %[2]v          %[3]v
const volumeTemplate string = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="/bootstrap.min.css">
    <link rel="stylesheet" href="/styles.css">
    <link href="/open-iconic/font/css/open-iconic-bootstrap.min.css" rel="stylesheet">
    <title>Volume %[2]v</title>
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
          <li class="breadcrumb-item active" aria-current="page">%[2]v</li>
        </ol>
      </nav>

      <h1 class="display-4">
        Volume
        <small class="text-muted">%[2]v</small>
      </h1>

      <div id="table-container">
        <br>

        <table class="table table-sm table-striped table-hover">
          <tbody id="key-pair-data"></tbody>
        </table>
      </div>
      <!-- Back to top button -->
      <button type="button" class="btn btn-primary btn-floating btn-lg" id="btn-back-to-top">
        <span class="oi oi-chevron-top"></span>
      </button>
    </div>
    <script src="/jquery.min.js"></script>
    <script src="/popper.min.js"></script>
    <script src="/bootstrap.min.js"></script>
    <script src="/utils.js"></script>
    <script type="text/javascript">
      const json_data = %[3]v;

      /*********************************************************************************
      * WARNING! A lot of the code in this page is duplicated in the inode details     *
      * page, so if you're changing anything here, you might as well want to change    *
      * it in the other page.                                                          *
      *********************************************************************************/

      const getListOfComplexKeys = function(data) {
        let keys = [];
        for (let key in data) {
          if (
            Array.isArray(data[key]) &&                                         // An array
            data[key].length !== 0 &&                                           // not empty
            Object.prototype.toString.call(data[key][0]) === "[object Object]"  // with "[object Object]" elements (ie: dictionaries, because we're getting this from JSON)
          ) {
            keys.push(key);
          }
        }
        return keys;
      };

      const getSimpleKeyValuePairsTableMarkupWithData = function(data, complex_keys) {
        let table_markup = "";
        for (let key in data) {
          if (complex_keys.includes(key)) {
            table_markup += "          <tr>\n";
            table_markup += "            <th scope=\"row\">" + key + "</th>\n";
            table_markup += "            <td class=\"text-right\"><a href='#" + key + "-table'>Go to " + key + "</a></td>\n";
            table_markup += "          </tr>\n";
          } else if (Array.isArray(data[key]) && data[key].length === 0) {
            table_markup += "          <tr>\n";
            table_markup += "            <th scope=\"row\">" + key + "</th>\n";
            table_markup += "            <td class=\"text-right\">[ empty ]</td>\n";
            table_markup += "          </tr>\n";
          } else if (Array.isArray(data[key]) && data[key].length !== 0) {
            let first = true;
            data[key].forEach((item) => {
              table_markup += "          <tr>\n";
              if (first) {
                table_markup += "            <th scope=\"row\">" + key + "</th>\n";
                first = false;
              } else {
                table_markup += "            <th scope=\"row\"></th>\n";
              }
              table_markup += "            <td class=\"text-right\">" + item + "</td>\n";
              table_markup += "          </tr>\n";
            });
          } else {
            table_markup += "          <tr>\n";
            table_markup += "            <th scope=\"row\">" + key + "</th>\n";
            table_markup += "            <td class=\"text-right\">" + data[key] + "</td>\n";
            table_markup += "          </tr>\n";
          }
        }
        return table_markup;
      };

      const getColsClass = function(keys) {
        const keys_length = keys.length;
        if (keys_length === 2) {
          return " class=\"w-50\"";
        } else if (keys_length === 3) {
          return " class=\"w-25\"";
        } else {
          return "";
        }
      };

      const getOnclickForInode = function(volume_name, inode_number) {
        return " onclick=\"window.location.assign('/volume/" + volume_name + "/inode/" + inode_number + "');\"";
      };

      const getComplexDataTableMarkup = function(data, key) {
        let table_markup = "";
        const data_for_key = data[key];
        const object_keys = Object.keys(data_for_key[0]);
        let cols_class = getColsClass(object_keys);
        table_markup += "      <br>";
        table_markup += "      <div class=\"d-flex justify-content-between\">";
        table_markup += "        <h3>" + key + "</h3>";
        table_markup += "      </div>";
        table_markup += "      <table class=\"table table-sm table-striped table-hover\" id=\"" + key + "-table\">";
        table_markup += "        <thead>";
        table_markup += "          <tr>";
        object_keys.forEach ((object_key) => {
          table_markup += "            <th scope=\"col\"" + cols_class + ">" + object_key + "</th>";
        });
        table_markup += "          </tr>";
        table_markup += "        </thead>";
        table_markup += "        <tbody>";
        data_for_key.forEach((element) => {
          if (key === "InodeTable") {
            table_markup += "          <tr class=\"clickable\"" + getOnclickForInode(data["Name"], element["InodeNumber"]) + ">";
          } else {
            table_markup += "          <tr>";
          }
          object_keys.forEach ((object_key) => {
            table_markup += "            <td><pre class=\"no-margin\">" + element[object_key] + "</pre></td>";
          });
          table_markup += "          </tr>";
        });
        table_markup += "        </tbody>";
        table_markup += "      </table>";
        return table_markup;
      }

      const addTablesForComplexData = function(data, complex_keys) {
        let complex_tables_markup = "";
        complex_keys.forEach((key) => {
          complex_tables_markup += getComplexDataTableMarkup(json_data, key);
        });
        document.getElementById("table-container").innerHTML = document.getElementById("table-container").innerHTML + complex_tables_markup;
      }

      // Create / fill tables
      const complex_keys = getListOfComplexKeys(json_data);
      document.getElementById("key-pair-data").innerHTML = getSimpleKeyValuePairsTableMarkupWithData(json_data, complex_keys);
      addTablesForComplexData(json_data, complex_keys);

      // Fancy back to top behavior
      addBackToTopBehavior();
    </script>
  </body>
</html>
`

// To use: fmt.Sprintf(inodeTemplate, proxyfsVersion, volumeName, inodeNumber, inodeJSONString)
//                                          %[1]v       %[2]v        %[3]v          %[4]v
const inodeTemplate string = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="/bootstrap.min.css">
    <link rel="stylesheet" href="/styles.css">
    <link href="/open-iconic/font/css/open-iconic-bootstrap.min.css" rel="stylesheet">
    <title>Inode %[3]v | Volume %[2]v</title>
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
          <li class="breadcrumb-item"><a href="/volume/%[2]v">%[2]v</a></li>
          <li class="breadcrumb-item"><a href="/volume/%[2]v#InodeTable-table">inode</a></li>
          <li class="breadcrumb-item active" aria-current="page">%[3]v</li>
        </ol>
      </nav>

      <h1 class="display-4">
        Inode
        <small class="text-muted">%[3]v</small>
      </h1>

      <h2><small class="text-muted">Volume testvol</small></h2>

      <div id="table-container">
        <br>

        <table class="table table-sm table-striped table-hover">
          <tbody id="key-pair-data"></tbody>
        </table>
      </div>
      <!-- Back to top button -->
      <button type="button" class="btn btn-primary btn-floating btn-lg" id="btn-back-to-top">
        <span class="oi oi-chevron-top"></span>
      </button>
    </div>
    <script src="/jquery.min.js"></script>
    <script src="/popper.min.js"></script>
    <script src="/bootstrap.min.js"></script>
    <script src="/utils.js"></script>
    <script type="text/javascript">
      const json_data = %[4]v;

      /*********************************************************************************
      * WARNING! A lot of the code in this page is duplicated in the volume details    *
      * page, so if you're changing anything here, you might as well want to change    *
      * it in the other page.                                                          *
      *********************************************************************************/

      const getListOfComplexKeys = function(data) {
        let keys = [];
        for (let key in data) {
          if (
            Array.isArray(data[key]) &&                                         // An array
            data[key].length !== 0 &&                                           // not empty
            Object.prototype.toString.call(data[key][0]) === "[object Object]"  // with "[object Object]" elements (ie: dictionaries, because we're getting this from JSON)
          ) {
            keys.push(key);
          }
        }
        return keys;
      };

      const getSimpleKeyValuePairsTableMarkupWithData = function(data, complex_keys) {
        let table_markup = "";
        for (let key in data) {
          if (complex_keys.includes(key)) {
            table_markup += "          <tr>\n";
            table_markup += "            <th scope=\"row\">" + key + "</th>\n";
            table_markup += "            <td class=\"text-right\"><a href='#" + key + "-table'>Go to " + key + "</a></td>\n";
            table_markup += "          </tr>\n";
          } else if (Array.isArray(data[key]) && data[key].length === 0) {
            table_markup += "          <tr>\n";
            table_markup += "            <th scope=\"row\">" + key + "</th>\n";
            table_markup += "            <td class=\"text-right\">[ empty ]</td>\n";
            table_markup += "          </tr>\n";
          } else if (Array.isArray(data[key]) && data[key].length !== 0) {
            let first = true;
            data[key].forEach((item) => {
              table_markup += "          <tr>\n";
              if (first) {
                table_markup += "            <th scope=\"row\">" + key + "</th>\n";
                first = false;
              } else {
                table_markup += "            <th scope=\"row\"></th>\n";
              }
              table_markup += "            <td class=\"text-right\">" + item + "</td>\n";
              table_markup += "          </tr>\n";
            });
          } else {
            table_markup += "          <tr>\n";
            table_markup += "            <th scope=\"row\">" + key + "</th>\n";
            table_markup += "            <td class=\"text-right\">" + data[key] + "</td>\n";
            table_markup += "          </tr>\n";
          }
        }
        return table_markup;
      };

      const getColsClass = function(keys) {
        const keys_length = keys.length;
        if (keys_length === 2) {
          return " class=\"w-50\"";
        } else if (keys_length === 3) {
          return " class=\"w-25\"";
        } else {
          return "";
        }
      };

      const getOnclickForInode = function(volume_name, inode_number) {
        return " onclick=\"window.location.assign('./" + inode_number + "');\"";
      };

      const getComplexDataTableMarkup = function(data, key) {
        let table_markup = "";
        const data_for_key = data[key];
        const object_keys = Object.keys(data_for_key[0]);
        let cols_class = getColsClass(object_keys);
        table_markup += "      <br>";
        table_markup += "      <div class=\"d-flex justify-content-between\">";
        table_markup += "        <h3>" + key + "</h3>";
        table_markup += "      </div>";
        table_markup += "      <table class=\"table table-sm table-striped table-hover\" id=\"" + key + "-table\">";
        table_markup += "        <thead>";
        table_markup += "          <tr>";
        object_keys.forEach ((object_key) => {
          table_markup += "            <th scope=\"col\"" + cols_class + ">" + object_key + "</th>";
        });
        table_markup += "          </tr>";
        table_markup += "        </thead>";
        table_markup += "        <tbody>";
        data_for_key.forEach((element) => {
          if (key === "LinkTable") {
            table_markup += "          <tr class=\"clickable\"" + getOnclickForInode(data["Name"], element["ParentDirInodeNumber"]) + ">";
          } else if (data["InodeType"] === "Dir" && key === "Payload") {
            table_markup += "          <tr class=\"clickable\"" + getOnclickForInode(data["Name"], element["InodeNumber"]) + ">";
          } else {
            table_markup += "          <tr>";
          }
          object_keys.forEach ((object_key) => {
            table_markup += "            <td><pre class=\"no-margin\">" + element[object_key] + "</pre></td>";
          });
          table_markup += "          </tr>";
        });
        table_markup += "        </tbody>";
        table_markup += "      </table>";
        return table_markup;
      }

      const addTablesForComplexData = function(data, complex_keys) {
        let complex_tables_markup = "";
        complex_keys.forEach((key) => {
          complex_tables_markup += getComplexDataTableMarkup(json_data, key);
        });
        document.getElementById("table-container").innerHTML = document.getElementById("table-container").innerHTML + complex_tables_markup;
      }

      // Create / fill tables
      const complex_keys = getListOfComplexKeys(json_data);
      document.getElementById("key-pair-data").innerHTML = getSimpleKeyValuePairsTableMarkupWithData(json_data, complex_keys);
      addTablesForComplexData(json_data, complex_keys);

      // Fancy back to top behavior
      addBackToTopBehavior();
    </script>
  </body>
</html>
`
