var JSONTree = (function() {

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
