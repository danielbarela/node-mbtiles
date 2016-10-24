var async = require('async');

//var sqljs = require('sql.js/js/sql-memory-growth');
var sqljs = require('sql.js/js/sql-debug');

module.exports.createAdapter = function(filePath, callback) {
  if (filePath) {
    var xhr = new XMLHttpRequest();
    xhr.open('GET', filePath, true);
    xhr.responseType = 'arraybuffer';

    xhr.onload = function(e) {
      if (xhr.status !== 200) {
        var db = new sqljs.Database();
        var adapter = new Adapter(db);
        return callback(null, adapter);
      }
      var uInt8Array = new Uint8Array(this.response);
      var db = new sqljs.Database(uInt8Array);
      var adapter = new Adapter(db);
      adapter.stat = {
        size: uInt8Array.length
      };
      callback(null, adapter);
    };
    xhr.onerror = function(e) {
      var db = new sqljs.Database();
      var adapter = new Adapter(db);
      return callback(null, adapter);
    };
    xhr.send();
  } else {
    var db = new sqljs.Database();
    var adapter = new Adapter(db);
    callback(null, adapter);
  }
}

module.exports.createAdapterFromDb = function(db) {
  return new Adapter(db);
}

function Adapter(db) {
  this.db = db;
}

Adapter.prototype.close = function() {
  this.db.close();
}

Adapter.prototype.getDBConnection = function () {
  return this.db;
};

Adapter.prototype.export = function(callback) {
  callback(null, this.db.export());
}

Adapter.prototype.get = function () {
  var callback = arguments[arguments.length-1];
  // if (typeof params === 'function') {
  //   callback = params;
  //   params = [];
  // }
  var row;

  try {
  var statement = this.db.prepare(arguments[0]);
  var params = [].slice.call(arguments, 1, arguments.length-1);
  statement.bind(params);

    var hasResult = statement.step();

    if (hasResult) {
      row = statement.getAsObject();
    }

    statement.free();
  } catch (e) { }
  callback(null, row);
};

Adapter.prototype.prepare = function () {
  var callback = typeof arguments[arguments.length - 1] === 'function' ? arguments[arguments.length - 1] : function() {}
  var statement;
  try {
    if (typeof arguments[arguments.length - 1] === 'function') {
      if (arguments.length > 2) {
        statement = this.db.prepare.apply(this.db, [arguments[0], [[].slice.call(arguments, 1, arguments.length -2)]]);
      } else {
        statement = this.db.prepare.apply(this.db, [arguments[0]]);
      }
    } else if (arguments.length > 1){
      statement = this.db.prepare.apply(this.db, [arguments[0], [[].slice.call(arguments, 1, arguments.length -1)]]);
    } else {
      statement = this.db.prepare.apply(this.db, arguments);
    }
  } catch (e) {
    return callback(e);
  }
  var origRun = statement.run;
  statement.run = function() {
    if (typeof arguments[arguments.length - 1] === 'function') {
      origRun.apply(this, [[].slice.call(arguments, 1, arguments.length - 1)], arguments[arguments.length - 1]);
    } else {
      origRun.apply(this, [[].slice.call(arguments, 1, arguments.length)]);
    }
  };

  statement.on = function() { }

  var origGet = statement.get;
  statement.get = function() {
    if (typeof arguments[arguments.length - 1] === 'function') {
      var results = origGet.apply(this, [[].slice.call(arguments, 0, arguments.length -1)]);
      if (!results.length) {
        return arguments[arguments.length - 1](null);
      }
      var columnNames = this.getColumnNames();
      var obj = {};
      for (var i = 0; i < columnNames.length; i++) {
        obj[columnNames[i]] = results[i];
      }
      arguments[arguments.length - 1](null, obj);
    } else {
      origGet.apply(this, [[].slice.call(arguments, 0, arguments.length)]);
    }
  }
  statement.finalize = function(cb) {
    if (cb) cb();
  };

  async.setImmediate(function() {
    callback();
  });

  return statement;
};

Adapter.prototype.all = function (sql, params, callback) {
  var rows = [];
  this.each(sql, params, function(err, row, rowDone) {
    rows.push(row);
    rowDone();
  }, function(err) {
    callback ? callback(err, rows) : params(err, rows);
  });
};

Adapter.prototype.each = function () {
  var rowCallback = function() {};
  var completeCallback = function() {};
  var sql = arguments[0];
  var params = [];
  if (typeof arguments[arguments.length-1] === 'function') {
    rowCallback = arguments[arguments.length-1];
    params = [].slice.call(arguments, 1, arguments.length - 1);
  }
  if (typeof arguments[arguments.length-2] === 'function') {
    completeCallback = rowCallback;
    rowCallback = arguments[arguments.length-2];
    params = [].slice.call(arguments, 1, arguments.length - 2);
  }

  try {
    var statement = this.db.prepare(sql);
    statement.bind(params);
    var count = 0;

    async.whilst(
      function() {
        return statement.step();
      },
      function(callback) {
        async.setImmediate(function() {
          var row = statement.getAsObject();
          count++;
          rowCallback(null, row, callback);
        });
      },
      function() {
        statement.free();
        completeCallback(null, count);
      }
    );
  } catch (e) {
    completeCallback();
  }
};

Adapter.prototype.exec = function(sql, callback) {
  this.run(sql, callback);
};

Adapter.prototype.serialize = function(callback) {
  callback();
};

Adapter.prototype.run = function(sql, params, callback) {
  if (callback) {
    this.db.run(sql, params);
    return callback();
  }
  if (params && typeof params === 'function') {
    this.db.run(sql);
    return params();
  }
  this.db.run(sql, params);
};

Adapter.prototype.insert = function(sql, params, callback) {
  try {
    var statement = this.db.prepare(sql, params);
    statement.step();
  } catch (e) {
    console.trace();
    return callback(e);
  }
  statement.free();
  var lastId = this.db.exec('select last_insert_rowid();');
  if (lastId) {
    return callback(null, lastId[0].values[0][0]);
  } else {
    return callback();
  }
};

Adapter.prototype.delete = function(sql, params, callback) {
  var rowsModified = 0;
  try {
    var statement = this.db.prepare(sql, params);
    statement.step();
    rowsModified = this.db.getRowsModified();
    statement.free();
  } catch (e) {
    console.trace();
    return callback(e);
  }
  return callback(null, rowsModified);
};

Adapter.prototype.dropTable = function(table, callback) {
  var response = this.db.exec('DROP TABLE IF EXISTS \'' + table + '\'');
  return callback(null, !!response);
};

Adapter.prototype.count = function (tableName, callback) {
  this.get('SELECT COUNT(*) as count FROM \'' + tableName + '\'', function(err, result) {
    callback(null, result.count);
  });
};
