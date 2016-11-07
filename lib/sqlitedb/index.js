
var SqliteDb = function(filePath, callback) {
  if (typeof(process) !== 'undefined' && process.version) {
    this.adapterCreator = require('./sqlite3Adapter');
  } else {
    this.adapterCreator = require('./sqljsAdapter');
  }

  if (typeof filePath === 'string') {
    this.adapterCreator.createAdapter(filePath, function(err, adapter) {
      this.adapter = adapter;
      this.stat = adapter ? adapter.stat : undefined;
      callback(err, this);
    }.bind(this));
  } else {
    this.adapterCreator.createAdapterFromBuffer(filePath, function(err, adapter) {
      this.adapter = adapter;
      this.stat = adapter ? adapter.stat : undefined;
      this.filePath = adapter.filePath;
      callback(err, this);
    }.bind(this));
  }
}

SqliteDb.prototype.close = function(callback) {
  return this.adapter.close(callback);
}

SqliteDb.prototype.export = function(callback) {
  this.adapter.export(callback);
}

SqliteDb.prototype.getDBConnection = function () {
  return this.adapter.db;
};

SqliteDb.prototype.setDBConnection = function (db) {
  this.adapter = this.adapterCreator.createAdapterFromDb(db);
};

SqliteDb.prototype.serialize = function(callback) {
  return this.adapter.serialize(callback);
};

SqliteDb.prototype.get = function () {
  return this.adapter.get.apply(this.adapter, arguments);
};

SqliteDb.prototype.prepare = function () {
  return this.adapter.prepare.apply(this.adapter, arguments);
};

SqliteDb.prototype.exec = function(sql, callback) {
  return this.adapter.exec(sql, callback);
}

SqliteDb.prototype.run = function () {
  return this.adapter.run.apply(this.adapter, arguments);
};

SqliteDb.prototype.all = function () {
  return this.adapter.all.apply(this.adapter, arguments);
};

SqliteDb.prototype.each = function () {
  return this.adapter.each.apply(this.adapter, arguments);
};

SqliteDb.prototype.minOfColumn = function(table, column, where, whereArgs, callback) {
  var minStatement = 'select min('+column+') as min from "' + table + '"';
  if(where) {
    minStatement += ' ';
    if (where.indexOf('where')) {
      where = 'where ' + where;
    }
    minStatement += where;
  }
  return this.adapter.get(minStatement, whereArgs, function(err, result) {
    if (err || !result) return callback(err);
    callback(err, result.min);
  });
};

SqliteDb.prototype.maxOfColumn = function(table, column, where, whereArgs, callback) {
  var maxStatement = 'select max('+column+') as max from "' + table + '"';
  if(where) {
    maxStatement += ' ';
    if (where.indexOf('where')) {
      where = 'where ' + where;
    }
    maxStatement += where;
  }
  return this.adapter.get(maxStatement, whereArgs, function(err, result) {
    if (err || !result) return callback(err);
    callback(err, result.max);
  });
};

SqliteDb.prototype.count = function(table, callback) {
  return this.adapter.count(table, callback);
};

SqliteDb.prototype.insert = function (sql, params, callback) {
  return this.adapter.insert(sql, params, callback);
};

SqliteDb.prototype.delete = function(tableName, where, whereArgs, callback) {
  var deleteStatement = 'DELETE FROM "' + tableName + '"';

  if (where) {
    deleteStatement += ' WHERE ' + where;
  }

  return this.adapter.delete(deleteStatement, whereArgs, callback);
};

SqliteDb.prototype.dropTable = function(tableName, callback) {
  return this.adapter.dropTable(tableName, callback);
};

SqliteDb.prototype.tableExists = function(tableName, callback) {
  return this.adapter.get('SELECT name FROM sqlite_master WHERE type="table" AND name=?', [tableName], callback);
};

module.exports = SqliteDb;

SqliteDb.connect = function(filePath, callback) {
  new SqliteDb(filePath, callback);
}

SqliteDb.connectWithDatabase = function(db, callback) {
  new SqliteDb(undefined, function(err, connection) {
    connection.setDBConnection(db);
    callback(err, connection);
  });
}

SqliteDb.connectWithDbBuffer = function(buffer, callback) {
  new SqliteDb(buffer, callback);
}
