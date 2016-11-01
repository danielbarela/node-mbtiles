var fs = require('fs');

module.exports.createAdapter = function(filePath, callback) {
  var sqlite3 = require('sqlite3').verbose();
  filePath = filePath || "";
  var db = new sqlite3.Database(filePath, function(err) {
    if (err) {
      console.log('cannot open ' + filePath);
      return callback(err);
    }
    var adapter = new Adapter(db);
    adapter.filePath = filePath;
    callback(err, adapter);
  });
}

module.exports.createAdapterFromDb = function(db) {
  return new Adapter(db);
}

function Adapter(db) {
  this.db = db;
}

Adapter.prototype.close = function(callback) {
  return this.db.close(callback);
}

Adapter.prototype.export = function(callback) {
  return fs.readFile(this.filePath, callback);
}

Adapter.prototype.getDBConnection = function () {
  return this.db;
};

Adapter.prototype.serialize = function(callback) {
  return this.db.serialize(callback);
};

Adapter.prototype.get = function () {
  return this.db.get.apply(this.db, arguments);
};

Adapter.prototype.prepare = function () {
  return this.db.prepare.apply(this.db, arguments);
};

Adapter.prototype.all = function () {
  return this.db.all.apply(this.db, arguments);
};

Adapter.prototype.exec = function(sql, callback) {
  return this.db.exec.apply(this.db, arguments);
};

Adapter.prototype.run = function() {
  return this.db.run.apply(this.db, arguments);
};

Adapter.prototype.insert = function(sql, params, callback) {
  return this.db.run(sql, params, function(err) {
    if(err) return callback(err);
    return callback(err, this.lastID);
  });
};

Adapter.prototype.delete = function(sql, params, callback) {
  return this.db.run(sql, params, function(err) {
    callback(err, this.changes);
  });
};

Adapter.prototype.each = function () {
  return this.db.each.apply(this.db, arguments);
};

Adapter.prototype.dropTable = function(table, callback) {
  return this.db.run('DROP TABLE IF EXISTS "' + table + '"', function(err) {
    if(err) return callback(err);
    return callback(err, !!this.changes);
  });
};

Adapter.prototype.count = function (tableName, callback) {
  return this.get('SELECT COUNT(*) as count FROM "' + tableName + '"', function(err, result) {
    callback(err, result.count);
  });
};
