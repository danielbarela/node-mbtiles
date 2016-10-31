// require('sqlite3').verbose();

var fs = require('fs');
var tape = require('tape');
var MBTiles = require('..');
var async = require('async');
var fixtureDir = __dirname + '/fixtures/output';

if (typeof(process) !== 'undefined' && process.version) {
// Recreate output directory to remove previous tests.
try { fs.unlinkSync(fixtureDir + '/write_2.mbtiles'); } catch(err) {}
try { fs.mkdirSync(fixtureDir, 0755); } catch(err) {}
}

function loadGrid(gridPath, callback) {
  if (typeof(process) !== 'undefined' && process.version) {
    fs.readFile(gridPath, 'utf8', function(err, data) {
      callback(err, JSON.parse(data));
    });
  } else {
    var xhr = new XMLHttpRequest();
    xhr.open('GET', gridPath, true);
    xhr.responseType = 'json';

    xhr.onload = function(e) {
      if (xhr.status !== 200) {
        return callback();
      }
      return callback(null, this.response);
    };
    xhr.onerror = function(e) {
      return callback();
    };
    xhr.send();
  }
}


tape('test mbtiles file creation', function(assert) {
    var completed = { written: 0, read: 0 };
    new MBTiles(fixtureDir + '/write_2.mbtiles', function(err, mbtiles) {
        completed.open = true;
        if (err) throw err;

        mbtiles.startWriting(function(err) {
            completed.started = true;
            if (err) throw err;

            fs.readdirSync(__dirname + '/fixtures/grids/').forEach(insertGrid);
        });

        function insertGrid(file) {
            var coords = file.match(/^plain_2_(\d+)_(\d+)_(\d+).json$/);
            if (!coords) return;

            // Flip Y coordinate because file names are TMS, but .putGrid() expects XYZ.
            coords[2] = Math.pow(2, coords[3]) - 1 - coords[2];

            loadGrid(__dirname + '/fixtures/grids/' + file, function(err, grid) {
              mbtiles.putGrid(coords[3] | 0, coords[1] | 0, coords[2] | 0, grid, function(err) {
                  if (err) throw err;
                  completed.written++;
                  if (completed.written === 241) {
                      mbtiles.stopWriting(function(err) {
                          completed.stopped = true;
                          if (err) throw err;
                          verifyWritten();
                      });
                  }
              });
            });
        }

        function verifyWritten() {
          var grids = fs.readdirSync(__dirname + '/fixtures/grids/');
          async.eachSeries(grids, function(file, callback) {
            var coords = file.match(/^plain_2_(\d+)_(\d+)_(\d+).json$/);
            if (coords) {
                // Flip Y coordinate because file names are TMS, but .getTile() expects XYZ.
                coords[2] = Math.pow(2, coords[3]) - 1 - coords[2];
                loadGrid(__dirname + '/fixtures/grids/' + file, function(err, expectedGrid) {
                  mbtiles.getGrid(coords[3] | 0, coords[1] | 0, coords[2] | 0, function(err, grid) {
                      assert.deepEqual(grid, expectedGrid);
                      completed.read++;
                      callback(err);
                  });
                });
            }
          }, function done() {
            assert.equal(completed.read, 241);
            assert.end();
          });
        }
    });
});
