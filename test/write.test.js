// require('sqlite3').verbose();

var fs = require('fs');
var tape = require('tape');
var MBTiles = require('..');
var async = require('async');
var fixtureDir = __dirname + '/fixtures/output';

if (typeof(process) !== 'undefined' && process.version) {
  // Recreate output directory to remove previous tests.
  try { fs.unlinkSync(fixtureDir + '/write_1.mbtiles'); } catch(err) {}
  try { fs.mkdirSync(fixtureDir, 0755); } catch(err) {}
}


function loadTile(tilePath, callback) {
  if (typeof(process) !== 'undefined' && process.version) {
    fs.readFile(tilePath, callback);
  } else {
    var xhr = new XMLHttpRequest();
    xhr.open('GET', tilePath, true);
    xhr.responseType = 'arraybuffer';

    xhr.onload = function(e) {
      if (xhr.status !== 200) {
        return callback();
      }
      return callback(null, new Buffer(this.response));
    };
    xhr.onerror = function(e) {
      return callback();
    };
    xhr.send();
  }
}

tape('test mbtiles file creation', function(assert) {
    var completed = { written: 0, read: 0 };
    new MBTiles(fixtureDir + '/write_1.mbtiles', function(err, mbtiles) {
        completed.open = true;
        if (err) throw err;

        mbtiles.startWriting(function(err) {
            completed.started = true;
            if (err) throw err;

            fs.readdirSync(__dirname + '/fixtures/images/').forEach(insertTile);
        });

        function insertTile(file) {
            var coords = file.match(/^plain_1_(\d+)_(\d+)_(\d+).png$/);
            if (!coords) return;

            // Flip Y coordinate because file names are TMS, but .putTile() expects XYZ.
            coords[2] = Math.pow(2, coords[3]) - 1 - coords[2];

            // fs.readFileSync(__dirname + '/fixtures/images/' + file);
            loadTile(__dirname + '/fixtures/images/' + file, function(err, tile) {
              mbtiles.putTile(coords[3] | 0, coords[1] | 0, coords[2] | 0, tile, function(err) {
                  if (err) throw err;
                  completed.written++;
                  if (completed.written === 285) {
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
          var images = fs.readdirSync(__dirname + '/fixtures/images/');
          async.eachSeries(images, function(file, callback) {
            var coords = file.match(/^plain_1_(\d+)_(\d+)_(\d+).png$/);
            if (coords) {
                // Flip Y coordinate because file names are TMS, but .getTile() expects XYZ.
                coords[2] = Math.pow(2, coords[3]) - 1 - coords[2];
                loadTile(__dirname + '/fixtures/images/' + file, function(err, expectedTile) {
                  mbtiles.getTile(coords[3] | 0, coords[1] | 0, coords[2] | 0, function(err, tile) {
                        assert.deepEqual(tile, expectedTile);
                        callback(err);
                    });
                });
            }
          }, function done() {
            assert.end();
          });
        }
    });
});
