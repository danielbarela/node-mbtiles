var fs = require('fs');
var util = require('util');
var MBTiles = require('..');
var tape = require('tape');
var queue = require('d3-queue').queue;
var crypto = require('crypto');

var expected = {
    bounds: '-141.005548666451,41.6690855919108,-52.615930948992,83.1161164353916',
    lat: 56.8354595949484,
    lon: -110.424643384994,
    name: 'Canada',
    population: 33487208,
    search: 'Canada, CA'
};

var tmp = require('os').tmpdir() + '/mbtiles-test-' + (+new Date).toString(16);
var tilesOnly;
var index;
var from;
var to;

if (typeof(process) !== 'undefined' && process.version) {
  try { fs.mkdirSync(tmp); } catch(err) { throw err; }
}

tape('setup', function(assert) {
    tilesOnly  = new MBTiles(tmp + '/tilesOnly.mbtiles', assert.end);
});
tape('setup', function(assert) {
    index = new MBTiles(__dirname + '/fixtures/geocoder_data.mbtiles', assert.end);
});
tape('setup', function(assert) {
    from = new MBTiles(__dirname + '/fixtures/geocoder_legacy.mbtiles', assert.end);
});
tape('setup', function(assert) {
    to = new MBTiles(tmp + '/indexed.mbtiles', assert.end);
});

tape('getGeocoderData', function(assert) {
    index.getGeocoderData('term', 0, function(err, buffer) {
        assert.ifError(err);
        assert.equal(3891, buffer.length);
        assert.end();
    });
});

tape('getGeocoderData (nodata)', function(assert) {
    index.getGeocoderData('term', 1e6, function(err, buffer) {
        assert.ifError(err);
        assert.equal(buffer, undefined);
        assert.end();
    });
});

tape('getGeocoderData (no table)', function(assert) {
    tilesOnly.getGeocoderData('term', 0, function(err, buffer) {
        assert.ifError(err);
        assert.equal(buffer, undefined);
        assert.end();
    });
});

tape('putGeocoderData', function(assert) {
    to.startWriting(function(err) {
        assert.ifError(err);
        to.putGeocoderData('term', 0, new Buffer('asdf'), function(err) {
            assert.ifError(err);
            to.stopWriting(function(err) {
                assert.ifError(err);
                to.getGeocoderData('term', 0, function(err, buffer) {
                    assert.ifError(err);
                    assert.deepEqual('asdf', buffer.toString());
                    assert.end();
                });
            });
        });
    });
});

tape('geocoderDataIterator', function(assert) {
    to.startWriting(function(err) {
        assert.ifError(err);
        // get a bunch of shards of different sizes and put them in in an arbitrary order
        var shardIds = {};
        var q = queue()
        while (true) {
            var id = Math.floor(Math.random() * Math.pow(2, 16));
            if (shardIds[id]) continue;

            shardIds[id] = 1;
            q.defer(function(id, cb) {
                to.putGeocoderData("term", id, [0x62, 0x75, 0x66, 0x66, 0x65, 0x72], cb);
            }, id);

            if (Object.keys(shardIds).length >= 50) break;
        }
        q.awaitAll(function() {
            to.stopWriting(function(err) {
                var it = to.geocoderDataIterator("term");
                var data = [];
                var n = function(err, item) {
                    assert.ifError(err);
                    if (item.done) {
                        assert.equal(data.length, 51, "iterator produces 51 shards");

                        assert.equal(data[0].shard, 0);
                        assert.equal(data[0].data.toString(), "asdf", "first shard data is preserved");

                        var sorted = true;
                        for (var i = 1; i < data.length; i++) {
                            if (data[i - 1].shard >= data[i].shard) sorted = false;
                        }
                        assert.equal(sorted, true, "shards come back in order");

                        assert.end();
                    } else {
                        data.push(item.value);
                        it.asyncNext(n);
                    }
                }
                it.asyncNext(n);
            });
        })
    });
})

tape('getIndexableDocs', function(assert) {
    from.getIndexableDocs({ limit: 10 }, function(err, docs, pointer) {
        assert.ifError(err);
        assert.equal(docs.length, 10);
        assert.deepEqual(pointer, { limit: 10, offset: 10, nogrids: false });
        assert.deepEqual(docs[0], {
            AREA: 0,
            FIPS: 'AA',
            ISO2: 'AW',
            ISO3: 'ABW',
            name: 'Aruba',
            POP2005: 102897,
            REGION: 19,
            SUBREGION: 29,
            UN: 533,
            _id: 4214083313,
            _text: 'Aruba',
            _zxy: [ '4/4/7' ],
            _center: [ -69.977, 12.517 ]
        });
        from.getIndexableDocs(pointer, function(err, docs, pointer) {
            assert.ifError(err);
            assert.equal(docs.length, 10);
            assert.deepEqual(pointer, { limit: 10, offset: 20, nogrids: false });
            assert.deepEqual(docs[0], {
                AREA: 20,
                FIPS: 'AQ',
                ISO2: 'AS',
                ISO3: 'ASM',
                name: 'American Samoa',
                POP2005: 64051,
                REGION: 9,
                SUBREGION: 61,
                UN: 16,
                _id: 2093723708,
                _text: 'American Samoa',
                _zxy: [ '4/0/8' ],
                _center: [ -170.73, -14.318 ]
            });
            assert.end();
        });
    });
});

tape('geocoderCentroid ABW', function(assert) {
    from.geocoderCentroid('ABW', ['4/4/7'], function(err, center) {
        assert.ifError(err);
        assert.equal(parseFloat(center[0].toFixed(10)), -70.3125);
        assert.equal(parseFloat(center[1].toFixed(10)), 12.5545635286);
        assert.end();
    });
});

tape('geocoderCentroid ASM', function(assert) {
    from.geocoderCentroid('ASM', ['4/0/8'], function(err, center) {
        assert.ifError(err);
        assert.equal(parseFloat(center[0].toFixed(10)), -170.859375);
        assert.equal(parseFloat(center[1].toFixed(10)), -14.2643830876);
        assert.end();
    });
});

tape('geocoderCentroid USA', function(assert) {
    from.geocoderCentroid('USA', [
        '4/0/7',
        '4/0/6',
        '4/0/5',
        '4/0/4',
        '4/0/3',
        '4/1/7',
        '4/1/4',
        '4/1/3',
        '4/2/6',
        '4/2/5',
        '4/2/4',
        '4/3/6',
        '4/3/5',
        '4/4/7',
        '4/4/6',
        '4/4/5',
        '4/5/5',
        '4/15/5'
    ], function(err, center) {
        assert.ifError(err);
        assert.equal(parseFloat(center[0].toFixed(10)), -118.828125);
        assert.equal(parseFloat(center[1].toFixed(10)), 46.0732306254);
        assert.end();
    });
});

tape('cleanup', function(assert) { tilesOnly.close(assert.end); });
tape('cleanup', function(assert) { index.close(assert.end); });
tape('cleanup', function(assert) { from.close(assert.end); });
tape('cleanup', function(assert) { to.close(assert.end); });
tape('cleanup', function(assert) {
    try { fs.unlinkSync(tmp + '/tilesOnly.mbtiles'); } catch(err) { throw err; }
    try { fs.unlinkSync(tmp + '/indexed.mbtiles'); } catch(err) { throw err; }
    try { fs.rmdirSync(tmp); } catch(err) { throw err; }
    assert.end();
});
