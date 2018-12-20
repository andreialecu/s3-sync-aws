var LevelWriteStream = require('level-write-stream')
  , createQueue = require('queue-async')
  , backoff = require('backoff')
  , es = require('event-stream')
  , crypto = require('crypto')
  , xtend = require('xtend')
  , mime = require('mime')
  , once = require('once')
  , AWS = require('aws-sdk')
  , url = require('url')
  , fs = require('fs')

module.exports = s3syncer

function s3syncer(db, options) {
  if (!options) {
    options = db || {}
    db = false
  }

  options.concurrency = options.concurrency || 16
  options.headers = options.headers || {}
  options.cacheSrc = options.cacheSrc || __dirname + '/.sync'
  options.cacheDest = options.cacheDest || '/.sync'
  options.retries = options.retries || 7
  options.acl = options.acl || 'public-read'
  options.force = !!options.force
  options.accessKeyId = options.accessKeyId || options.key
  options.secretAccessKey = options.secretAccessKey || options.secret

  var client = new AWS.S3(options)
    , queue = createQueue(options.concurrency)
    , region = options.region === 'us-standard' ? false : options.region
    , secure = options.secure || !('secure' in options)
    , subdomain = region ? 's3-' + region : 's3'
    , protocol = secure ? 'https' : 'http'
    , prefix = options.prefix || ''
    , hashkey = options.hashKey || function(details) {
      return details.fullPath
    }

  var stream = es.map(function(data, next) {
    queue.defer(function(details, done) {
      details.fullPath = details.fullPath || details.src
      details.path = details.path || details.dest
      syncFile(details, function(err) {
        return err ? next(err) : done(), next(null, details)
      })
    }, data)
  })

  stream.getCache = getCache
  stream.putCache = putCache

  function syncFile(details, next) {
    var absolute = details.fullPath
      , relative = prefix + (details.path.charAt(0) === '/'
        ? details.path.slice(1)
        : details.path)

    relative = relative.replace(/\\/g, '/')

    var destination =
          protocol + '://'
        + subdomain
        + '.amazonaws.com/'
        + options.bucket
        + '/' + relative

    hashFile(absolute, destination, function(err, md5) {
      if (err) return next(err)
      details.md5 = md5
      details.url = destination
      details.fresh = false
      details.cached = false

      if (!db) return checkForUpload(next)

      var key = 'md5:' + hashkey(details)

      db.get(key, function(err, result) {
        if (!err && result === md5) {
          details.cached = true
          return next(null, details)
        }
        checkForUpload(function(err) {
          if (err) return next(err)
          db.put(key, md5, next)
        })
      })
    })

    function checkForUpload(next) {
      client.headObject({Bucket: options.bucket, Key: relative}, function(err, res) {
        if (err && err.statusCode !== 404) return next(err)
        if (err && err.statusCode === 404) return uploadFile(details, next)
        if (
          options.force || (
          res.Metadata['syncfilehash'] !== details.md5
        )) return uploadFile(details, next)
        return next(null, details)
      })
    }
  }

  function uploadFile(details, next) {
    var absolute = details.fullPath
      , relative = prefix + details.path
      , lasterr
      , off = backoff.fibonacci({
        initialDelay: 1000
      })

    relative = relative.replace(/\\/g, '/')
    details.fresh = true

    off.failAfter(options.retries)
    off.on('fail', function() {
      next(lasterr || new Error('unknown error'))
    }).on('ready', function() {
      var params = xtend({
        Bucket: options.bucket,
        Key: relative,
        ContentType: mime.lookup(absolute),
        ACL: options.acl,
        Metadata: {
          syncfilehash: details.md5
        },
        Body: fs.createReadStream(absolute)
      }, options.headers)

      client.putObject(params, function(err, res) {
        if (err) {
          err = new Error('Bad status code: ' + err.statusCode)
        } else {
          return next(null, details)
        }

        lasterr = err
        stream.emit('fail', err)
        off.backoff()
      })
    }).backoff()
  }

  function getCache(callback) {
    callback = once(callback)

    var readStream = client.getObject({
      Bucket: options.bucket,
      Key: options.cacheDest
    }).createReadStream();

    es.pipeline(
      readStream
      , es.split()
      , es.parse()
      , LevelWriteStream(db)()
    )
      .once('close', callback)
      .once('error', function(err) {
        if (err && err.statusCode === 404) {
          return callback(null)
        } else {
          return callback(err);
        }
      });
  }

  function putCache(callback) {
    callback = once(callback)

    db.createReadStream()
      .pipe(es.stringify())
      .pipe(fs.createWriteStream(options.cacheSrc))
      .once('error', callback)
      .once('close', function() {
        client.putObject({
          Bucket: options.bucket,
          Key: options.cacheDest,
          Body: fs.createReadStream(options.cacheSrc)
        }, function(err) {
          if (err) return callback(err)
          fs.unlink(options.cacheSrc, callback)
        })
     })
  }

  function hashFile(filename, destination, callback) {
    var hash = crypto.createHash('md5')
      , done = false

    hash.update(JSON.stringify([
        options.headers
      , destination
    ]))

    fs.createReadStream(filename).on('data', function(d) {
      hash.update(d)
    }).once('error', function(err) {
      if (!done) callback(err)
      done = true
    }).once('close', function() {
      if (!done) callback(null, hash.digest('hex'))
      done = true
    })
  }

  return stream
}
