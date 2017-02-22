var mongoose = require('mongoose')
var async = require('async')
var path = require('path')
var fs = require('fs')
var pubsub

try {
  pubsub = require('fw_pubsub')
} catch (e) {
  console.warn('no pubsub', e)
}

var casimir = global.casimir
var server = casimir.server
var db = casimir.db
var properties = casimir.properties
var logger = casimir.logger

properties.scanner.scan = process.env.SCAN || properties.scanner && properties.scanner.scan
properties.scanner.mempool = process.env.MEMPOOL || properties.scanner && properties.scanner.mempool
properties.scanner.mempool_only = process.env.MEMPOOLONLY || properties.scanner && properties.scanner.mempool_only
properties.sockets.all_channels = process.env.ALLCHANNELS || properties.sockets.all_channels

properties.bus = properties.bus || {}
properties.bus.redis = process.env.BUS_REDIS || properties.bus.redis
properties.bus.channel = process.env.BUS_CHANNEL || properties.bus.channel
properties.bus.mongodb = process.env.BUS_MONGODB || properties.bus.mongodb
properties.bus.timer = parseInt(process.env.BUS_TIMER || properties.bus.timer || '0')
properties.bus.short_ttl = parseInt(process.env.BUS_SHORT_TTL || properties.bus.short_ttl || '604800000') // 1000 * 60 * 60 * 24 * 7 (week)
properties.bus.long_ttl = parseInt(process.env.BUS_LONG_TTL || properties.bus.long_ttl || '315360000000') // 1000 * 60 * 60 * 24 * 365 * 10 (10 years)
properties.bus.debug = properties.bus.debug || process.env.BUS_DEBUG || 'false'
properties.bus.debug = properties.bus.debug.toLowerCase() === 'true'
properties.bus.returnBuffers = properties.bus.returnBuffers || process.env.BUS_RETURN_BUFFER || 'false'
properties.bus.returnBuffers = properties.bus.returnBuffers.toLowerCase() === 'true'
properties.bus.subscribe = properties.bus.subscribe || process.env.BUS_SUBSCRIBE || 'false'
properties.bus.subscribe = properties.bus.subscribe.toLowerCase() === 'true'


var Sockets = require('./app/modules/sockets.js')
var Scanner = require('cc-block-parser')
var scanner

process.on('message', function (msg) {
  if (msg.last_block) {
    properties.last_block = msg.last_block
  }
  if (scanner && process.env.ROLE === properties.roles.API) {
    if (msg.newblock) {
      scanner.emit('newblock', msg.newblock)
    }
    if (msg.newtransaction) {
      scanner.emit('newtransaction', msg.newtransaction)
    }
    if (msg.newcctransaction) {
      scanner.emit('newcctransaction', msg.newcctransaction)
    }
    if (msg.revertedblock) {
      scanner.emit('revertedblock', msg.revertedblock)
    }
    if (msg.revertedtransaction) {
      scanner.emit('revertedtransaction', msg.revertedtransaction)
    }
    if (msg.revertedcctransaction) {
      scanner.emit('revertedcctransaction', msg.revertedcctransaction)
    }
    if (msg.mempool) {
      global.mempool = true
    }
  }
  if (scanner && process.env.ROLE === properties.roles.SCANNER) {
    if (msg.parse_priority) {
      console.log('priority_parse scanner got request '+ msg.parse_priority)
      scanner.priority_parse(msg.parse_priority, function (err) {
        console.time('priority_parse scanner_to_parent '+ msg.parse_priority)
        process.send({
          to: properties.roles.API,
          priority_parsed: msg.parse_priority,
          err: err
        })
        console.timeEnd('priority_parse scanner_to_parent '+ msg.parse_priority)
      })
    }
  }
})

function getApiVersions () {
  var versionFolders = []
  var routesPath = path.join(__dirname, '/routes')
  var files = fs.readdirSync(routesPath)
  files.forEach(function (file_name) {
    var file_path = path.join(routesPath, file_name)
    if (fs.lstatSync(file_path).isDirectory() && file_name[0] === 'v' && !isNaN(file_name.substring(1, file_name.length))) {
        versionFolders.push(file_name)
    }
  })
  return versionFolders
}

async.waterfall([
  function (callback) {
    db.init(properties.db, mongoose, callback)
  },
  function (mongoose, callback) {
    if (process.env.ROLE === properties.roles.API) {
      global.mempool = false
      server.http_server.listen(server.port, function () {
        logger.info('server started on port ' + server.port)
        callback(null, server.http_server)
      })
    } else {
      callback(null, null)
    }
  },
  function (app, callback) {
    var settings = {
      properties: properties,
      debug: properties.debug,
      next_hash: properties.next_hash,
      last_hash: properties.last_hash,
      last_block: properties.last_block,
      last_fully_parsed_block: properties.last_fully_parsed_block,
      rpc_settings: {
        host: process.env.BITCOINNETWORK || properties.bitcoin_rpc.url,
        port: process.env.BITCOINPORT || properties.bitcoin_rpc.port,
        user: process.env.RPCUSERNAME || properties.bitcoin_rpc.username,
        pass: process.env.RPCPASSWORD || properties.bitcoin_rpc.password,
        path: process.env.BITCOINPATH || properties.bitcoin_rpc.path || '',
        timeout: parseInt(process.env.BITCOINTIMEOUT || properties.bitcoin_rpc.timeout, 10)
      }
    }
    casimir.scanner = scanner = new Scanner(settings, mongoose)
    if (pubsub && properties.bus.redis && properties.bus.mongodb && properties.bus.channel && process.env.ROLE === properties.roles.SCANNER) {
      casimir.bus = new pubsub.PBus(properties.bus)
      casimir.bus.once('ready', function() {
        callback()
      })
      casimir.bus.on('error', function (err) {
        console.error('BUS_ERROR', err)
      })
      casimir.bus.create()
    } else {
      callback()
    }
  },
  function (callback) {
    var opts = {
      io: casimir.server.io_server,
      api_versions: getApiVersions(),
      scanner: scanner
    }
    if (casimir.bus) {
      opts.bus = casimir.bus
      opts.channel_prefix = properties.bus.channel
    }
    casimir.sockets = new Sockets(opts)
    if (properties.scanner.scan === 'true' && properties.scanner.mempool_only !== 'true') {
      if (process.env.ROLE === properties.roles.SCANNER) scanner.scan_blocks()
      if (process.env.ROLE === properties.roles.FIXER) scanner.fix_blocks()
      if (process.env.ROLE === properties.roles.CC_PARSER) scanner.parse_cc()
    }
    if (properties.scanner.mempool_only === 'true') {
      if (process.env.ROLE === properties.roles.SCANNER) scanner.scan_mempol_only()
    }
    callback(null, process.env.ROLE)
  }
], function (err, result) {
  if (err) {
    logger.info('Critical Error so killing server - ' + err)
    casimir.running = false
    return process.exit(1)
  }
  casimir.running = true
  logger.info('Finished Loading the Server, Last function passed - ' + result)
})
