var async = require('async')

var casimir = global.casimir
var server = casimir.server
var db = casimir.db
var properties = casimir.properties
var logger = casimir.logger

properties.scanner.scan = process.env.SCAN || properties.scanner && properties.scanner.scan
properties.scanner.mempool = process.env.MEMPOOL || properties.scanner && properties.scanner.mempool
properties.scanner.mempool_only = process.env.MEMPOOLONLY || properties.scanner && properties.scanner.mempool_only
properties.sockets.all_channels = process.env.ALLCHANNELS || properties.sockets.all_channels

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
})

async.waterfall([
  function (callback) {
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
      debug: false,
      properties: properties,
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
    casimir.scanner = scanner = new Scanner(settings, db)
    if (process.env.ROLE === properties.roles.API) casimir.sockets = new Sockets(casimir.server.io_server, scanner)
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
    logger.info('Critical Error so killing server - ', JSON.stringify(err))
    casimir.running = false
    return process.exit(1)
  }
  casimir.running = true
  logger.info('Finished Loading the Server, Last function passed - ' + result)
})
