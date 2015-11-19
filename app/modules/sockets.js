var casimir = global.casimir
var logger = casimir.logger

var channels = ['newblock', 'newtransaction', 'newcctransaction', 'revertedblock', 'revertedtransaction', 'revertedcctransaction']

var Sockets = function (io, scanner) {
  var self = this

  self.io = io
  self.events = io.of('/events')
  self.scanner = scanner
  self.on_address_sockets = {}
  self.on_trasaction_socket = {}
  self.open_socket()
  self.open_channels()
}

Sockets.prototype.open_channels = function () {
  var self = this
  if (self.scanner) {
    channels.forEach(function (channel) {
      self.scanner.on(channel, function (data) {
        var msg = {}
        msg[channel] = data
        if (casimir.properties.sockets && casimir.properties.sockets.all_channels === 'true') {
          self.io.emit(channel, msg)
        }
        self.events.to(channel).emit(channel, msg)
      })
    })
  }
  self.scanner.on('newtransaction', function (transaction) {
    var msg = {
      txid: transaction.txid,
      transaction: transaction
    }
    self.events.to('transaction/' + transaction.txid).emit('transaction', msg)
    var assets = []
    var addresses = []
    // logger.debug('txid:', transaction.txid)
    if (transaction.vin) {
      transaction.vin.forEach(function (vin) {
        if (vin.previousOutput && vin.previousOutput.addresses) {
          vin.previousOutput.addresses.forEach(function (address) {
            if (addresses.indexOf(address) === -1) {
              // logger.debug('input address:', address)
              addresses.push(address)
            }
          })
        }
        if (vin.assets) {
          vin.assets.forEach(function (asset) {
            if (assets.indexOf(asset.assetId) === -1) {
              // logger.debug('input asset:', asset.assetId)
              assets.push(asset.assetId)
            }
          })
        }
      })
    }
    if (transaction.vout) {
      transaction.vout.forEach(function (vout) {
        if (vout.scriptPubKey && vout.scriptPubKey.addresses) {
          vout.scriptPubKey.addresses.forEach(function (address) {
            if (addresses.indexOf(address) === -1) {
              // logger.debug('output address:', address)
              addresses.push(address)
            }
          })
        }
        if (vout.assets) {
          vout.assets.forEach(function (asset) {
            if (assets.indexOf(asset.assetId) === -1) {
              // logger.debug('output asset:', asset.assetId)
              assets.push(asset.assetId)
            }
          })
        }
      })
    }
    addresses.forEach(function (address) {
      // logger.debug('emiting to:', 'address/'+address)
      var msg = {
        address: address,
        transaction: transaction
      }
      self.events.to('address/' + address).emit('transaction', msg)
    })

    assets.forEach(function (assetId) {
      // logger.debug('emiting to:', 'asset/'+assetId)
      var msg = {
        assetId: assetId,
        transaction: transaction
      }
      self.events.to('asset/' + assetId).emit('transaction', msg)
    })
  })
}

Sockets.prototype.open_socket = function () {
  var self = this
  self.events.on('connection', function (socket) {
    logger.info('socketid', socket.id, 'connected.')
    socket.on('join', function (rooms) {
      if (!Array.isArray(rooms)) {
        rooms = [rooms]
      }
      rooms.forEach(function (room) {
        socket.join(room)
        logger.info('socket ' + socket.id + ' joined room ' + room + '.')
      })
    })

    socket.on('leave', function (rooms) {
      if (!Array.isArray(rooms)) {
        rooms = [rooms]
      }
      rooms.forEach(function (room) {
        socket.leave(room)
        logger.info('socket ' + socket.id + ' left room ' + room + '.')
      })
    })

    socket.on('disconnect', function () {
      logger.info('socketid', socket.id, 'disconnected.')
    })
  })
}

module.exports = Sockets
