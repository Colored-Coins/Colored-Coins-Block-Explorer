var casimir = global.casimir
var properties = casimir.properties
var logger = casimir.logger

var channels = ['newblock', 'newtransaction', 'newcctransaction', 'revertedblock', 'revertedtransaction', 'revertedcctransaction']

var Sockets = function (opt) {
  var self = this

  self.io = opt.io
  if (opt.bus) {
    self.bus = opt.bus
    self.channel_prefix = opt.channel_prefix
    var index = self.channel_prefix.indexOf('.*')
    if (~index) {
      self.channel_prefix = self.channel_prefix.substring(0, index)
    }
  }

  var apiVersions = opt.api_versions
  self.events = [self.io.of('/events')]
  self.events = self.events.concat(apiVersions.map(version => self.io.of('/' + version + '/events')))
  self.scanner = opt.scanner
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
          if (process.env.ROLE === properties.roles.API) {

            self.io.local.emit(channel, msg)
          } else if (self.bus) {
            self.bus.push({message: msg}, self.channel_prefix + '.' + channel, properties.bus.short_ttl)
          }
        }
        if (process.env.ROLE === properties.roles.API) {
          self.events.forEach(event => event.to(channel).local.emit(channel, msg)) // if the scanner is paralelize then the local flag should be false
        } else {
          if (self.bus) {
            if (channel === 'newcctransaction') {
              self.bus.push({message: msg}, self.channel_prefix + '.' + channel, properties.bus.long_ttl)
            } else {
              self.bus.push({message: msg}, self.channel_prefix + '.' + channel, properties.bus.short_ttl)
            }
          }
        }
      })
    })
  }
  self.scanner.on('newtransaction', function (transaction) {
    var msg = {
      txid: transaction.txid,
      transaction: transaction
    }
    if (process.env.ROLE === properties.roles.API) {
      self.events.forEach(event => event.to('transaction/' + transaction.txid).local.emit('transaction', msg))
    } else if (self.bus) {
      self.bus.push({message: msg}, self.channel_prefix + '.transaction.' + transaction.txid, properties.bus.short_ttl)
    }
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
      if (process.env.ROLE === properties.roles.API) {
        self.events.forEach(event => event.to('address/' + address).local.emit('transaction', msg))
      } else if (self.bus) {
        self.bus.push({message: msg}, self.channel_prefix + '.address.' + address, properties.bus.short_ttl)
      }
    })

    assets.forEach(function (assetId) {
      // logger.debug('emiting to:', 'asset/'+assetId)
      var msg = {
        assetId: assetId,
        transaction: transaction
      }
      if (process.env.ROLE === properties.roles.API) {
        self.events.forEach(event => event.to('asset/' + assetId).local.emit('transaction', msg))
      } else if (self.bus) {
        self.bus.push({message: msg}, self.channel_prefix + '.asset.' + assetId, properties.bus.short_ttl)
      }
    })
  })
}

Sockets.prototype.open_socket = function () {
  var self = this
  self.events.forEach(event => event.on('connection', function (socket) {
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
  }))
}

module.exports = Sockets
