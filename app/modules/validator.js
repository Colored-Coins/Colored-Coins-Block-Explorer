var expressValidator = require('express-validator')
var validator = require('validator')
var cs = require('coinstring')
var bs58 = require('bs58')
var ecdsa = require('ecdsa')
var ecurve = require('ecurve')
var libphonenumber = require('google-libphonenumber')
var phoneUtils = libphonenumber.PhoneNumberUtil.getInstance()

var hash_array = [
  {
    name: 'bitcoin',
    value: 0x00
  }, {
    name: 'bitcoin_script_hash',
    value: 0x05
  }, {
    name: 'testnet',
    value: 0x6F
  }, {
    name: 'testnet_script_hash',
    value: 0xC4
  }, {
    name: 'dogecoin',
    value: 0x1E
  }, {
    name: 'litecoin',
    value: 0x30
  }, {
    name: 'namecoin',
    value: 0x34
  }
]

var assetid_prefixes = [
  {
    name: 'unlocked',
    value: 0x2e
  }, {
    name: 'locked1',
    value: 0x20
  }, {
    name: 'locked2',
    value: 0x21
  }
]

validator.isCryptoAddress = function (address) {
  var result = false
  if (typeof address === 'string') address = [address]
  address.forEach(function (singleAddress) {
    for (var hash in hash_array) {
      if (cs.isValid(singleAddress, hash_array[hash].value)) {
        result = true
      }
    }
  })
  return result
}

validator.isCryptoAddresses = function (addresses) {
  if (!Array.isArray(addresses)) return false
  for (var i = 0; i < addresses.length; i++) {
    if (!validator.isCryptoAddress(addresses[i])) {
      return false
    }
  }
  return true
}

validator.isHexInLength = function (str, num) {
  if (str.length === num && validator.isHexadecimal(str)) {
    return true
  }
  return false
}

validator.isHexInLength32 = function (str) {
  return validator.isHexInLength(str, 32)
}

validator.isHexInLength64 = function (str) {
  return validator.isHexInLength(str, 64)
}

validator.isAssetId = function (str) {
  if (typeof str === 'string') {
    str = [str]
  }
  var ans = true
  str.forEach(function (assetId) {
    var is_valid = false
    assetid_prefixes.forEach(function (assetid_prefix) {
      if (is_valid || cs.isValid(assetId, assetid_prefix.value)) {
        is_valid = true
      }
    })
    if (!is_valid) {
      ans = false
    }
  })
  return ans
}

validator.isUtxo = function (utxo) {
  if (typeof utxo !== 'string') return false
  var utxoParts = utxo.split(':')
  if (utxoParts.length !== 2) return false
  return validateUtxoParts(utxoParts)
}

var validateUtxoParts = function (utxoParts) {
  return validator.isHexInLength64(utxoParts[0]) && validator.isNumeric(utxoParts[1])
}

validator.isUtxos = function (utxos) {
  if (!Array.isArray(utxos)) return false
  for (var i = 0; i < utxos.length; i++) {
    if (typeof utxos[i].txid === 'undefined' || typeof utxos[i].index === 'undefined' || !validateUtxoParts([utxos[i].txid, utxos[i].index])) {
      return false
    }
  }
  return true
}

var isV0 = function (url) {
  return !url.match(/\/v\d\//)
}

module.exports = function (req, res, next) {
  if (isV0(req.originalUrl)) {
    return next()
  }

  return expressValidator({
    customValidators: {
      height_or_hash: function (height_or_hash) {
        return validator.isHexInLength64(height_or_hash) || validator.isNumeric(height_or_hash)
      },
      colored: validator.isBoolean,
      txid: validator.isHexInLength64,
      address: validator.isCryptoAddress,
      confirmations: validator.isNumeric,
      assetId: validator.isAssetId,
      utxo: validator.isUtxo,
      limit: validator.isNumeric,
      start: validator.isNumeric,
      end: validator.isNumeric,
      interval: validator.isNumeric,
      arg: function () { return true },
      index: validator.isNumeric,
      addresses: validator.isCryptoAddresses,
      utxos: validator.isUtxos
    }
  })(req, res, next)
}
