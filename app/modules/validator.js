var validator = require('validator')
var cs = require('coinstring')
var bs58 = require('bs58')
var ecdsa = require('ecdsa')
var ecurve = require('ecurve')
var libphonenumber = require('google-libphonenumber')
var phoneUtils = libphonenumber.PhoneNumberUtil.getInstance()

var hash_array = [{
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

var assetid_prefixes = [{
  name: 'locked',
  value: 0xc8
}, {
  name: 'unlocked',
  value: 0x8e
}]

var enckey_prefixes = [{
  name: 'ec',
  value: 0x0142
}, {
  name: 'no-ec',
  value: 0x0143
}]

var extpubkey_prefixes = [{
  name: 'mainnet',
  value: 0x0488B21E
}, {
  name: 'testnet',
  value: 0x043587CF
}]

validator.isObject = function (array) {
  if (typeof array === 'string') {
    try {
      JSON.parse(array)
      return true
    } catch (e) {
      return false
    }
  }
  return typeof array === 'object'
}

validator.isString = function (str) {
  return typeof str === 'string'
}

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

validator.isPubKey = function (pubkey) {
  if (!Buffer.isBuffer(pubkey)) {
    try {
      if (pubkey.length === 66 || pubkey.length === 130) {
        pubkey = new Buffer(pubkey, 'hex')
      } else {
        return false
      }
    } catch (error) {
      return false
    }
  }
  try {
    ecurve.Point.decodeFrom(ecurve.getCurveByName('secp256k1'), pubkey)
  } catch (e) {
    return false
  }
  return true
}

validator.isExtendedPubKey = function (str) {
  try {
    var buf = bs58.decode(str)
    var new_buf = new Buffer(buf)
    var hex = new_buf.toString('hex')[0] + new_buf.toString('hex')[1] + new_buf.toString('hex')[2] + new_buf.toString('hex')[3] + new_buf.toString('hex')[4] + new_buf.toString('hex')[5] + new_buf.toString('hex')[6] + new_buf.toString('hex')[7]
    var prefix_int = parseInt(hex, 16)
    for (var i in extpubkey_prefixes) {
      if (prefix_int === extpubkey_prefixes[i].value) {
        return true
      }
    }
  } catch (e) {
    return false
  }
  return false
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

validator.isEncKey = function (str) {
  try {
    var buf = bs58.decode(str)
    var new_buf = new Buffer(buf)
    var prefix_int = parseInt(new_buf.toString('hex')[0] + new_buf.toString('hex')[1] + new_buf.toString('hex')[2] + new_buf.toString('hex')[3], 16)
    for (var i in enckey_prefixes) {
      if (prefix_int === enckey_prefixes[i].value) {
        return true
      }
    }
  } catch (e) {
    return false
  }
  return false
}

validator.isRandomCode = function (num) {
  return validator.isNumeric(num) && num.toString().split('').length === 4
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

validator.isDER = function (str) {
  try {
    ecdsa.parseSig(new Buffer(str, 'base64'))
  } catch (e) {
    return false
  }
  return true
}

validator.isPhone = function (str) {
  var num
  if (str[0] !== '+') str = '+' + str
  try {
    num = phoneUtils.parse(str)
    return !!num.getCountryCode()
  } catch (e) {
    return false
  }
}

validator.isEmailList = function (str) {
  var result = true
  try {
    var arr = str.split(',')
    if (arr.length <= 0) return false
    arr.forEach(function (email) {
      if (!validator.isEmail(email)) {
        result = false
      }
    })
    return result
  } catch (e) {
    return false
  }
}

module.exports = {
  height_or_hash: function () { return true },
  colored: function () { return true },
  txid: function () { return true },
  address: function () { return true },
  confirmations: function () { return true },
  assetId: function () { return true },
  utxo: function () { return true },
  sortBy: function () { return true },
  limit: function () { return true },
  start: function () { return true },
  end: function () { return true },
  interval: function () { return true },
  arg: function () { return true },
  index: function () { return true },
  addresses: function () { return true },
  utxos: function () { return true },
  txHex: function () { return true }
}
