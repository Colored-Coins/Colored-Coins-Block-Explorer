var casimir = global.casimir
var properties = casimir.properties
var db = casimir.db
var _ = require('lodash')

var Sequelize = db.Sequelize
var sequelize = db.sequelize
var Blocks = db.blocks
var Transactions = db.transactions
var Outputs = db.outputs
var Inputs = db.inputs
var AddressesOutputs = db.addressesoutputs
var AddressesTransactions = db.addressestransactions
var AssetsOutputs = db.assetsoutputs
var AssetsTransactions = db.assetstransactions
var AssetsAddresses = db.assetsaddresses
var Assets = db.assets

var inputAttributes = {exclude: ['output_id', 'input_txid', 'input_index']}
var outputAttributes = {exclude: ['id', 'txid']}

var squel = require('squel').useFlavour('postgres')

var MAX_BLOCKS_ALLOWED = 50

var get_transaction = function (req, res, next) {
  var params = req.data
  var txid = params.txid

  find_transaction(txid, function (err, tx) {
    if (err) return next(err)
    return res.send(tx)
  })
}

var get_block = function (req, res, next) {
  var params = req.data
  var height_or_hash = params.height_or_hash

  find_block(height_or_hash, false, function (err, block) {
    if (err) return next(err)
    return res.send(block)
  })
}

var get_block_with_transactions = function (req, res, next) {
  var params = req.data
  var height_or_hash = params.height_or_hash

  find_block(height_or_hash, true, function (err, block) {
    if (err) return next(err)
    return res.send(block)
  }) 
}

var get_address_info = function (req, res, next) {
  var params = req.data
  var address = params.address
  var confirmations = params.confirmations || 0
  confirmations = parseInt(confirmations, 10)

  find_address_info(address, false, confirmations, function (err, info) {
    if (err) return next(err)
    if ('transactions' in info) delete info['transactions']
    if ('utxos' in info) delete info['utxos']
    return res.send(info)
  })
}

var get_address_info_with_transactions = function (req, res, next) {
  var params = req.data
  var address = params.address
  var confirmations = params.confirmations || 0
  confirmations = parseInt(confirmations, 10)

  find_address_info(address, true, confirmations, function (err, info) {
    if (err) return next(err)
    return res.send(info)
  })
}

var get_addresses_utxos = function (req, res, next) {
  var params = req.data
  var addresses = params.addresses
  var confirmations = params.confirmations || 0
  confirmations = parseInt(confirmations, 10)

  find_addresses_utxos(addresses, confirmations, function (err, utxos) {
    if (err) return next(err)
    return res.send(utxos)
  })
}

var get_address_utxos = function (req, res, next) {
  var params = req.data
  var address = params.address
  var confirmations = params.confirmations || 0
  confirmations = parseInt(confirmations, 10)

  find_address_utxos(address, confirmations, function (err, info) {
    if (err) return next(err)
    return res.send(info)
  })
}

var get_blocks = function (req, res, next) {
  var params = req.data
  var start = params.start
  var end = params.end
  find_blocks(start, end, function (err, blocks) {
    if (err) return next(err)
    return res.send(blocks)
  })
}

var get_utxo = function (req, res, next) {
  var params = req.data
  var txid = params.txid
  var index = params.index
  
  find_utxo(txid, index, function (err, utxo) {
    if (err) return next(err)
    utxo = utxo || null
    res.send(utxo)
  })
}

var get_utxos = function (req, res, next) {
  var params = req.data
  var utxos = params.utxos

  find_utxos(utxos, function (err, ans) {
    if (err) return next(err)
    res.send(ans)
  })
}

var find_transaction = function (txid, callback) {
  Transactions.findById(txid, {
    attributes: { exclude: ['index_in_block'] },
    include: [
      { model: Inputs, as: 'vin', attributes: inputAttributes, include: [
        { model: Outputs, as: 'previousOutput', attributes: outputAttributes }
      ]},
      { model: Outputs, as: 'vout', attributes: outputAttributes }
    ],
    order: [
      [{model: Inputs, as: 'vin'}, 'input_index', 'ASC'],
      [{model: Outputs, as: 'vout'}, 'n', 'ASC']
    ]
  }).then(function(transaction) {
    callback(null, transaction)
  });
}

var find_block = function (height_or_hash, with_transactions, callback) {
  var where
  if (typeof height_or_hash === 'string' && height_or_hash.length > 10) {
    where = {
      hash: height_or_hash
    }
  } else {
    var height = parseInt(height_or_hash, 10)
    if (height) {
      where = {
        height: height
      }
    } else {
      return callback()
    }
  }
  var include = [
    {
      model: Transactions,
      as: 'transactions'
    }
  ]
  var order = [
    [{model: Transactions, as: 'transactions'}, 'index_in_block', 'ASC']
  ]
  if (!with_transactions) {
    include[0].attributes = ['txid']
  } else {
    include[0].attributes = {exclude: ['index_in_block']}
    include[0].include = [
      { model: Inputs, as: 'vin', attributes: inputAttributes, include: [
        { model: Outputs, as: 'previousOutput', attributes: outputAttributes }
      ]},
      { model: Outputs, as: 'vout', attributes: outputAttributes }
    ]
    order.push([{model: Transactions, as: 'transactions'}, {model: Inputs, as: 'vin'}, 'input_index', 'ASC'])
    order.push([{model: Transactions, as: 'transactions'}, {model: Outputs, as: 'vout'}, 'n', 'ASC'])
  }
  Blocks.find({ where: where, include: include, order: order })
    .then(function (block) {
      var block = block.toJSON()
      block.tx = _(block.transactions).map('txid').value()
      if (!with_transactions) {
        delete block.transactions
      }
      callback(null, block)
    })
}

var find_address_info = function (address, with_transactions, confirmations, callback) {
  var ans = {
    address: address
  }
  var transactions = ans.transactions = []
  var utxos = ans.utxos = []
  var assets = assets = {}
  ans.balance = 0
  ans.received = 0

  var where = {
    address: address, 
  }
  var include = [
    {
      model: Transactions,
      attributes: { exclude: ['index_in_block'] },
      as: 'transaction',
      where: !confirmations ? null : {
        blockheight: { $gte: 0, $lte: properties.last_block - confirmations + 1 }
      },
      order: [{model: Outputs, as: 'vout'}, 'n', 'ASC']
    }
  ]

  if (!with_transactions) {
    include[0].include = [
      {
        model: Outputs,
        attributes: {exclude: ['id']},
        as: 'vout'
      } // TODO Oded - include assets
    ]
  } else {
    include[0].include = [
      { model: Inputs, as: 'vin', attributes: inputAttributes, include: [
        { model: Outputs, as: 'previousOutput', attributes: outputAttributes }
      ]},
      { model: Outputs, as: 'vout', attributes: outputAttributes }
    ]
    include[0].order.push([{model: Inputs, as: 'vin'}, 'input_index', 'ASC'])
  }

  AddressesTransactions.findAll({ where: where, include: include })
    .then(function (address_transactions) {
      var txs = address_transactions.map(function (address_transaction) { return address_transaction.toJSON().transaction })
      txs.forEach(function (tx) {
        if ('vout' in tx && tx.vout) {
          tx.vout.forEach(function (vout) {
            if ('scriptPubKey' in vout && vout.scriptPubKey) {
              if ('addresses' in vout.scriptPubKey && vout.scriptPubKey.addresses && vout.scriptPubKey.addresses.indexOf(address) !== -1) {
                ans.received += vout.value
                if (!vout.used) {
                  vout.blockheight = tx.blockheight
                  vout.blocktime = tx.blocktime
                  utxos.push(vout)
                }
                if (vout.assets && vout.assets.length) {
                  vout.assets.forEach(function (asset) {
                    var assetId = asset.assetId
                    assets[assetId] = assets[assetId] || {
                      assetId: assetId,
                      balance: 0,
                      received: 0,
                      divisibility: asset.divisibility,
                      lockStatus: asset.lockStatus
                    }
                    assets[assetId].received += asset.amount
                  })
                }
              }
            }
          })
        }
      })

      utxos.forEach(function (utxo) {
        ans.balance += utxo.value
        utxo.assets = utxo.assets || []
        utxo.assets.forEach(function (asset) {
          var assetId = asset.assetId
          assets[assetId] = assets[assetId] || {
            assetId: assetId,
            balance: 0,
            received: 0,
            divisibility: asset.divisibility,
            lockStatus: asset.lockStatus
          }
          assets[assetId].balance += asset.amount
        })
      })

      ans.transactions = txs
      ans.assets = []
      for (var assetId in assets) {
        ans.assets.push(assets[assetId])
      }
      ans.numOfTransactions = ans.transactions.length
      callback(null, ans)
    })
    .catch(callback)
}

var find_addresses_utxos = function (addresses, confirmations, callback) {
  var ans = []

  var where = { address: {$in: addresses} }
  var attributes = ['address']
  var include = [{
    model: Outputs,
    as: 'output',
    attributes: { exclude: ['n', 'id'], include: [['n', 'index']]},
    where: { used: false },
    include: [{
      model: Transactions,
      as: 'transaction',
      attributes: ['blockheight', 'blocktime'],
      where: !confirmations ? null : {
        blockheight: { $gte: 0, $lte: properties.last_block - confirmations + 1 }
      }
    }]
  }]

  AddressesOutputs.findAll({ where: where, attributes: attributes, include: include, raw: true })
    .then(function (utxos) {
      _(utxos)
        .groupBy('address')
        .mapKeys(function (utxos, address) { ans.push({ address: address, utxos: format_utxos(utxos) }) })
        .value()
      callback(null, ans)
  })
}

var find_address_utxos = function (address, confirmations, callback) {
  var ans = {
    address: address
  }
  ans.utxos = []

  var where = { address: address }
  var attributes = ['address']
  var include = [{
    model: Outputs,
    as: 'output',
    attributes: { exclude: ['n', 'id'], include: [['n', 'index']]},
    where: { used: false },
    include: [{
      model: Transactions,
      as: 'transaction',
      attributes: ['blockheight', 'blocktime'],
      where: !confirmations ? null : {
        blockheight: { $gte: 0, $lte: properties.last_block - confirmations + 1 }
      }
    }]
  }]

  AddressesOutputs.findAll({ where: where, attributes: attributes, include: include, raw: true })
    .then(function (utxos) {
      ans.utxos = format_utxos(utxos)
      callback(null, ans)
  })
}

var find_blocks = function (start, end, callback) {
  var ans = []
  start = start || 0
  end = end || 0

  start = parseInt(start, 10)
  end = parseInt(end, 10)

  if (typeof start !== 'number' || typeof end !== 'number') {
    return callback('Arguments must be numbers.')
  }
  var conditions = {}
  var include = [
    {
      model: Transactions,
      as: 'transactions',
      attributes: ['txid']
    }
  ]
  var order = [
    ['height', 'DESC'],
    [{model: Transactions, as: 'transactions'}, 'index_in_block', 'ASC']
  ]
  var limit = MAX_BLOCKS_ALLOWED
  if (start < 0 && !end) {
    limit = -start
    if (limit > MAX_BLOCKS_ALLOWED) {
      return callback('Can\'t query more then ' + MAX_BLOCKS_ALLOWED + ' blocks.')
    }
    conditions = {
      ccparsed: true
    }
  } else {
    if (end - start + 1 > MAX_BLOCKS_ALLOWED) {
      return callback('Can\'t query more then ' + MAX_BLOCKS_ALLOWED + ' blocks.')
    }
    conditions = {
      height: {$gte: start, $lte: end},
      ccparsed: true
    }
  }
  Blocks.findAll({ where: conditions, include: include, order: order, limit: limit })
    .then(function (blocks) {
      blocks.forEach(function (block, i) {
        blocks[i] = block.toJSON()
        blocks[i].tx = _(blocks[i].transactions).map('txid').value()
        delete blocks[i].transactions
      })
      // var last_height = -1
      // blocks.forEach(function (block) {
      //   var height = block.height
      //   if (last_height !== height) {
      //     ans.push(block)
      //     ans[ans.length - 1].tx = [block['transactions.txid']]
      //     ans[ans.length - 1].confirmations = properties.last_block - height + 1  // if performing raw query - need to calculate confirmations manually
      //     delete ans.transactions
      //   } else {
      //     ans[ans.length - 1].tx.push(block['transactions.txid'])
      //   }
      //   last_height = height
      // })
      callback(null, blocks)
    })
    .catch(callback)
}

var find_utxo = function (txid, index, callback) {
  var where = {
    txid: txid,
    n: index
  }
  var attributes = {
    exclude: ['n', 'id'],
    include: [['n', 'index']]
  }
  var include = [{
    model: Transactions,
    as: 'transaction',
    attributes: ['blockheight', 'blocktime']
  }]
  Outputs.findOne({ where: where, attributes: attributes, include: include, raw: true })
    .then(function (utxo) {
      callback(null, format_utxo(utxo))
    })
    .catch(callback)
}

var find_utxos = function (utxos, callback) {
  if (!utxos || !utxos.length) return callback(null, [])
  var or = utxos.map(function (utxo) {
    return {txid: utxo.txid, n: utxo.index}
  })
  var where = {
    $or: or
  }
  var attributes = {
    exclude: ['n', 'id'],
    include: [['n', 'index']]    
  }
  var include = [{
    model: Transactions,
    as: 'transaction',
    attributes: ['blockheight', 'blocktime']
  }]
  Outputs.findAll({ where: where, attributes: attributes, include: include, raw: true})
    .then(function (utxos) {
      callback(null, format_utxos(utxos))
    })
    .catch(callback)
}

var format_utxos = function (utxos) {
  return utxos.map(format_utxo)
}

var format_utxo = function (utxo) {
  var currUtxo = {}
  var key
  var trimmedKey
  for (key in utxo) {
    trimmedKey = key.substring(key.lastIndexOf('.') + 1) 
    if (trimmedKey === 'address') continue
    currUtxo[trimmedKey] = utxo[key]
  }
  currUtxo.assets = currUtxo.assets || []
  return currUtxo
}

module.exports = {
  get_transaction: get_transaction,
  get_block: get_block,
  get_block_with_transactions: get_block_with_transactions,
  get_blocks: get_blocks,
  get_address_utxos: get_address_utxos,
  get_addresses_utxos: get_addresses_utxos,
  get_utxo: get_utxo,
  get_utxos: get_utxos,
  get_address_info: get_address_info,
  get_address_info_with_transactions: get_address_info_with_transactions
}
