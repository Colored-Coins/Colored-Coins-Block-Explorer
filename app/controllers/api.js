var bitcoin = require('bitcoinjs-lib')
var async = require('async')
var moment = require('moment')
var Cache = require('ttl')
var errors = require('cc-errors')
var randomstring = require("randomstring")
var _ = require('lodash')
var binarySearch = require('binary-search')

var casimir = global.casimir
var properties = casimir.properties
var db = casimir.db
var logger = casimir.logger
var scanner = casimir.scanner
var cache = new Cache({
  ttl: 10 * 1000, // 10 Seconds
  capacity: 300
})

var Blocks = db.get_model('Blocks')
var RawTransactions = db.get_model('RawTransactions')
var Utxos = db.get_model('Utxo')
var AddressesTransactions = db.get_model('AddressesTransactions')
var AddressesUtxos = db.get_model('AddressesUtxos')
var AssetsTransactions = db.get_model('AssetsTransactions')
var AssetsUtxos = db.get_model('AssetsUtxos')
var AssetsAddresses = db.get_model('AssetsAddresses')
var AddressesBalances = db.get_model('AddressesBalances')

var MAX_BLOCKS_ALLOWED = 50

var no_id = {
  _id: 0
}

properties.last_block = properties.last_block || 0
var debug = properties.debug

var find_block = function (height_or_hash, callback) {
  var conditions
  if (typeof height_or_hash === 'string' && height_or_hash.length > 10) {
    conditions = {
      hash: height_or_hash
    }
  } else {
    var height = parseInt(height_or_hash, 10)
    if (height) {
      conditions = {
        height: height,
        ccparsed: true
      }
    } else {
      return callback()
    }
  }
  conditions.ccparsed = true
  return Blocks.findOne(conditions, no_id).lean().exec(callback)
}

var add_used_txid = function (tx, callback) {

  if (!tx || !tx.vout) return callback(null, tx)
  tx.ccdata = tx.ccdata || []
  var outputs = tx.vout.map(function (output) { return { txid: tx.txid, index: output.n}})
  find_utxos(outputs, function (err, utxos) {
    if (err) return callback (err)
    if (utxos.length !== outputs.length) return callback('cant find all transaction ' + tx.txid + ' outputs')
    utxos.forEach(function (utxo) {
      var output = tx.vout[utxo.index]
      if (!output) return
      output.assets = output.assets || []
      output.used = utxo.used
      output.blockheight = utxo.blockheight
      output.usedBlockheight = utxo.usedBlockheight
      output.usedTxid = utxo.usedTxid
    })
    callback(null, tx)
  })
  
  // if (!tx || !tx.vout) return callback(null, tx)
  // tx.ccdata = tx.ccdata || []
  // async.eachLimit(tx.vout, 100, function (vout, cb) {
  //   vout.assets = vout.assets || []
  //   find_utxo(tx.txid, vout.n, function (err, utxo) {
  //     if (err) return cb(err)
  //     if (!utxo) return cb('cant find transaction: ' + tx.txid + ' output: ' + vout.n)
  //     vout.used = utxo.used
  //     vout.blockheight = utxo.blockheight
  //     vout.usedBlockheight = utxo.usedBlockheight
  //     vout.usedTxid = utxo.usedTxid
  //     cb()
  //   })
  // },
  // function (err) {
  //   if (err) return callback(err)
  //   callback(null, tx)
  // })
}

var find_transaction = function (txid, callback) {
  if (typeof txid === 'string') {
    var conditions = {
      txid: txid
    }
  } else {
    return callback()
  }
  return RawTransactions.findOne(conditions, no_id).lean().exec(function (err, tx) {
    if (err) return callback(err)
    add_used_txid(tx, callback)
  })
}

var find_transactions = function (txids, colored, callback) {
  if (!Array.isArray(txids)) return callback(null, [])
  var conditions = {
    txid: { $in: txids }
  }
  if (colored) {
    conditions.colored = true
  }
  return RawTransactions.find(conditions, no_id).sort('time').lean().exec(function (err, txs) {
    if (err) return callback(err)
    async.map(txs, function (tx, cb) {
      add_used_txid(tx, cb)
    },
    callback)
  })
}

var find_addresses_info = function (addresses, confirmations, callback) {
  var ans = []
  // logger.debug('addresses', JSON.stringify(addresses))
  async.each(addresses, function (address, cb) {
    // logger.debug('address', address)
    find_address_info(address, confirmations, function (err, address_info) {
      if (err) return cb(err)
      ans.push(address_info)
      cb()
    })
  },
  function (err) {
    // logger.debug('err', err)
    // logger.debug('ans', ans)
    return callback(err, ans)
  })
}

var find_address_info = function (address, confirmations, callback) {
  var rand = randomstring.generate()
  var limit = 100
  var ans = {
    address: address
  }
  var transactions = ans.transactions = []
  var utxos = ans.utxos = []
  var assets = assets = {}
  ans.balance = 0
  ans.received = 0
  if (debug) console.time(rand + ':find_address_info')
  async.waterfall([
    function (cb) {
      if (debug) console.time(rand + ':AddressesTransactions.find')
      AddressesTransactions.find({address: address}, no_id).lean().exec(cb)
    },
    function (address_txids, cb) {
      if (debug) {
        console.timeEnd(rand + ':AddressesTransactions.find')
        console.time(rand + ':get_txs')
      }
      var txids = address_txids.map(function (address_txid) {
        return address_txid.txid
      })
      // console.log('txids', txids)
      var txs = []
      async.whilst(function () { return txids.length > 0 }, function (cb) {
        var bulk_txids = txids.splice(0, limit)
        // console.log('bulk_txids', bulk_txids)
        var conditions = {
          txid: {$in: bulk_txids}
        }
        if (confirmations) {
          conditions.blockheight = {
            $lte: properties.last_block - confirmations + 1,
            $gte: 0
          }
        }
        RawTransactions.find(conditions, no_id).lean().exec(function (err, bulk_txs) {
          if (err) return cb(err)
          txs = txs.concat(bulk_txs)
          // console.log('txs', txs.map(function (tx) { return tx.txid }))
          cb()
        })
      }, 
      function (err) {
        cb(err, txs)
      })
    },
    function (txs, cb) {
      if (debug) {
        console.timeEnd(rand + ':get_txs')
        console.time(rand + ':process_txs')
      }
      txs.forEach(function (tx) {
        if ('vout' in tx && tx.vout) {
          tx.vout.forEach(function (vout) {
            if ('scriptPubKey' in vout && vout.scriptPubKey) {
              if ('addresses' in vout.scriptPubKey && vout.scriptPubKey.addresses && vout.scriptPubKey.addresses.indexOf(address) !== -1) {
                ans.received += vout.value
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

        if (transactions.indexOf(tx) === -1) {
          transactions.push(tx)
        }
      })
      return cb()
    },
    function (cb) {
      if (debug) {
        console.timeEnd(rand + ':process_txs')
        console.time(rand + ':AddressesUtxos.find')
      }
      AddressesUtxos.find({address: address}, no_id).lean().exec(cb)
    },
    function (address_utxos, cb) {
      if (debug) {
        console.timeEnd(rand + ':AddressesUtxos.find')
        console.time(rand + ':get_utxos')
      }
      if (!address_utxos.length) return cb(null, [])
      var conditions = []
      address_utxos.forEach(function (address_utxo) {
        var cond = {
          txid: address_utxo.utxo.split(':')[0],
          index: address_utxo.utxo.split(':')[1],
          used: false
        }
        if (confirmations) {
          cond.blockheight = {
            $lte: properties.last_block - confirmations + 1,
            $gte: 0
          }
        }
        conditions.push(cond)
      })
      Utxos.find({$or: conditions}, no_id).lean().exec(cb)
    },
    function (unspents, cb) {
      if (debug) {
        console.timeEnd(rand + ':get_utxos')
        console.time(rand + ':process_utxos')
      }
      unspents.forEach(function (tx) {
        ans.balance += tx.value
        tx.assets = tx.assets || []
        tx.assets.forEach(function (asset) {
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
        if (utxos.indexOf(tx) === -1) {
          utxos.push(tx)
        }
      })
      async.mapLimit(transactions, 100, function (tx, cb) {
        add_used_txid(tx, cb)
      },
      cb)
    }
  ],
  function (err, txs) {
    if (err) return callback(err)
    if (debug) {
      console.timeEnd(rand + ':process_utxos')
      console.timeEnd(rand + ':find_address_info')
    }
    ans.transactions = txs
    ans.assets = []
    for (var assetId in assets) {
      ans.assets.push(assets[assetId])
    }
    ans.numOfTransactions = ans.transactions.length
    return callback(err, ans)
  })
}

var find_addresses_utxos = function (addresses, confirmations, callback) {
  var ans = []
  async.each(addresses, function (address, cb) {
    find_address_utxos(address, confirmations, function (err, utxos) {
      if (err) return cb(err)
      // ans.push({address: address, utxos: utxos}) //TODO: change to this in V2
      ans.push(utxos)
      cb()
    })
  },
  function (err) {
    return callback(err, ans)
  })
}

var find_address_utxos = function (address, confirmations, callback) {
  var ans = {
    address: address
  }
  var utxos = ans.utxos = []

  async.waterfall([
     function (cb) {
      AddressesUtxos.find({address: address}, no_id).lean().exec(cb)
    },
    function (address_utxos, cb) {
      if (!address_utxos.length) return cb(null, [])
      var conditions = []
      address_utxos.forEach(function (address_utxo) {
        var cond = {
          txid: address_utxo.utxo.split(':')[0],
          index: address_utxo.utxo.split(':')[1],
          used: false
        }
        if (confirmations) {
          cond.blockheight = {
            $lte: properties.last_block - confirmations + 1,
            $gte: 0
          }
        }
        conditions.push(cond)
      })
      Utxos.find({$or: conditions}, no_id).lean().exec(cb)
    },
    function (unspents, cb) {
      unspents.forEach(function (tx) {
        tx.assets = tx.assets || []
        if (utxos.indexOf(tx) === -1) {
          utxos.push(tx)
        }
      })
      return cb()
    }
  ],
  function (err) {
    return callback(err, ans)
  })
}

var count_asset_transactions = function (assetId, type, callback) {
  var conditions = {assetId: assetId}
  if (type) {
    conditions.type = type
  }
  AssetsTransactions.count(conditions).lean().exec(callback)  
}

var find_asset_transactions = function (assetId, confirmations, type, callback) {
  if (typeof type === 'function') {
    callback = type
    type = null
  }
  var transactions = []

  async.waterfall([
     function (cb) {
      var conditions = {assetId: assetId}
      if (type) {
        conditions.type = type
      }
      AssetsTransactions.find(conditions, no_id).lean().exec(cb)
    },
    function (assets_transactions, cb) {
      if (!assets_transactions.length) return cb(null, [])
      var conditions = []
      assets_transactions.forEach(function (address_txids) {
        var cond = {
          txid: address_txids.txid
        }
        if (confirmations) {
          cond.blockheight = {
            $lte: properties.last_block - confirmations + 1,
            $gte: 0
          }
        }
        conditions.push(cond)
      })
      RawTransactions.find({$or: conditions}, no_id).lean().exec(cb)
    },
    function (txs, cb) {
      txs.forEach(function (tx) {
        if (transactions.indexOf(tx) === -1) {
          transactions.push(tx)
        }
      })
      return cb()
    },
    function (cb) {
      async.map(transactions, function (tx, cb) {
        add_used_txid(tx, cb)
      },
      cb)
    }
  ],
  function (err, transactions) {
    if (err) return callback(err)
    return callback(null, transactions)
  })
}

var find_asset_first_block = function (assetId, callback) {
  var conditions = {assetId: assetId}
  conditions.type = 'issuance'
  AssetsTransactions.find(conditions).lean().exec(function (err, assets_transactions) {
    if (err) return callback(err)
    var txids = []
    assets_transactions.forEach(function (asset_transaction) {
      txids.push(asset_transaction.txid)
    })
    RawTransactions.find({txid: {$in: txids}, blockheight: {$gte: 0}}, {blockheight:1, _id:0}).sort({blockheight: 1}).limit(1).lean().exec(function (err, transactions) {
      if (err) return callback(err)
      var blockheight = transactions.length? transactions[0].blockheight : -1
      callback(null, blockheight)
    })
  })
}

var find_asset_utxos = function (assetId, confirmations, callback) {
  var ans = {
    assetId: assetId
  }
  var utxos = ans.utxos = []

  async.waterfall([
     function (cb) {
      AssetsUtxos.find({assetId: assetId}, no_id).lean().exec(cb)
    },
    function (assets_utxos, cb) {
      if (!assets_utxos.length) return cb(null, [])
      var conditions = []
      assets_utxos.forEach(function (address_utxo) {
        var cond = {
          txid: address_utxo.utxo.split(':')[0],
          index: address_utxo.utxo.split(':')[1],
          used: false
        }
        if (confirmations) {
          cond.blockheight = {
            $lte: properties.last_block - confirmations + 1,
            $gte: 0
          }
        }
        conditions.push(cond)
      })
      Utxos.find({$or: conditions}, no_id).lean().exec(cb)
    },
    function (unspents, cb) {
      unspents.forEach(function (tx) {
        tx.assets = tx.assets || []
        if (utxos.indexOf(tx) === -1) {
          utxos.push(tx)
        }
      })
      return cb()
    }
  ],
  function (err) {
    return callback(err, ans)
  })
}

var find_blocks = function (start, end, callback) {
  start = start || 0
  end = end || 0

  start = parseInt(start, 10)
  end = parseInt(end, 10)

  if (typeof start !== 'number' || typeof end !== 'number') {
    return callback('Arguments must be numbers.')
  }
  var conditions = {}
  var limit = MAX_BLOCKS_ALLOWED
  if (start < 0 && !end) {
    limit = -start
    if (limit > MAX_BLOCKS_ALLOWED) {
      return callback(new errors.BlocksRangeTooHighError({
        explanation: 'Can\'t query more than ' + MAX_BLOCKS_ALLOWED + ' blocks.'
      }))
    }
    conditions = {
      ccparsed: true
    }
  } else {
    if (end - start + 1 > MAX_BLOCKS_ALLOWED) {
      return callback(new errors.BlocksRangeTooHighError({
        explanation: 'Can\'t query more than ' + MAX_BLOCKS_ALLOWED + ' blocks.'
      }))
    }
    conditions = {
      height: {$gte: start, $lte: end},
      ccparsed: true
    }
  }
  Blocks.find(conditions, no_id).lean().sort('-height').limit(limit).exec(callback)
}

var find_asset_holders = function (assetId, confirmations, callback) {
  var holders = {}
  var divisibility
  var lockStatus
  var some_utxo
  var aggregationPolicy
  find_asset_utxos(assetId, confirmations, function (err, asset_utxos) {
    if (err) return callback(err)
    if (asset_utxos.utxos) {
      asset_utxos.utxos.forEach(function (utxo) {
        if (utxo.assets) {
          utxo.assets.forEach(function (asset) {
            if (asset.assetId === assetId && asset.amount) {
              if (!some_utxo) {
                some_utxo = utxo.txid + ':' + utxo.index
              }
              divisibility = asset.divisibility
              lockStatus = asset.lockStatus
              aggregationPolicy = asset.aggregationPolicy
              if (utxo.scriptPubKey && utxo.scriptPubKey.addresses) {
                utxo.scriptPubKey.addresses.forEach(function (address) {
                  holders[address] = holders[address] || 0
                  holders[address] += asset.amount
                })
              }
            }
          })
        }
      })
    }
    var ans = {
      assetId: assetId,
      holders: [],
      divisibility: divisibility,
      lockStatus: lockStatus,
      aggregationPolicy: aggregationPolicy,
      someUtxo: some_utxo
    }
    for (var address in holders) {
      ans.holders.push({
        address: address,
        amount: holders[address]
      })
    }
    callback(null, ans)
  })
}

var find_asset_info = function (assetId, with_transactions, callback) {
  if (typeof with_transactions == 'function') {
    callback = with_transactions
    with_transactions = false
  }

  var functions = {
    holders: function (cb) {
      find_asset_holders(assetId, 0, cb)
    },
    issuances: function (cb) {
      find_asset_transactions(assetId, 0, 'issuance', cb)
    },
    burns: function (cb) {
      find_asset_transactions(assetId, 0, 'burn', cb)
    }
  }
  if (!with_transactions) {
    functions.numOfTransfers = function (cb) {
      count_asset_transactions(assetId, 'transfer', cb)
    }
    functions.firstBlock = function (cb) {
      find_asset_first_block(assetId, cb)
    }

  } else {
    functions.transfers = function (cb) {
      find_asset_transactions(assetId, 0, 'transfer', cb)
    }
  }

  async.parallel(functions,
  function (err, results) {
    if (err) return callback(err)

    var asset_info = results.holders
    asset_info.numOfHolders = asset_info.holders.length
    var totalIssued = _.sumBy(results.issuances, find_issuance_amount)
    var totalBurned = _.sumBy(results.burns, function (burn) { return find_burn_amounts(burn)[assetId] || 0 }) || 0
    asset_info.totalSupply = totalIssued - totalBurned

    if (!with_transactions) {
      asset_info.numOfTransfers = results.numOfTransfers
      asset_info.numOfIssuance = results.issuances.length
      asset_info.numOfBurns = results.burns.length
      asset_info.firstBlock = results.firstBlock

    } else {
      asset_info.transfers = results.transfers
      asset_info.issuances = results.issuances
      asset_info.burns = results.burns
      asset_info.numOfTransfers = asset_info.transfers.length
      asset_info.numOfIssuances = asset_info.issuances.length
      asset_info.numOfBurns = asset_info.burns.length
      asset_info.issuances.forEach(function (transaction) {
        if (!asset_info.firstBlock || asset_info.firstBlock === -1 || (transaction.blockheight !== -1 && asset_info.firstBlock > transaction.blockheight)) {
          asset_info.firstBlock = transaction.blockheight
        }
      })
    }

    return callback(null, asset_info)
  })
}

var find_issuance_amount = function (issuanceTx) {
  return _.sumBy(issuanceTx.ccdata, function (cc) {
    return cc.amount || 0
  })
}

// returns an array of objects, where each object consists of assetId and its respective burn amount
var find_burn_amounts = function (burnTx) {
  // difference between inputs assets and outputs assets
  var assets = {}
  burnTx.vin.forEach(function (input) {
    input.assets.forEach(function (asset) {
      assets[asset.assetId] = assets[asset.assetId] || 0
      assets[asset.assetId] += asset.amount
    })
  })
  burnTx.vout.forEach(function (output) {
    output.assets.forEach(function (asset) {
      assets[asset.assetId] -= asset.amount
      if (!assets[asset.assetId]) delete assets[asset.assetId]
    })
  })
  return assets
}

var find_block_with_transactions = function (height_or_hash, colored, callback) {
  find_block(height_or_hash, function (err, block) {
    if (err) return callback(err)
    if (block && block.tx) {
      find_transactions(block.tx, colored, function (err, transactions) {
        if (err) return callback(err)
        block.transactions = transactions
        return callback(null, block)
      })
    } else {
      return callback(null, block)
    }
  })
}

var find_first_issuance = function (assetId, utxo, callback) {
  logger.debug(JSON.stringify(utxo.split(':')))
  var conditions = {
    txid: utxo.split(':')[0],
    index: utxo.split(':')[1]
  }
  Utxos.findOne(conditions, no_id).lean().exec(function (err, utxo_obj) {
    if (err) return callback(err)
    if (!utxo_obj) return callback()
    var found = false
    utxo_obj.assets = utxo_obj.assets || []
    utxo_obj.assets.forEach(function (asset) {
      if (!found && asset.assetId === assetId) {
        found = true
        return callback(null, asset.issueTxid)
      }
    })
    if (!found) {
      return callback()
    }
  })
}

var find_main_stats = function (callback) {
  var main_stats = {}
  async.parallel([
    function (cb) {
      RawTransactions.count({colored: true}, (err, numOfCCTransactions) => {
        main_stats.numOfCCTransactions = numOfCCTransactions
        cb()
      })
    },
    function (cb) {
      AssetsTransactions.distinct('assetId').exec(function (err, assets) {
        if (err) return cb(err)
        main_stats.numOfAssets = assets.length
        cb()
      })
    },
    function (cb) {
      AssetsAddresses.distinct('address').exec(function (err, holders) {
        if (err) return cb(err)
        main_stats.numOfHolders = holders.length
        cb()
      })
    }
  ],
  function (err) {
    return callback(err, main_stats)
  })
}

var find_transactions_by_intervals = function (assetId, start, end, interval, callback) {
  end = end || moment.utc().millisecond(999).seconds(59).minutes(59).hours(23).valueOf() + 1
  start = start || end - 10 * 24 * 60 * 60 * 1000
  interval = interval || (end - start) / 10

  start = Math.round(start)
  end = Math.round(end)
  interval = Math.round(interval)
  var numOfIntervals = Math.round((end - start) / interval)  
  if (numOfIntervals > 1000) return callback(new errors.ResolutionTooHighError())

  async.waterfall([
    function (cb) {
      if (!assetId) return cb(null, null)
      AssetsTransactions.distinct('txid', {assetId: assetId}).exec(cb)
    },
    function (txids, cb) {
      var conditions = {
        colored: true,
        blocktime: {
          $gte: start,
          $lt: end
        }
      }
      if (txids) {
        conditions.txid = {$in: txids}
      }
      RawTransactions.aggregate()
        .match(conditions)
        .project({
          indexInInterval: {
            $floor: {
              $divide: [
                {
                  $subtract: ['$blocktime', start]
                },
                interval
              ]
            }
          }
        })
        .group({
          _id: '$indexInInterval',
          txsSum: {$sum: 1}
        })
        .sort({_id: 1})
        .exec(cb)
    }
  ],
  function (err, groups) {
    if (err) return callback(err)
    var intervals = []
    _.times(numOfIntervals, (i) => {
      var group
      var group_index = binarySearch(groups, i, (a, b) => {return a._id - b})
      if (group_index >= 0) {
        group = groups[group_index]
      }
      intervals.push({
        from: start + i * interval,
        untill: start + (i + 1) * interval,
        txsSum: (group && group.txsSum) || 0 
      })
    })
    callback(null, intervals)
  })
}

var find_cc_transactions = function (skip, limit, callback) {
  limit = limit || 10
  limit = parseInt(limit, 10)
  limit = Math.min(limit, 5000)

  skip = skip || 0
  skip = parseInt(skip, 10)

  var txs

  async.waterfall([
    function (cb) {
      var conditions = {
        colored: true,
        ccparsed: true,
        blockheight: -1
      }
      RawTransactions.find(conditions, no_id).lean().sort('-blocktime').limit(limit).skip(skip).exec(cb)
    },
    function (mempool_txs, cb) {
      txs = mempool_txs
      if (txs.length) {
        limit -= txs.length
        skip = 0
        return cb()
      }
      var conditions = {
        colored: true,
        ccparsed: true,
        blockheight: -1
      }
      RawTransactions.find(conditions).count().exec(function (err, count) {
        if (err) return cb(err)
        skip -= count
        skip = Math.max(skip, 0)
        cb()
      })
    },
    function (cb) {
      if (!limit) return cb()
       var conditions = {
        colored: true,
        ccparsed: true,
        blockheight: {$gte: 0}
      }
      RawTransactions.find(conditions, no_id).lean().sort('-blockheight').limit(limit).skip(skip).exec(function (err, conf_txs) {
        if (err) return cb(err)
        txs = txs.concat(conf_txs)
        cb()
      })
    }
  ], function (err) {
    if (err) return callback(err)
    callback(null, txs)
  })
}

var find_popular_assets = function (sort_by, limit, callback) {
  var collection
  if (sort_by === 'transactions') {
    collection = AssetsTransactions
  } else if (sort_by === 'holders') {
    collection = AssetsAddresses
  } else {
    collection = AssetsTransactions
  }
  collection.aggregate([
    {
      $group: {
        _id: {assetId: '$assetId'},
        count: { $sum: 1 }
      }
    }
  ]).sort('-count').limit(limit).exec(function (err, asset_counts) {
    if (err) return callback(err)
    var assets = []
    var assetsOrder = {}
    var i = 0
    asset_counts.forEach(function (asset_count) {
      assetsOrder[asset_count._id.assetId] = i++
    })
    async.each(asset_counts, function (asset_count, cb) {
      find_asset_info(asset_count._id.assetId, function (err, info) {
        if (err) return cb(err)
        if ('holders' in info) delete info['holders']
        assets[assetsOrder[asset_count._id.assetId]] = info
        cb()
      })
    },
    function (err) {
      callback(err, assets)
    })
  })
}

var find_utxo = function (txid, index, callback) {
  Utxos.findOne({txid: txid, index: index}, no_id).lean().exec(function (err, utxo) {
    if (err) return callback(err)
    if (utxo) utxo.assets = utxo.assets || []
    callback(null, utxo)
  })
}

var find_utxos = function (utxos, callback) {
  if (!utxos || !utxos.length) return callback(null, [])
  var or = utxos.map(function (utxo) {
    return {txid: utxo.txid, index: utxo.index}
  })
  Utxos.find({$or: or}, no_id).lean().exec(function (err, txos) {
    if (err) return callback(err)
    txos.forEach(function (utxo) {
      utxo.assets = utxo.assets || []
    })
    callback(null, txos)
  })
}

var is_asset = function (assetId, callback) {
  AssetsTransactions.findOne({assetId: assetId}, function (err, asset_transaction) {
    if (err) return callback(err)
    return callback(null, !!asset_transaction)
  })
}

var find_mempool_txids = function (colored, callback) {
  var conditions = {
    blockheight: -1
  }
  if (colored) {
    conditions.colored = true
  }
  var projection = {txid: 1, _id: 0}
  RawTransactions.find(conditions, projection).lean().exec(function (err, txs) {
    if (err) return callback(err)
    return callback(null, txs.map(function (tx) {
      return tx.txid
    }))
  })
}

var find_info = function (callback) {
  scanner.get_info(function (err, info) {
    if (err) return callback(err)
    delete info.balance
    info.mempool = global.mempool
    find_last_blocks(function (err, blocks) {
      if (err) return callback(err)
      info.parsedblocks = blocks.last_parsed_block
      info.fixedblocks = blocks.last_fixed_block
      info.ccparsedblocks = blocks.last_cc_parsed_block
      info.timeStamp = new Date()
      callback(null, info)
    })
  })
}

var find_last_parsed_block = function (callback) {
  var conditions = {
    txinserted: true
  }
  Blocks.findOne(conditions, no_id)
  .lean()
  .sort('-height')
  .exec(function (err, block_data) {
    if (err) return callback(err)
    if (!block_data) return callback(null, -1)
    callback(null, block_data.height)
  })
}

var find_last_fixed_block = function (callback) {
  var conditions = {
    txinserted: true,
    txsparsed: true
  }
  Blocks.findOne(conditions, no_id)
  .lean()
  .sort('-height')
  .exec(function (err, block_data) {
    if (err) return callback(err)
    if (!block_data) return callback(null, -1)
    callback(null, block_data.height)
  })
}

var find_last_cc_parsed_block = function (callback) {
  var conditions = {
    txinserted: true,
    txsparsed: true,
    ccparsed: true
  }
  Blocks.findOne(conditions, no_id)
  .lean()
  .sort('-height')
  .exec(function (err, block_data) {
    if (err) return callback(err)
    if (!block_data) return callback(null, -1)
    callback(null, block_data.height)
  })
}

var find_last_blocks = function (callback) {
  async.parallel({
    last_parsed_block: find_last_parsed_block,
    last_fixed_block: find_last_fixed_block,
    last_cc_parsed_block: find_last_cc_parsed_block
  },
  callback)
}

// ---------------------------

var get_info = function (req, res, next) {
  find_info(function (err, info) {
    if (err) return next(err)
    res.send(info)
  })
}

var get_mempool_txids = function (req, res, next) {
  var params = req.data
  var colored = params.colored

  find_mempool_txids(colored, function (err, txids) {
    if (err) return next(err)
    res.send(txids)
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

var parse_tx = function (req, res, next) {
  var params = req.data
  var txid = params.txid || ''
  var callback
  if (debug) console.time('parse_tx: full_parse ' + txid)
  callback = function (data) {
    if (data.priority_parsed === txid) {
      if (debug) console.timeEnd('parse_tx: full_parse ' + txid)
      process.removeListener('message', callback)
      if (data.err) return next(data.err)
      res.send({txid: txid})
    }
  }

  process.on('message', callback)

  logger.debug('priority_parse: api_to_parent ' + txid)
  process.send({to: properties.roles.SCANNER, parse_priority: txid})
}

var get_popular_assets = function (req, res, next) {
  var cache_key = req.originalUrl
  var ttl = 12 * 60 * 60 * 1000 // 12 hours
  var assets = cache.get(cache_key)

  if (assets) {
    res.send(assets)
  } else {
    var params = req.data
    var sort_by = params.sortBy
    var limit = params.limit || 10
    limit = parseInt(limit, 10)
    limit = Math.min(limit, 100)

    find_popular_assets(sort_by, limit, function (err, assets) {
      if (err) return next(err)
      cache.put(cache_key, assets, ttl)
      res.send(assets)
    })
  }
}

var get_cc_transactions = function (req, res, next) {
  var params = req.data
  var limit = params.limit || 10
  limit = parseInt(limit, 10)
  limit = Math.min(limit, 100)
  var skip = params.skip || 0
  skip = parseInt(skip, 10)

  find_cc_transactions(skip, limit, function (err, transactions) {
    if (err) return next(err)
    res.send(transactions)
  })
}

var get_transactions_by_intervals = function (req, res, next) {
  var cache_key = req.originalUrl
  var ttl = 12 * 60 * 60 * 1000 // 12 hours
  var intervals = cache.get(cache_key)

  if (intervals) {
    res.send(intervals)
  } else {
    var params = req.data
    var start = params.start
    var end = params.end
    var interval = params.interval
    var assetId = params.assetId

    try {
      start = parseInt(start, 10)
      end = parseInt(end, 10)
      interval = parseInt(interval, 10)
    } catch (e) {
      return next(e)
    }

    find_transactions_by_intervals(assetId, start, end, interval, function (err, intervals) {
      if (err) return next(err)
      cache.put(cache_key, intervals, ttl)
      return res.send(intervals)
    })
  }
}

var get_main_stats = function (req, res, next) {
  var cache_key = req.originalUrl
  var ttl = 1 * 60 * 60 * 1000 // 1 hour
  var main_stats = cache.get(cache_key)

  if (main_stats) {
    res.send(main_stats)
  } else {
    find_main_stats(function (err, main_stats) {
      cache.put(cache_key, main_stats, ttl)
      if (err) return next(err)
      return res.send(main_stats)
    })
  }
}

var get_block = function (req, res, next) {
  var params = req.data
  var height_or_hash = params.height_or_hash

  find_block(height_or_hash, function (err, block) {
    if (err) return next(err)
    return res.send(block)
  })
}

var get_transaction = function (req, res, next) {
  var params = req.data
  var txid = params.txid

  find_transaction(txid, function (err, tx) {
    if (err) return next(err)
    return res.send(tx)
  })
}

var get_asset_info = function (req, res, next) {
  var params = req.data
  var assetId = params.assetId
  var utxo = params.utxo
  var verbosity = parseInt(params.verbosity)
  verbosity = ([0,1].indexOf(verbosity) > -1)? verbosity : 1

  logger.debug(utxo)

  async.parallel([
    function (cb) {
      if (verbosity == 0) return cb()
      find_asset_info(assetId, false, cb) 
    },
    function (cb) {
      if (!utxo) return cb()
      find_first_issuance(assetId, utxo, cb)  
    }
  ],
  function (err, results) {
    if (err) return next(err)
    var asset_info = results[0] || {}
    var issuanceTxid = results[1]
    if (issuanceTxid) asset_info.issuanceTxid = issuanceTxid
    if ((verbosity < 2) && ('holders' in asset_info)) delete asset_info['holders']
    res.send(asset_info)
  })
}

var get_asset_info_with_transactions = function (req, res, next) {
  var params = req.data
  var assetId = params.assetId

  find_asset_info(assetId, true, function (err, info) {
    if (err) return next(err)
    return res.send(info)
  })
}

var get_addresses_info = function (req, res, next) {
  var params = req.data
  var addresses = params.addresses
  var confirmations = params.confirmations || 0
  confirmations = parseInt(confirmations, 10)

  find_addresses_info(addresses, confirmations, function (err, infos) {
    if (err) return next(err)
    infos.forEach(function (info) {
      if ('transactions' in info) delete info['transactions']
      if ('utxos' in info) delete info['utxos']
    })
    return res.send(infos)
  })
}
var get_addresses_info_with_transactions = function (req, res, next) {
  var params = req.data
  var addresses = params.addresses
  var confirmations = params.confirmations || 0
  confirmations = parseInt(confirmations, 10)

  find_addresses_info(addresses, confirmations, function (err, infos) {
    if (err) return next(err)
    return res.send(infos)
  })
}

var get_address_info = function (req, res, next) {
  var params = req.data
  var address = params.address
  var confirmations = params.confirmations || 0
  confirmations = parseInt(confirmations, 10)

  find_address_info(address, confirmations, function (err, info) {
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

  find_address_info(address, confirmations, function (err, info) {
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

// var to_set = function (arr1, arr2) {
//   var ans = null
//   if (Array.isArray(arr1) && Array.isArray(arr2)) {
//     ans = arr1.slice()
//     arr2.forEach(function (item) {
//       if (ans.indexOf(item) === -1) {
//         ans.push(item)
//       }
//     })
//   }
//   return ans
// }

var search = function (req, res, next) {
  var params = req.data
  var arg = params.arg

  find_transaction(arg, function (err, tx) {
    if (err) return next(err)
    if (tx) return res.send({transaction: arg})
    is_asset(arg, function (err, is_asset) {
      if (err) return next(err)
      if (is_asset) return res.send({assetId: arg})
      try {
        var address = bitcoin.Address.fromBase58Check(arg)
        if (address) {
          return res.send({addressinfo: arg})
        } else {
          return next(['Not found.', 404])
        }
      } catch (e) {
        find_block(arg, function (err, block) {
          if (err) return next(err)
          if (block) return res.send({block: arg})
          return next(['Not found.', 404])
        })
      }
    })
  })
}

var get_asset_holders = function (req, res, next) {
  var params = req.data
  var assetId = params.assetId
  var confirmations = params.confirmations || 0
  confirmations = parseInt(confirmations, 10)

  find_asset_holders(assetId, confirmations, function (err, holders) {
    if (err) return next(err)
    return res.send(holders)
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

var get_block_with_transactions = function (req, res, next) {
  var params = req.data
  var height_or_hash = params.height_or_hash
  var colored = params.colored

  find_block_with_transactions(height_or_hash, colored, function (err, block) {
    if (err) return next(err)
    return res.send(block)
  })
}

var is_active = function (req, res, next) {
  var params = req.data
  var addresses = params.addresses
  if (!addresses || !Array.isArray(addresses)) return next('addresses should be array')
  var match = {
    address: {
      $in: addresses
    }
  }
  var group = {
    _id: "$address"
  }
  var sort = {
    _id: 1
  }

  AddressesTransactions.aggregate(
    {$match   : match},
    {$group   : group},
    {$sort: sort}
  ).exec(function (err, active_addresses) {
    if (err) return next(err)
    var ans = addresses.map(function (address) {
      var found = binarySearch(active_addresses, address, (a, b) => { return a._id.localeCompare(b) }) >= 0
      return {
        address: address,
        active: found
      }
    })
    res.send(ans)
  })
}

var transmit = function (req, res, next) {
  var params = req.data
  var txHex = params.txHex

  scanner.transmit(txHex, function (err, ans) {
    if (err) return next(err)
    res.send(ans)
  })
}

module.exports = {
  get_block: get_block,
  get_block_with_transactions: get_block_with_transactions,
  get_transaction: get_transaction,
  get_address_info: get_address_info,
  get_address_info_with_transactions: get_address_info_with_transactions,
  get_addresses_info: get_addresses_info,
  get_addresses_info_with_transactions: get_addresses_info_with_transactions,
  get_asset_info: get_asset_info,
  get_asset_info_with_transactions: get_asset_info_with_transactions,
  get_address_utxos: get_address_utxos,
  get_addresses_utxos: get_addresses_utxos,
  search: search,
  get_blocks: get_blocks,
  get_asset_holders: get_asset_holders,
  get_transactions_by_intervals: get_transactions_by_intervals,
  get_main_stats: get_main_stats,
  get_cc_transactions: get_cc_transactions,
  get_popular_assets: get_popular_assets,
  parse_tx: parse_tx,
  get_utxo: get_utxo,
  get_utxos: get_utxos,
  get_mempool_txids: get_mempool_txids,
  get_info: get_info,
  is_active: is_active,
  transmit: transmit
}
