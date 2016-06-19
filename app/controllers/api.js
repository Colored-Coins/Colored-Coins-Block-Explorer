var bitcoin = require('bitcoinjs-lib')
var async = require('async')
var moment = require('moment')
var Cache = require('ttl')

var casimir = global.casimir
var properties = casimir.properties
var db = casimir.db
var scanner = casimir.scanner
var _ = require('lodash')
var cache = new Cache({
  ttl: 10 * 1000, // 10 Seconds
  capacity: 300
})

var sequelize = db.sequelize
var Blocks = db.blocks
var Transactions = db.transactions
var Outputs = db.outputs
var Inputs = db.inputs
var AddressesOutputs = db.addressesoutputs
var AssetsOutputs = db.assetsoutputs
var Assets = db.assets

var squel = require('squel')
squel.cls.DefaultQueryBuilderOptions.autoQuoteFieldNames = true
squel.cls.DefaultQueryBuilderOptions.nameQuoteCharacter = '"'
squel.cls.DefaultQueryBuilderOptions.separator = '\n'
var sql_builder = require('nodejs-sql')(squel)

var MAX_BLOCKS_ALLOWED = 50

var get_transaction = function (req, res, next) {
  var params = req.data
  var txid = params.txid

  find_transactions([txid], function (err, transactions) {
    if (err) return next(err)
    var transaction = (transactions.length && transactions[0]) || {}
    return res.send(transaction)
  })
}

var get_asset_info = function (req, res, next) {
  var params = req.data
  var assetId = params.assetId
  var utxo = params.utxo

  find_asset_info(assetId, {utxo: utxo, with_transactions: false}, function (err, asset_info) {
    if (err) return next(err)
    delete asset_info['holders']
    return res.send(asset_info)
  })
}

var get_asset_info_with_transactions = function (req, res, next) {
  var params = req.data
  var assetId = params.assetId

  find_asset_info_with_transactions(assetId, {with_transactions: true}, function (err, asset_info) {
    if (err) return next(err)
    return res.send(asset_info)
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

var get_block = function (req, res, next) {
  var params = req.data
  var height_or_hash = params.height_or_hash

  find_block(height_or_hash, false, function (err, block) {
    if (err) return next(err)
    return res.send(block)
  })
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

  find_addresses_info([address], confirmations, function (err, infos) {
    if (err) return next(err)
    var info = infos[0]
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

  find_addresses_info([address], confirmations, function (err, infos) {
    if (err) return next(err)
    var info = infos[0]
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

var search = function (req, res, next) {
  console.log('search')
  var params = req.data
  var arg = params.arg

  is_transaction(arg, function (err, tx) {
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
        find_block(arg, false, function (err, block) {
          if (err) return next(err)
          if (block) return res.send({block: arg})
          return next(['Not found.', 404])
        })
      }
    })
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
  console.time('parse_tx: full_parse ' + txid)
  scanner.priority_parse(txid, function (err) {
    if (err) return next(err)
    console.timeEnd('parse_tx: full_parse ' + txid)
    res.send({txid: txid})
  })
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

var is_transaction = function (txid, callback) {
  console.log('is_transaction, txid = ', txid)
  Transactions.findById(txid, {raw: true, logging: console.log, benchmark: true})
    .then(function (tx) { callback(null, !!tx) })
    .catch(callback)
}

var get_find_transactions_query = function (transactions_condition) {
  return '' +
    'SELECT\n' +
    '  ' + sql_builder.to_columns_of_model(Transactions, {exclude: ['index_in_block']}) + ',\n' +
    '  to_json(array(\n' +
    '    SELECT\n' +
    '      vin\n' +
    '    FROM\n' +
    '      (SELECT\n' +
    '       ' + sql_builder.to_columns_of_model(Inputs, {exclude: ['input_index', 'input_txid', 'output_id']}) + ',\n' +
    '       "previousOutput"."scriptPubKey" AS "previousOutput",\n' +
    '       to_json(array(\n' +
    '          SELECT\n' +
    '            assets\n' +
    '          FROM\n' +
    '            (SELECT\n' +
    '              ' + sql_builder.to_columns_of_model(AssetsOutputs, {exclude: ['index_in_output', 'output_id']}) + ',\n' +
    '              assets.*\n' +
    '            FROM\n' +
    '              assetsoutputs\n' +
    '            INNER JOIN assets ON assets."assetId" = assetsoutputs."assetId"\n' +
    '            WHERE assetsoutputs.output_id = inputs.output_id ORDER BY index_in_output)\n' +
    '        AS assets)) AS assets\n' +
    '      FROM\n' +
    '        inputs\n' +
    '      LEFT OUTER JOIN\n' +
    '        (SELECT outputs.id, outputs."scriptPubKey"\n' +
    '         FROM outputs) AS "previousOutput" ON "previousOutput".id = inputs.output_id\n' +
    '      WHERE\n' +
    '        inputs.input_txid = transactions.txid\n' +
    '      ORDER BY input_index) AS vin)) AS vin,\n' +
    '  to_json(array(\n' +
    '    SELECT\n' +
    '      vout\n' +
    '    FROM\n' +
    '      (SELECT\n' +
    '        ' + sql_builder.to_columns_of_model(Outputs, {exclude: ['id', 'txid']}) + ',\n' +
    '        to_json(array(\n' +
    '         SELECT assets FROM\n' +
    '           (SELECT\n' +
    '              ' + sql_builder.to_columns_of_model(AssetsOutputs, {exclude: ['index_in_output', 'output_id']}) + ',\n' +
    '              assets.*\n' +
    '            FROM\n' +
    '              assetsoutputs\n' +
    '            INNER JOIN assets ON assets."assetId" = assetsoutputs."assetId"\n' +
    '            WHERE assetsoutputs.output_id = outputs.id ORDER BY index_in_output)\n' +
    '        AS assets)) AS assets\n' +
    '      FROM\n' +
    '        outputs\n' +
    '      WHERE outputs.txid = transactions.txid\n' +
    '      ORDER BY n) AS vout)) AS vout\n' +
    'FROM\n' +
    '  transactions\n' +
    'WHERE\n' +
    '  ' + transactions_condition
}

var find_transactions = function (txids, callback) {
  var find_transactions_query = get_find_transactions_query('txid IN ' + sql_builder.to_values(txids)) + ';'
  console.log('find_transactions_query = ', find_transactions_query)
  sequelize.query(find_transactions_query, {type: sequelize.QueryTypes.SELECT, logging: console.log, benchmark: true})
    .then(function (transactions) {
      transactions.forEach(function (transaction) {
        transaction.confirmations = properties.last_block - transaction.blockheight + 1
      })
      callback(null, transactions)
    })
    .catch(callback)
}

var find_block = function (height_or_hash, with_transactions, callback) {
  var block_condition
  if (typeof height_or_hash === 'string' && height_or_hash.length > 10) {
    block_condition = 'hash = \'' + height_or_hash + '\''
  } else {
    var height = parseInt(height_or_hash, 10)
    if (height) {
      block_condition = 'height = ' + height_or_hash
    } else {
      return callback()
    }
  }
  block_condition += ' AND ccparsed = TRUE'

  var find_block_query = '' +
    'SELECT\n' +
    '  blocks.*,\n' +
    '  to_json(array(\n' +
    (with_transactions ? (
    '    SELECT\n' +
    '      transactions\n' +
    '    FROM\n' +
    '      (' + get_find_transactions_query('transactions.blockheight = blocks.height') + '\n' +
    '    ORDER BY\n' +
    '      index_in_block) AS transactions)) AS transactions') : (
    '      SELECT\n' +
    '        txid\n' +
    '      FROM\n' +
    '        transactions\n' +
    '      WHERE\n' +
    '        transactions.blockheight = blocks.height\n' +
    '      ORDER BY\n' +
    '        transactions.index_in_block)) AS tx')) + '\n' +
    'FROM\n' +
    '  blocks\n' +
    'WHERE\n' +
    '  ' + block_condition
  console.log(find_block_query)
  sequelize.query(find_block_query, {type: sequelize.QueryTypes.SELECT, logging: console.log, benchmark: true})
    .then(function (blocks) {
      var block = blocks[0]
      if (!block) return callback()
      var confirmations = properties.last_block - block.height + 1
      block.confirmations = confirmations
      if (with_transactions) {
        block.tx = _.map(block.transactions, 'txid')
        block.transactions.forEach(function (transaction) {
          transaction.confirmations = confirmations
        })
      }
      callback(null, block)
    })
    .catch(callback)
}

var find_main_stats = function (callback) {
  var main_stats = {}
  async.parallel([
    function (cb) {
      var query = '' +
        'SELECT\n' +
        '  COUNT(DISTINCT assetstransactions."assetId") AS "numOfAssets",\n' +
        '  COUNT(DISTINCT assetstransactions.txid) AS "numOfCCTransactions"\n' +
        'FROM\n' +
        '  assetstransactions;'
      sequelize.query(query, {type: sequelize.QueryTypes.SELECT, logging: console.log, benchmark: true})
        .then(function (info) {
          main_stats.numOfAssets = info[0].numOfAssets
          main_stats.numOfCCTransactions = info[0].numOfCCTransactions
          cb()
        })
        .catch(cb)
    },
    function (cb) {
      var query = '' +
        'SELECT\n' +
        '  COUNT(DISTINCT assetsaddresses.address) AS "numOfHolders"\n' +
        'FROM\n' +
        '  assetsaddresses;'
      sequelize.query(query, {type: sequelize.QueryTypes.SELECT, logging: console.log, benchmark: true})
        .then(function (info) {
          main_stats.numOfHolders = info[0].numOfHolders
          cb()
        })
        .catch(cb)
    }
  ],
  function (err) {
    return callback(err, main_stats)
  })
}

var find_transactions_by_intervals = function (assetId, start, end, interval, callback) {
  console.time('find_transactions_by_intervals')
  end = end || moment.utc().millisecond(999).seconds(59).minutes(59).hours(23).valueOf() + 1
  start = start || end - 10 * 24 * 60 * 60 * 1000
  interval = interval || (end - start) / 10

  start = Math.round(start)
  end = Math.round(end)
  interval = Math.round(interval)

  if (Math.round((end - start) / interval) > 1000) return callback(['Sample resolution too high.', 500])

  var query = '' +
    'SELECT\n' +
    '  DIV(blocktime - :start, :interval) AS interval_index,\n' +
    '  COUNT(transactions.txid) AS "txsSum"\n' +
    'FROM transactions\n' +
    'JOIN assetstransactions ON assetstransactions.txid = transactions.txid\n' +
    'WHERE transactions.blocktime BETWEEN :start AND :end\n' +
    'GROUP BY interval_index'
  sequelize.query(query, {type: sequelize.QueryTypes.SELECT, replacements: {start: start, interval: interval, end: end}, logging: console.log, benchmark: true})
    .then(function (counts) {
      var ans = counts.map(function (count) {
        var from = (count.interval_index * interval) + start
        return {
          txsSum: count.txsSum,
          from: from,
          untill: from + interval
        }
      })
      callback(null, ans)
    })
    .catch(callback)
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
      console.time('find_cc_transactions find_mempool_cc_txs_query')
      var find_mempool_cc_txs_query = get_find_transactions_query('ccparsed = TRUE AND colored = TRUE and blockheight = -1') + '\n' +
        'ORDER BY\n' +
        '  blocktime DESC\n' +
        'LIMIT ' + limit + '\n' +
        'OFFSET ' + skip
      sequelize.query(find_mempool_cc_txs_query, {type: sequelize.QueryTypes.SELECT, logging: console.log, benchmark: true})
        .then(function (cc_mempool_txs) { cb(null, cc_mempool_txs) })
        .catch(cb)
    },
    function (cc_mempool_txs, cb) {
      console.timeEnd('find_cc_transactions find_mempool_cc_txs_query')
      console.time('find_cc_transactions count_mempool_cc_txs_query')
      console.log('cc_mempool_txs = ', JSON.stringify(cc_mempool_txs))
      txs = cc_mempool_txs
      if (txs.length) {
        limit -= txs.length
        skip = 0
        return cb()
      }
      var count_mempool_cc_txs_query = '' +
        'SELECT COUNT(*)\n' +
        'FROM\n' +
        '  transactions\n' +
        'WHERE\n' +
        '  ccparsed = TRUE AND colored = TRUE and blockheight = -1'
      sequelize.query(count_mempool_cc_txs_query, {type: sequelize.QueryTypes.SELECT, logging: console.log, benchmark: true})
        .then(function (results) {
          console.log('results = ', JSON.stringify(results))
          var count = results[0].count
          skip -= count
          skip = Math.max(skip, 0)
          cb()
        })
        .catch(cb)
    },
    function (cb) {
      console.timeEnd('find_cc_transactions count_mempool_cc_txs_query')
      console.time('find_cc_transactions find_latest_confirmed_cc_txs_query')
      if (!limit) return cb()
      var find_latest_confirmed_cc_txs_query = get_find_transactions_query('ccparsed = TRUE AND colored = TRUE and blockheight > 0') + '\n' +
        'ORDER BY\n' +
        '  blockheight DESC\n' +
        'LIMIT ' + limit + '\n' +
        'OFFSET ' + skip

      sequelize.query(find_latest_confirmed_cc_txs_query, {type: sequelize.QueryTypes.SELECT, logging: console.log, benchmark: true})
        .then(function (conf_txs) {
          console.log('cc_mempool_txs = ', JSON.stringify(conf_txs))
          txs = txs.concat(conf_txs)
          cb()
        })
        .catch(cb)
    }
  ], function (err) {
    if (err) return callback(err)
    console.timeEnd('find_cc_transactions find_latest_confirmed_cc_txs_query')
    callback(null, txs)
  })
}

var find_addresses_info = function (addresses, confirmations, callback) {
  var transactions_conditions = !confirmations ? '1 = 1' : 'blockheight BETWEEN 0 AND ' + (properties.last_block - confirmations + 1)

  var find_addresses_info_query = '' +
    'SELECT\n' +
    '  addressestransactions.address,\n' +
    '  to_json(array_agg(transactions.*)) AS transactions\n' +
    'FROM addressestransactions\n' +
    'LEFT OUTER JOIN\n' +
    '(' + get_find_transactions_query(transactions_conditions) + ') AS transactions ON transactions.txid = addressestransactions.txid\n' +
    'WHERE\n' +
    '  address IN ' + sql_builder.to_values(addresses) + '\n' +
    'GROUP BY\n' +
    '   address;'

  sequelize.query(find_addresses_info_query, {type: sequelize.QueryTypes.SELECT, logging: console.log, benchmark: true})
    .then(function (addresses_info) {
      if (!addresses_info || !addresses_info.length) return callback()
      addresses_info.forEach(function (address_info) {
        var utxos = address_info.utxos = []
        var assets = {}
        address_info.balance = 0
        address_info.received = 0
        address_info.transactions.forEach(function (tx) {
          tx.confirmations = properties.last_block - tx.blockheight + 1
          if ('vout' in tx && tx.vout) {
            tx.vout.forEach(function (vout) {
              if ('scriptPubKey' in vout && vout.scriptPubKey) {
                if ('addresses' in vout.scriptPubKey && vout.scriptPubKey.addresses && vout.scriptPubKey.addresses.indexOf(address_info.address) !== -1) {
                  address_info.received += vout.value
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
          address_info.balance += utxo.value
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

        address_info.assets = []
        for (var assetId in assets) {
          address_info.assets.push(assets[assetId])
        }
        address_info.numOfTransactions = address_info.transactions.length
      })
      callback(null, addresses_info)
    })
    .catch(callback)
}

var get_find_addresses_utxos_query = function (confirmations, addresses_condition) {
  confirmations = confirmations || 0
  return '' +
    'SELECT\n' +
    'addressesoutputs.address,\n' +
    sql_builder.to_columns_of_model(Outputs, {exclude: ['id']}) + ',\n' +
    'to_json(array(\n' +
    '  SELECT assets FROM\n' +
    '    (SELECT\n' +
    '      assetsoutputs."assetId", assetsoutputs."amount", assetsoutputs."issueTxid",\n' +
    '      assets.*\n' +
    '    FROM\n' +
    '      assetsoutputs\n' +
    '    INNER JOIN assets ON assets."assetId" = assetsoutputs."assetId"\n' +
    '    WHERE assetsoutputs.output_id = outputs.id ORDER BY index_in_output)\n' +
    '    AS assets)) AS assets\n' +
    'FROM\n' +
    '  addressesoutputs\n' +
    'LEFT OUTER JOIN outputs ON outputs.id = addressesoutputs.output_id\n' +
    (confirmations ? 'INNER JOIN transactions ON transactions.txid = outputs.txid\n' : '') +
    'WHERE outputs.used = FALSE AND ' + (confirmations ? '(transactions.blockheight BETWEEN 0 AND ' + (properties.last_block - confirmations + 1) + ') AND\n' : '') +
    addresses_condition + ';'
}

var find_address_utxos = function (address, confirmations, callback) {
  var query = get_find_addresses_utxos_query(confirmations, 'addressesoutputs.address = :address')
  console.log(query)
  sequelize.query(query, {type: sequelize.QueryTypes.SELECT, replacements: {address: address}, logging: console.log, benchmark: true})
    .then(function (utxos) {
      var ans = {address: address, utxos: utxos}
      return callback(null, ans)
    })
    .catch(callback)
}

var find_addresses_utxos = function (addresses, confirmations, callback) {
  var query = get_find_addresses_utxos_query(confirmations, 'addressesoutputs.address IN ' + sql_builder.to_values(addresses))
  sequelize.query(query, {type: sequelize.QueryTypes.SELECT, logging: console.log, benchmark: true})
    .then(function (utxos) {
      var ans = _(utxos)
        .groupBy('address')
        .transform(function (result, utxos, address) {
          result.push({address: address, utxos: utxos})
        }, [])
        .value()
      return callback(null, ans)
    })
    .catch(callback)
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
      return callback('Can\'t query more than ' + MAX_BLOCKS_ALLOWED + ' blocks.')
    }
    conditions = {
      ccparsed: true
    }
  } else {
    if (end - start + 1 > MAX_BLOCKS_ALLOWED) {
      return callback('Can\'t query more than ' + MAX_BLOCKS_ALLOWED + ' blocks.')
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
      callback(null, blocks)
    })
    .catch(callback)
}

var find_asset_holders = function (assetId, confirmations, callback) {
  var find_asset_holders_query = '' +
    'SELECT\n' +
      'assetsoutputs.address,\n' +
      'min(assetsoutputs.txid || \':\' || assetsoutputs.n) AS "someUtxo",\n' +
      'min(assetsoutputs.divisibility) AS divisibility,\n' +
      'min(assetsoutputs."aggregationPolicy") AS "aggregationPolicy",\n' +
      'bool_or(assetsoutputs."lockStatus") AS "lockStatus",\n' +
      'sum(assetsoutputs.amount) AS amount\n' +
    'FROM\n' +
    '  (SELECT\n' +
    '    assets.*,\n' +
    '    assetsoutputs.amount,\n' +
    '    outputs.txid,\n' +
    '    outputs.n,\n' +
    '    jsonb_array_elements(outputs.addresses) AS address\n' +
    '  FROM\n' +
    '    assets\n' +
    '  LEFT OUTER JOIN (\n' +
    '    SELECT\n' +
    '      assetsoutputs.output_id,\n' +
    '      assetsoutputs."issueTxid",\n' +
    '      assetsoutputs.amount,\n' +
    '      assetsoutputs."assetId"\n' +
    '    FROM\n' +
    '      assetsoutputs\n' +
    '  ) AS assetsoutputs ON assetsoutputs."assetId" = assets."assetId"\n' +
    '  INNER JOIN (\n' +
    '    SELECT\n' +
    '      outputs.id,\n' +
    '      outputs.txid,\n' +
    '      outputs.n,\n' +
    '      outputs."scriptPubKey"->\'addresses\' AS addresses\n' +
    '    FROM\n' +
    '      outputs\n' +
    '    WHERE\n' +
    '      outputs.used = FALSE\n' +
    '  ) AS outputs ON outputs.id = assetsoutputs.output_id\n' +
    (confirmations ?
    '  INNER JOIN (\n' +
    '    SELECT\n' +
    '      transactions.txid,\n' +
    '      transactions.blockheight\n' +
    '    FROM\n' +
    '      transactions\n' +
    '    WHERE\n' +
    '      blockheight BETWEEN 0 AND ' + (properties.last_block - confirmations + 1) + '\n' +
    '  ) AS transactions ON transactions.txid = outputs.txid\n' : '') +
    'WHERE\n' +
    '  assets."assetId" = :assetId) AS assetsoutputs\n' +
    'GROUP BY address;'

  sequelize.query(find_asset_holders_query, {type: sequelize.QueryTypes.SELECT, replacements: {assetId: assetId}, logging: console.log, benchmark: true})
    .then(function (holders) {
      if (!holders.length) {
        return callback(null, [])
      }
      var ans = {}
      ans.assetId = assetId
      ans.someUtxo = holders[0].someUtxo
      ans.divisibility = holders[0].divisibility
      ans.lockStatus = holders[0].lockStatus
      ans.aggregationPolicy = holders[0].aggregationPolicy
      ans.holders = holders
      ans.holders.forEach(function (holder) {
        delete holder.someUtxo
        delete holder.divisibility
        delete holder.lockStatus
        delete holder.aggregationPolicy
      })
      callback(null, ans)
    })
}

var find_asset_info = function (assetId, options, callback) {
  var utxo
  var with_transactions
  if (typeof options === 'function') {
    callback = options
    utxo = null
    with_transactions = false
  } else {
    utxo = (options && options.utxo) || null
    with_transactions = (options && options.with_transactions) === true
  }

  var find_asset_info_query = '' +
    'SELECT\n' +
    '  assetsoutputs."assetId",\n' +
    (utxo ? '  min(CASE WHEN assetsoutputs.txid = :txid AND assetsoutputs.n = :n THEN assetsoutputs."issueTxid" ELSE NULL END) AS "issuanceTxid",\n' : '') +
    '  min(CASE WHEN assetsoutputs.blockheight > -1 THEN assetsoutputs.blockheight ELSE NULL END) AS "firstBlock",\n' +
    '  min(assetsoutputs.txid || \':\' || assetsoutputs.n) AS "someUtxo",\n' +
    '  min(assetsoutputs.divisibility) AS divisibility,\n' +
    '  min(assetsoutputs."aggregationPolicy") AS "aggregationPolicy",\n' +
    '  bool_or(assetsoutputs."lockStatus") AS "lockStatus",\n' +
    '  to_json(array_agg(holders) FILTER (WHERE used = false)) as holders,\n' +
    (with_transactions ?
    '  to_json(array_agg(DISTINCT (assetsoutputs.txid)) FILTER (WHERE assetsoutputs.txid IS NOT NULL AND assetsoutputs.type = \'transfer\')) AS "transfers",\n' +
    '  to_json(array_agg(DISTINCT (assetsoutputs.txid)) FILTER (WHERE assetsoutputs.txid IS NOT NULL AND assetsoutputs.type = \'issuance\')) AS "issuances",\n' :
    '  count(DISTINCT (CASE WHEN assetsoutputs.type = \'issuance\' THEN assetsoutputs.txid ELSE NULL END)) AS "numOfIssuance",\n' +
    '  count(DISTINCT (CASE WHEN assetsoutputs.type = \'transfer\' THEN assetsoutputs.txid ELSE NULL END)) AS "numOfTransfers",\n') +
    '  sum(CASE WHEN assetsoutputs.used = false THEN assetsoutputs.amount ELSE NULL END) AS "totalSupply"\n' +
    'FROM\n' +
    '  (SELECT\n' +
    '    assets.*,\n' +
    '    assetsoutputs.amount,\n' +
    '    assetsoutputs."issueTxid",\n' +
    '    outputs.txid,\n' +
    '    outputs.n,\n' +
    '    outputs.used,\n' +
    '    json_build_object(\'addresses\', outputs.addresses, \'amount\', amount) AS holders,\n' +
    '    transactions.blockheight,\n' +
    '    transactions.type\n' +
    '  FROM\n' +
    '    assets\n' +
    '  LEFT OUTER JOIN (\n' +
    '    SELECT\n' +
    '      assetsoutputs.output_id,\n' +
    '      assetsoutputs."issueTxid",\n' +
    '      assetsoutputs.amount,\n' +
    '      assetsoutputs."assetId"\n' +
    '    FROM\n' +
    '      assetsoutputs\n' +
    '    ) AS assetsoutputs ON assetsoutputs."assetId" = assets."assetId"\n' +
    '  INNER JOIN (\n' +
    '      SELECT\n' +
    '        outputs.id,\n' +
    '        outputs.used,\n' +
    '        outputs.txid,\n' +
    '        outputs.n,\n' +
    '        outputs."scriptPubKey"->\'addresses\' AS addresses\n' +
    '      FROM\n' +
    '        outputs\n' +
    '    ) AS outputs ON outputs.id = assetsoutputs.output_id\n' +
    '  INNER JOIN (\n' +
    '    SELECT\n' +
    '      transactions.txid,\n' +
    '      transactions.blockheight,\n' +
    '      transactions.ccdata::JSON->0->>\'type\' AS type\n' +
    '    FROM\n' +
    '      transactions\n' +
    '    ) AS transactions ON transactions.txid = outputs.txid) AS assetsoutputs\n' +
    'WHERE\n' +
    '  "assetId" = :assetId\n' +
    'GROUP BY\n' +
    '  "assetId"'

  var replacements = {
    assetId: assetId
  }
  if (utxo) {
    var utxo_split = utxo.split(':')
    replacements.txid = utxo_split[0]
    replacements.n = utxo_split[1]
  }

  sequelize.query(find_asset_info_query, {replacements: replacements, type: sequelize.QueryTypes.SELECT, logging: console.log, benchmark: true})
    .then(function (asset_info) {
      if (!asset_info || !asset_info.length) return callback(null, {
        assetId: assetId,
        totalSupply: 0,
        numOfHolders: 0,
        numOfTransfers: 0,
        numOfIssuance: 0,
        firstBlock: -1
      })
      asset_info = asset_info[0]
      asset_info.firstBlock = asset_info.firstBlock || -1
      var holders = {}
      if (with_transactions) {
        asset_info.numOfIssuance = asset_info.issuances.length
        asset_info.numOfTransfers = asset_info.transfers.length
      }
      asset_info.holders.forEach(function (holder) {
        holder.addresses.forEach(function (address) {
          holders[address] = holders[address] || 0
          holders[address] += holder.amount
        })
      })
      asset_info.holders = Object.keys(holders).map(function (address) {
        return {
          address: address,
          amount: holders[address]
        }
      })
      asset_info.numOfHolders = asset_info.holders.length
      callback(null, asset_info)
    })
    .catch(callback)
}

var find_asset_info_with_transactions = function (assetId, options, callback) {
  find_asset_info(assetId, options, function (err, asset_info) {
    if (err) return callback(err)
    if (!asset_info) return callback()
    async.parallel([
      function (cb) {
        find_transactions(asset_info.issuances, cb)
      },
      function (cb) {
        find_transactions(asset_info.transfers, cb)
      }
    ],
    function (err, issuances, transfers) {
      if (err) return callback(err)
      asset_info.issuances = issuances
      asset_info.transfers = transfers
      callback(null, asset_info)
    })
  })
}

var find_popular_assets = function (sort_by, limit, callback) {
  var table_name
  if (sort_by === 'transactions') {
    table_name = 'assetstransactions'
  } else if (sort_by === 'holders') {
    table_name = 'assetsaddresses'
  } else {
    table_name = 'assetstransactions'
  }

  var query = '' +
    'SELECT' +
    ' "assetId", count(*) AS count\n' +
    'FROM\n' +
    ' :table_name\n' +
    'GROUP BY\n' +
    ' "assetId"\n' +
    'ORDER BY\n' +
    ' count DESC\n' +
    'LIMIT :limit'

  sequelize.query(query, {replacements: {table_name: table_name, limit: limit}, type: sequelize.QueryTypes.SELECT, logging: console.log, benchmark: true})
    .then(function (assets) {
      async.map(assets, function (asset, cb) {
        find_asset_info(asset.assetId, function (err, info) {
          if (err) return cb(err)
          if ('holders' in info) delete info['holders']
          cb(null, info)
        })
      },
      function (err, results) {
        callback(err, results)
      })
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
  Outputs.findAll({where: where, attributes: attributes, include: include, raw: true})
    .then(function (utxos) {
      callback(null, format_utxos(utxos))
    })
    .catch(callback)
}

var is_asset = function (assetId, callback) {
  Assets.findOne({where: {assetId: assetId}, raw: true})
    .then(function (asset) { callback(null, !!asset) })
    .catch(callback)
}

var find_mempool_txids = function (colored, callback) {
  var conditions = {
    blockheight: -1
  }
  if (colored) {
    conditions.colored = true
  }
  var attributes = ['txid']
  Transactions.findAll({where: conditions, attributes: attributes, raw: true, logging: console.log, benchmark: true})
    .then(function (transactions) {
      callback(null, transactions.map(function (tx) { return tx.txid }))
    })
    .catch(callback)
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
    txsinserted: true
  }
  var attributes = ['height']
  var order = [['height', 'DESC']]
  Blocks.findOne({where: conditions, attributes: attributes, order: order, raw: true, logging: console.log, benchmark: true})
    .then(function (block_data) {
      if (!block_data) return callback(null, -1)
      callback(null, block_data.height)
    })
    .catch(callback)
}

var find_last_fixed_block = function (callback) {
  var conditions = {
    txsinserted: true,
    txsparsed: true
  }
  var attributes = ['height']
  var order = [['height', 'DESC']]
  Blocks.findOne({where: conditions, attributes: attributes, order: order, raw: true, logging: console.log, benchmark: true})
    .then(function (block_data) {
      if (!block_data) return callback(null, -1)
      callback(null, block_data.height)
    })
    .catch(callback)
}

var find_last_cc_parsed_block = function (callback) {
  var conditions = {
    txsinserted: true,
    txsparsed: true,
    ccparsed: true
  }
  var attributes = ['height']
  var order = [['height', 'DESC']]
  Blocks.findOne({where: conditions, attributes: attributes, order: order, raw: true, logging: console.log, benchmark: true})
    .then(function (block_data) {
      if (!block_data) return callback(null, -1)
      callback(null, block_data.height)
    })
    .catch(callback)
}

var find_last_blocks = function (callback) {
  async.parallel({
    last_parsed_block: find_last_parsed_block,
    last_fixed_block: find_last_fixed_block,
    last_cc_parsed_block: find_last_cc_parsed_block
  },
  callback)
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

var is_active = function (req, res, next) {
  var params = req.data
  var addresses = params.addresses
  if (!addresses || !Array.isArray(addresses)) return next('addresses should be array')
  var is_active_query = '' +
    'SELECT\n' +
    '  to_json(array_agg(address)) AS addresses\n' +
    'FROM\n' +
    '  (SELECT\n' +
    '    addressestransactions.address\n' +
    '  FROM\n' +
    '    addressestransactions\n' +
    '  WHERE\n' +
    '    address IN ' + sql_builder.to_values(addresses) + '\n' +
    '  GROUP BY\n' +
    '    addressestransactions.address) AS addresses;'
  sequelize.query(is_active_query, {type: sequelize.QueryTypes.SELECT, logging: console.log, benchmark: true})
    .then(function (active_addresses) {
      active_addresses = active_addresses[0].addresses
      var ans = addresses.map(function (address) {
        return {
          address: address,
          active: active_addresses.indexOf(address) > -1
        }
      })
      res.send(ans)
    })
    .catch(next)
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
  get_transaction: get_transaction,
  get_block: get_block,
  get_block_with_transactions: get_block_with_transactions,
  get_blocks: get_blocks,
  get_address_utxos: get_address_utxos,
  get_addresses_utxos: get_addresses_utxos,
  get_asset_info: get_asset_info,
  get_asset_info_with_transactions: get_asset_info_with_transactions,
  get_asset_holders: get_asset_holders,
  get_transactions_by_intervals: get_transactions_by_intervals,
  get_main_stats: get_main_stats,
  search: search,
  parse_tx: parse_tx,
  get_utxo: get_utxo,
  get_utxos: get_utxos,
  get_address_info: get_address_info,
  get_address_info_with_transactions: get_address_info_with_transactions,
  get_addresses_info: get_addresses_info,
  get_addresses_info_with_transactions: get_addresses_info_with_transactions,
  get_cc_transactions: get_cc_transactions,
  get_popular_assets: get_popular_assets,
  get_mempool_txids: get_mempool_txids,
  get_info: get_info,
  is_active: is_active,
  transmit: transmit
}
