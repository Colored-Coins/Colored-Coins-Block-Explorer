var bitcoin = require('bitcoinjs-lib')
var async = require('async')

var casimir = global.casimir
var properties = casimir.properties
var db = casimir.db
var scanner = casimir.scanner
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

var transactionAttributes = {exclude: ['index_in_block']}
var inputAttributes = {exclude: ['output_id', 'input_txid', 'input_index']}
var outputAttributes = {exclude: ['id', 'txid']}

var squel = require('squel').useFlavour('postgres')

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
  console.log('parse_tx')
  var params = req.data
  var txid = params.txid || ''
  var callback
  console.time('parse_tx: full_parse ' + txid)
  callback = function (data) {
    if (data.priority_parsed === txid) {
      console.timeEnd('parse_tx: full_parse ' + txid)
      process.removeListener('message', callback)
      if (data.err) return next(data.err)
      res.send({txid: txid})
    }
  }

  process.on('message', callback)

  console.time('priority_parse: api_to_parent ' + txid)
  process.send({to: properties.roles.SCANNER, parse_priority: txid})
  console.timeEnd('priority_parse: api_to_parent ' + txid)
}

var is_transaction = function (txid, callback) {
  console.log('is_transaction, txid = ', txid)
  Transactions.findById(txid, {raw: true})
    .then(function (tx) { callback(null, !!tx) })
    .catch(callback)
}

var to_sql_columns = function (model, options) {
  var columns = []
  var table_name = model.getTableName()
  Object.keys(model.attributes).forEach(function (attribute) {
    if (!options || !options.exclude || options.exclude.indexOf(attribute) === -1) {
      columns.push(table_name + '."' + attribute + '"')
    }
  })
  return columns.join(', ') + (options.last ? '' : ',')
}

var to_sql_values = function (values) {
  return '(' + values.map(function (value) {
    return '\'' + value + '\'' 
  }).join(', ') + ')'
}

var get_find_transaction_query = function (transactions_condition) {
  return '' +
    'SELECT\n' +
    '  ' + to_sql_columns(Transactions, {exclude: ['index_in_block']}) + '\n' +
    '  to_json(array(\n' +
    '    SELECT\n' +
    '      vin\n' +
    '    FROM\n' +
    '      (SELECT\n' +
    '       ' + to_sql_columns(Inputs, {exclude: ['input_index', 'input_txid', 'output_id']}) + '\n' +
    '       "previousOutput"."scriptPubKey" AS "previousOutput",\n' +
    '       to_json(array(\n' +
    '          SELECT\n' +
    '            assets\n' +
    '          FROM\n' +
    '            (SELECT\n' +
    '              ' + to_sql_columns(AssetsOutputs, {exclude: ['index_in_output', 'output_id']}) + '\n' +
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
    '        ' + to_sql_columns(Outputs, {exclude: ['id', 'txid']}) + '\n' +
    '        to_json(array(\n' +
    '         SELECT assets FROM\n' +
    '           (SELECT\n' +
    '              ' + to_sql_columns(AssetsOutputs, {exclude: ['index_in_output', 'output_id']}) + '\n' +
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
  var find_transaction_query = get_find_transaction_query('txid IN ' + to_sql_values(txids)) + ';'
  console.log('find_transaction_query = ', find_transaction_query)
  sequelize.query(find_transaction_query, {type: sequelize.QueryTypes.SELECT, logging: console.log, benchmark: true})
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
    '      (' + get_find_transaction_query('transactions.blockheight = blocks.height') + '\n' +
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

var find_addresses_info = function (addresses, confirmations, callback) {
  var ans = []
  async.each(addresses, function (address, cb) {
    find_address_info(address, confirmations, function (err, address_info) {
      if (err) return cb(err)
      ans.push(address_info)
      cb()
    })
  },
  function (err) {
    return callback(err, ans)
  })
}

var find_address_info = function (address, confirmations, callback) {
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
      attributes: transactionAttributes,
      as: 'transaction',
      where: !confirmations ? null : {
        blockheight: { $gte: 0, $lte: properties.last_block - confirmations + 1 }
      },
      include: [
        { model: Inputs, as: 'vin', attributes: inputAttributes, include: [
          { model: Outputs, as: 'previousOutput', attributes: outputAttributes }
        ]}, // TODO Oded - include assets
        { model: Outputs, as: 'vout', attributes: outputAttributes }        
      ],
      order: [
        [{model: Inputs, as: 'vin'}, 'input_index', 'ASC'],
        [{model: Outputs, as: 'vout'}, 'n', 'ASC']
      ]
    }
  ]

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

  AddressesOutputs.findAll({ where: where, attributes: attributes, include: include, raw: true, logging: true, benchmark: true })
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
    '      blockheight BETWEEN 0 AND ' + (properties.last_block - confirmations + 1) +'\n' +
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

var find_asset_info_with_transactions = function (assetId, options, callback) {
  console.log('options = ', options)
  find_asset_info(assetId, options, function (err, asset_info) {
    if (err) return callback(err)
    console.log('asset_info = ', asset_info)
    async.parallel([
      function (cb) {
        console.log('before first')
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
    '    address IN ' + to_sql_values(addresses) + '\n' +
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
  search: search,
  parse_tx: parse_tx,
  get_utxo: get_utxo,
  get_utxos: get_utxos,
  get_address_info: get_address_info,
  get_address_info_with_transactions: get_address_info_with_transactions,
  get_addresses_info: get_addresses_info,
  get_addresses_info_with_transactions: get_addresses_info_with_transactions,
  get_mempool_txids: get_mempool_txids,
  get_info: get_info,
  is_active: is_active,
  transmit: transmit
}
