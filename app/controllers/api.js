var casimir = global.casimir
var properties = casimir.properties
var db = casimir.db
var async = require('async')
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

  find_transaction(txid, function (err, tx) {
    if (err) return next(err)
    return res.send(tx)
  })
}

var get_asset_info = function (req, res, next) {
  var params = req.data
  var assetId = params.assetId
  var utxo = params.utxo

  find_asset_info(assetId, utxo, function (err, asset_info) {
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
    .then(function (tx) { callback(!!tx) })
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

var find_transaction_query = [
  'SELECT',
  '  ' + to_sql_columns(Transactions, {exclude: ['index_in_block']}),
  '  to_json(array(',
  '    SELECT',
  '      vin',
  '    FROM',
  '      (SELECT',
  '       ' + to_sql_columns(Inputs, {exclude: ['input_index', 'input_txid', 'output_id']}),
  '       "previousOutput"."scriptPubKey" AS "previousOutput",',
  '       to_json(array(',
  '          SELECT',
  '            assets',
  '          FROM',
  '            (SELECT',
  '              ' + to_sql_columns(AssetsOutputs, {exclude: ['index_in_output', 'output_id']}),
  '              assets.*',
  '            FROM',
  '              assetsoutputs',
  '            INNER JOIN assets ON assets."assetId" = assetsoutputs."assetId"',
  '            WHERE assetsoutputs.output_id = inputs.output_id ORDER BY index_in_output)',
  '        AS assets)) AS assets',
  '      FROM',
  '        inputs',
  '      INNER JOIN',
  '        (SELECT outputs.id, outputs."scriptPubKey"',
  '         FROM outputs) AS "previousOutput" ON "previousOutput".id = inputs.output_id',
  '      WHERE',
  '        inputs.input_txid = transactions.txid',
  '      ORDER BY input_index) AS vin)) AS vin,',
  '  to_json(array(',
  '    SELECT',
  '      vout',
  '    FROM',
  '      (SELECT',
  '        ' + to_sql_columns(Outputs, {exclude: ['id', 'txid']}),
  '        to_json(array(',
  '         SELECT assets FROM',
  '           (SELECT',
  '              ' + to_sql_columns(AssetsOutputs, {exclude: ['index_in_output', 'output_id']}),
  '              assets.*',
  '            FROM',
  '              assetsoutputs',
  '            INNER JOIN assets ON assets."assetId" = assetsoutputs."assetId"',
  '            WHERE assetsoutputs.output_id = outputs.id ORDER BY index_in_output)',
  '        AS assets)) AS assets',
  '      FROM',
  '        outputs',
  '      WHERE outputs.txid = transactions.txid',
  '      ORDER BY n) AS vout)) AS vout',
  'FROM',
  '  transactions',
  'WHERE',
  '  txid = :txid;'
].join('\n')

var find_transaction = function (txid, callback) {
  sequelize.query(find_transaction_query, {replacements: {txid: txid}, type: sequelize.QueryTypes.SELECT, logging: console.log, benchmark: true})
    .then(function (transactions) {
      callback(null, transactions[0])
    })
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
    include[0].attributes = transactionAttributes
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

var find_asset_info = function (assetId, utxo, callback) {
  var find_asset_info_query = [
    'SELECT',
    '  assetsoutputs."assetId",',
    utxo ? '  min(CASE WHEN assetsoutputs.txid = :txid AND assetsoutputs.n = :n THEN assetsoutputs."issueTxid" ELSE NULL END) AS "issuanceTxid",' : '',
    '  min(assetsoutputs.blockheight) AS "firstBlock",',
    '  min(assetsoutputs.txid || \':\' || assetsoutputs.n) AS "someUtxo",',
    '  min(assetsoutputs.divisibility) AS divisibility,',
    '  min(assetsoutputs."aggregationPolicy") AS "aggregationPolicy",',
    '  bool_or(assetsoutputs."lockStatus") AS "lockStatus",',
    '  to_json(array_agg(holders) FILTER (WHERE used = false)) as holders,',
    '  count(DISTINCT (CASE WHEN assetsoutputs.type = \'issuance\' THEN assetsoutputs.txid ELSE NULL END)) AS "numOfIssuance",',
    '  count(DISTINCT (CASE WHEN assetsoutputs.type = \'transfer\' THEN assetsoutputs.txid ELSE NULL END)) AS "numOfTransfers",',
    '  sum(CASE WHEN assetsoutputs.used = false THEN assetsoutputs.amount ELSE NULL END) AS "totalSupply"',
    'FROM',
    '  (select',
    '    assets.*,',
    '    assetsoutputs.amount,',
    '    assetsoutputs."issueTxid",',
    '    outputs.txid,',
    '    outputs.n,',
    '    outputs.used,',
    '    json_build_object(\'addresses\', outputs.addresses, \'amount\', amount) AS holders,',
    '    transactions.blockheight,',
    '    transactions.type',
    '  FROM',
    '    assets',
    '  LEFT OUTER JOIN (',
    '    SELECT',
    '      assetsoutputs.output_id,',
    '      assetsoutputs."issueTxid",',
    '      assetsoutputs.amount,',
    '      assetsoutputs."assetId"',
    '    FROM',
    '      assetsoutputs',
    '    ) AS assetsoutputs ON assetsoutputs."assetId" = assets."assetId"',
    '  INNER JOIN (',
    '      SELECT',
    '        outputs.id,',
    '        outputs.used,',
    '        outputs.txid,',
    '        outputs.n,',
    '        outputs."scriptPubKey"->\'addresses\' AS addresses',
    '      FROM',
    '        outputs',
    '    ) AS outputs ON outputs.id = assetsoutputs.output_id',
    '  INNER JOIN (',
    '    SELECT',
    '      transactions.txid,',
    '      transactions.blockheight,',
    '      transactions.ccdata::JSON->0->>\'type\' AS type',
    '    FROM',
    '      transactions',
    '    ) AS transactions ON transactions.txid = outputs.txid) AS assetsoutputs',
    'WHERE',
    '  "assetId" = :assetId',
    'GROUP BY',
    '  "assetId"'
  ].join('\n')
  if (typeof utxo === 'function') {
    callback = utxo
    utxo = null
  }

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
      var holders = {}
      asset_info = asset_info[0]
      console.log(JSON.stringify(asset_info))
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
  AssetsTransactions.findOne({where: {assetId: assetId}, raw: true})
    .then(function (asset_transaction) { callback(null, !!asset_transaction) })
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
  search: search,
  parse_tx: parse_tx,
  get_utxo: get_utxo,
  get_utxos: get_utxos,
  get_address_info: get_address_info,
  get_address_info_with_transactions: get_address_info_with_transactions,
  get_addresses_info: get_addresses_info,
  get_addresses_info_with_transactions: get_addresses_info_with_transactions,
  transmit: transmit
}
