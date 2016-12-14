var errorHandler = require('cc-errors').errorHandler
var requestId = require('cc-request-id')
var casimir_core = require('casimircore')()
var properties = casimir_core.properties(__dirname + '/../config/')
if (properties.ENV.type === 'development') process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0'

properties.server.favicon = __dirname + '/../' + properties.server.favicon
properties.server.compression = process.env.COMPRESSION || properties.server.compression
properties.engine.view_folder = __dirname + '/../' + properties.engine.view_folder
properties.engine.static_folder = __dirname + '/../' + properties.engine.static_folder
properties.debug = process.env.DEBUG === 'true' || properties.debug === 'true'

var log_settings = {
  level: properties.log && properties.log.level,
  logentries_api_key: properties.log && properties.log.logentries_api_key,
  log_dir: __dirname + '/../app/log'
}

var logger = global.logger = casimir_core.logger(log_settings)
// Log console.log to logger.debug
console.log = logger.info
// Log console.error to logger.error
console.error = logger.error
// Log console.warn to logger.warn
console.warn = logger.warn

// Framework modules

var db = casimir_core.db

var requestSettings = {
  secret: properties.JWT.jwtTokenSecret,
  namespace: properties.server.name
}

// Add custom framwork modules for server
properties.modules = {
  validator: require(__dirname + '/../app/modules/validator.js'),
  router: casimir_core.router(__dirname + '/../routes/', __dirname + '/../app/controllers/'),
  error: errorHandler({env: properties.ENV.type}),
  logger: logger,
  requestid: requestId(requestSettings)
}

// Set server and server port
var server = casimir_core.server(properties)

module.exports = {
  server: server,
  logger: logger,
  properties: properties,
  db: db
}
