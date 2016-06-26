var jf = require('jsonfile')
var casimir_core = require('casimircore')()
var properties = casimir_core.properties(__dirname + '/../config/')
if (properties.ENV.type === 'development') process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0'

properties.server.favicon = __dirname + '/../' + properties.server.favicon
properties.engine.view_folder = __dirname + '/../' + properties.engine.view_folder
properties.engine.static_folder = __dirname + '/../' + properties.engine.static_folder

var log_settings = {
  env: properties.ENV.type,
  logentries_api_ley: properties.logentries.api_key,
  log_dir: __dirname + '/../app/log',
  logzio_token: properties.logzio.token,
  logzio_type: properties.logzio.type
}

var logger = global.logger = casimir_core.logger(log_settings)
// Log console.log to logger.debug
console.log = logger.info
// Log console.error to logger.error
console.error = logger.error
// Log console.warn to logger.warn
console.warn = logger.warn

// //////// Routes Files /////// //
var routesDir = __dirname + '/../routes/'

var GET_public = jf.readFileSync(routesDir + 'GET-public.json')
var GET_private = jf.readFileSync(routesDir + 'GET-private.json')
var POST_public = jf.readFileSync(routesDir + 'POST-public.json')
var POST_private = jf.readFileSync(routesDir + 'POST-private.json')
var PUT_public = jf.readFileSync(routesDir + 'PUT-public.json')
var PUT_private = jf.readFileSync(routesDir + 'PUT-private.json')
var DELETE_public = jf.readFileSync(routesDir + 'DELETE-public.json')
var DELETE_private = jf.readFileSync(routesDir + 'DELETE-private.json')

var routes = {
  // Routing settings
  GET: { Public: GET_public, Private: GET_private },
  POST: { Public: POST_public, Private: POST_private },
  PUT: { Public: PUT_public, Private: PUT_private },
  DELETE: { Public: DELETE_public, Private: DELETE_private }
}

// Framework modules
properties.db.dir = __dirname + '/../' + properties.db.dir
var db = casimir_core.sqldb(properties.db)

var requestSettings = {
  secret: properties.JWT.jwtTokenSecret,
  namespace: properties.server.name
}

// Add custom framwork modules for server
properties.modules = {
  validator: require(__dirname + '/../app/modules/validator.js'),
  router: casimir_core.router(routes, __dirname + '/../app/controllers/'),
  error: casimir_core.error(properties.ENV.type),
  logger: logger,
  requestid: casimir_core.request_id(requestSettings)
}

// Set server and server port
var server = casimir_core.server(properties)

module.exports = {
  server: server,
  logger: logger,
  properties: properties,
  db: db
}
