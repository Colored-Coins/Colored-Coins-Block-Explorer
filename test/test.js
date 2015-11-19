// var should = require('chai').should()
// var supertest = require('supertest')
// var async = require('async')
// var mongoose = require('mongoose')
process.env.NODE_ENV = 'QA'
process.env.MONGOOSE_DISABLE_STABILITY_WARNING = true
// var casimir = global.casimir = require(__dirname + '/../bin/casimir.js')
// var util = require('util')

// var server = casimir.server.http_server
// var db = casimir.db
// var properties = casimir.properties

// server.listen(casimir.server.port)
// var api = supertest(server)

describe('Initilazing Testing Enviroment', function () {
  // before(function (done) {
  //   db.init(properties.db, mongoose, function (err, blah) {
  //     if (err) throw err
  //     done()
  //   })
  // })

  it('dummy', function (done) {
    done()
  })
})
