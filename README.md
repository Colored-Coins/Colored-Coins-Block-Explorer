# Colored Coins Block-Explorer
[![Build Status][travis-image]][travis-url] [![Coverage percentage][coveralls-image]][coveralls-url] [![NPM version][npm-image]][npm-url] [![Dependency Status][daviddm-image]][daviddm-url] [![Slack Channel][slack-image]][slack-url]

[![js-standard-style][js-standard-image]][js-standard-url]

> The ColoredCoins Block Explorer server

### System Requirement

1. Bitcoind that runs as an RPC server with txindex=1
2. Mongodb
3. At least a 1TB hard-drive (The current size of the data is around 600GB)


### Installation
```sh
$ npm i -g cc-block-explorer
```

### Run

You can run the Block Explorer with the following options:

```sh
  cc-explorer [options]

  Options:

    -h, --help                                         output usage information
    -V, --version                                      output the version number
    -p, --port <port>                                  Port to listen on [$PORT or 8080/8081]
    -s, --ssl <key-file-path> <certificate-file-path>  Enable ssl
    -c, --conf <config-file-path>                      Load a custom configuration file
```

Or just run it with the defualts using:

```sh
$ cc-explorer
```

### Properties

#### The propeties to provide:

- Empty uncommented properites are MANDATORY
- Commented properties are OPTINAL
- Non empty uncommented properties are the DEFUALT values but can changed if wanted

```ini
[ENV]
type=production (you can switch between development/QA/production)

#The most basic HTTP server settings, must at least contain the port value
[server]
https_port=8081
http_port=8080
cookies_secret=1234
sockets=true
favicon=app/public/favicon.ico
name=blockexplorer
cluster=0

#SSL settings. Decomment the next lines to use SSL
[ssl]
#key=
#crt=

#mongoDB settings. Decomment the next lines to use it
[db]
host=localhost
port=27000
name=explorer
dir=db/
#user=
#pass=

#Your Bitcoind server settings
[bitcoin_rpc]
ssl=false
url=localhost
path=
username=rpcuser
password=rpcpassword
port=8332
timeout=30000

#Continue scanning and parsing new transaction or just be in API mode
[scanner]
scan=true
mempool=true
mempool_only=false

#Basic HTTP authentication to lock website
[basic]
#admin_users=
#realm=

#Allows logger to send logs to logentries
[logentries]
#api_key=
```

### Developmenet

1. Fork this repo
2. npm install
3. use the Standard coding style when hacking the code - https://github.com/feross/standard
4. Send us a pull request

## License

MIT © [ColoredCoins](http://coloredcoins.org)

[js-standard-url]: https://github.com/feross/standard
[js-standard-image]: https://cdn.rawgit.com/feross/standard/master/badge.svg
[npm-image]: https://badge.fury.io/js/cc-block-explorer.svg
[npm-url]: https://npmjs.org/package/cc-block-explorer
[travis-image]: https://travis-ci.org/Colored-Coins/Colored-Coins-Block-Explorer.svg?branch=master
[travis-url]: https://travis-ci.org/Colored-Coins/Colored-Coins-Block-Explorer
[daviddm-image]: https://david-dm.org/Colored-Coins/Colored-Coins-Block-Explorer.svg?theme=shields.io
[daviddm-url]: https://david-dm.org/Colored-Coins/Colored-Coins-Block-Explorer
[coveralls-image]: https://coveralls.io/repos/Colored-Coins/Colored-Coins-Block-Explorer/badge.svg
[coveralls-url]: https://coveralls.io/r//Colored-Coins/Colored-Coins-Block-Explorer
[slack-image]: http://slack.coloredcoins.org/badge.svg
[slack-url]: http://slack.coloredcoins.org
[mocha]: https://www.npmjs.com/package/mocha
[gulp]: http://gulpjs.com/
