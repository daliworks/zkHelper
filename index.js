'use strict';

var events = require('events'),
    util = require('util'),
    PATH = require('path'),
    async = require('async'),
    _ = require('lodash'),
    zookeeper = require ('node-zookeeper-client');

var BASE_PATH, CONFIG_PATH, TICKET_PATH, 
    logger = console;

var zkStatus = 'none',// 'initialized', 'deinitialized'
    exitCode = -1,
    mainCluster = {},
    mainMonitor,
    client;// zookeeper client

function Monitor() {
  this._saved = {};
  events.EventEmitter.call(this);
}
util.inherits(Monitor, events.EventEmitter);
mainMonitor = new Monitor();

function isNo1() {
  if (!mainCluster.ticket || _.isEmpty(mainCluster.votes)) {
    return false;
  }
  return mainCluster.ticket === mainCluster.votes[0];
}

function isMaster() {
  //no1 and exist
  return isNo1() && 
        mainCluster.master && 
        mainCluster.master === PATH.basename(mainCluster.node);
}

function appRestart(code, msg) {
  if (zkStatus === 'deinitialized') {
    logger.warn('[appRestart] not appRestart due to zkStatus=deinitialized / code=%s err=%s', code, msg);
    return;
  }

  logger.warn('[appRestart] zkStatus=%s code=%s err=%s', zkStatus, code, msg);

  if (client && code === 'disconnected') {
    logger.info('[appRestart] Disconnected - session id = ', client.getSessionId());

    setTimeout(function () {
      if (zkStatus === 'initialized' && client.getState() === zookeeper.State.SYNC_CONNECTED) {
        logger.info('[appRestart] disconnected from the zookeeper server but reconnected, state = ',
            client.getState(), ', session id = ', client.getSessionId(), ', zkStatus =', zkStatus);

        return;
      } else {
        logger.warn('[appRestart] disconnected from the zookeeper server and not reconnected, state = ',
            client.getState(), ', zkStatus =', zkStatus);

        exitCode = code ? code : 0;
        if (client) {
          client.close();//redundant call on disconnect should be ok.
        }
        process.kill(process.pid, 'SIGTERM');
      }
    }, 20 * 1000); // after 8 ticks
  } else {
    exitCode = code ? code : 0;
    if (client) {
      client.close();//redundant call on disconnect should be ok.
    }
    process.kill(process.pid, 'SIGTERM');
  }
}

function removeNode(path, cb) {
  client.remove(path, -1, function (err) {
    if (err) {
      logger.error('remove Node failure', err);
    }
    return cb && cb(err);
  });
}

function createNode(path, mode, data, cb) {
  if (typeof data === 'function') {
    cb = data;
    data = undefined;
  }

  var dirname = PATH.dirname(path);
  async.waterfall([
    function (done) {//create if not exists
      client.exists(dirname, function (err, stat) {
        if (err || stat) { //already exists or error
          return done(err);
        }
        client.mkdirp(dirname, zookeeper.CreateMode.PERSISTENT, function (err) {
          if (err) { return done(err); }
          logger.debug('created nodes/');
          return done();
        });
      });
    },
    function (done) {
      client.create(path, mode, data, function (err, rtnPath) {
        if (!err) { // persistent or sequential
          logger.debug('created ' + rtnPath);
          return done(err, rtnPath);
        } else if (err.getCode() === zookeeper.Exception.NODE_EXISTS) { //only ephermeral
          if (zookeeper.CreateMode.EPHEMERAL === mode) {
            client.transaction().
            remove(path).
            create(path, mode).
            commit(function (err, txnResults) {
              rtnPath = txnResults && txnResults[1] && txnResults[1].path;
              if (err) {
                logger.error('re-create error=', err);
              }
              logger.debug('re-created ' + rtnPath + ' txnResults=', txnResults);
              return done(err, rtnPath);
            });
          } else { //zookeeper.CreateMode.PERSISTENT
            if (data) {
              client.setData(path, data, -1, function (err) {
                if (err) {
                  logger.error('[setData] err', err);
                }
                return done(err, path);
              });
            } else {
              return done(undefined, path);
            }
          }
        } else {
          logger.error('created ' + path + ' error=', err);
          return done(err, rtnPath);
        }
      });
    }
  ], function (err, rtnPath) {
    return cb && cb(err, rtnPath);
  });
}

function _listChildren(p, monitor, done) {
  client.getChildren(p, function (/*event*/) {
      //logger.debug('Got watcher event: %s', event);
      _listChildren(p, monitor);
    },
    function (err, children) {
      if (err) {
        logger.fatal( 'Failed to list children of node: %s due to: %s.', p, err.toString(), err);
        appRestart(-1, 'watch children failure');
        return done && done(err);
      }

      var orgChildren = monitor._saved[p] || [],
        newChildren = monitor._saved[p] = _.sortBy(children),
        common = _.intersection(orgChildren, newChildren),
        added = _.difference(newChildren, common),
        deleted = _.difference(orgChildren, common);

      if (!_.isEmpty(added) || !_.isEmpty(deleted)) {
        //logger.debug('children [%s] new=%s old=%s added=%s deleted=%s',
        //  p, newChildren.toString(), orgChildren.toString(),
        //  added.toString(), deleted.toString());
        monitor.emit('children', p, newChildren, {
          added: _.clone(added, true),
          deleted: _.clone(deleted, true),
        });
      }
      return done && done(err);
    }
  );
}

function _getData(p, monitor, done) {
  client.getData(p, function (/*event*/) {
      //logger.debug('Got watcher event: %s', event);
      _getData(p, monitor);
    },
    function (err, data, stat) {
      if (err) {
        logger.fatal( 'Failed to getData of node: %s due to: %s.', p, err, stat);
        appRestart(-1, 'watch data failure');
        return done && done(err);
      }
      var property = p + '_data', orgData = monitor._saved[property],
        newData;
      try {
        newData = JSON.parse(data);
      } catch (e) {}

      monitor._saved[property] = newData;

      if (!_.isEqual(newData, orgData)) {
        //logger.debug('data [%s] new=%j old=%j', p, newData, orgData);
        monitor.emit('data', p, newData, _.clone(orgData, true));
      }

      return done && done(err);
    }
  );
}

function _watchNodes(path, monitor, cb) {
  _listChildren(path + '/nodes', monitor, function (err) {
    return cb && cb(err);
  });
}

function _watchVotes(path, monitor, cb) {
  _listChildren(path + '/votes', monitor, function (err) {
    return cb && cb(err);
  });
}

function _watchData(path, monitor, cb) {
  _getData(path, monitor, function (err) {
    return cb && cb(err);
  });
}

function watchAll(path, monitor, observerOnly, cb) {
  async.series([
    function (done) { // nodes
      _watchNodes(path, monitor, done);
    },
    function (done) { // votes
      if (mainCluster.observerOnly || observerOnly) {
        return done();
      }
      _watchVotes(path, monitor, done);
    },
    function (done) { // master data
      _watchData(path, monitor, done);
    }
  ], function (err) {
    return cb && cb(err);
  });
}

function setMaster(master, cb) {
  var data = {
    master: master ? PATH.basename(master) : undefined,
  };
  client.setData(BASE_PATH, new Buffer(JSON.stringify(data)), -1, 
    function (err/*, stat*/) {
    if (!err) {
      mainCluster.master = data.master;
    }
    return cb && cb(err);
  });
}

//Note: DO NOT check no1(actually cannot)
function getMaster(cluster) {
  if (!cluster) {
    cluster = mainCluster;
  }
  if (cluster.master &&
    _.contains(cluster.nodes, cluster.master)) {
    return cluster.master;
  }
  return;
}

function getSlaves(cluster) {
  var master;
  if (!cluster) {
    cluster = mainCluster;
  }
  master = getMaster(cluster);

  return _.difference(cluster.nodes, [master]);
}

function getNodes(cluster) {
  if (!cluster) {
    cluster = mainCluster;
  }
  return cluster.nodes;
}

function getConfig() {
  return mainCluster.config;
}

/* 
 * option:
 *  basePath: zk basePath, optional if observerOnly
 *  node: host [':' + port], optional if observerOnly
 *  servers: zookeeper servers
 *  configPath: optional 
 *  noRestartOnConfigChange: default false
 *  logger: console(default)
 *  clientOptions: zookeeper client options
 *  observerOnly: false(default), 
 *                true: no voting, read status change only
 *                  - watch node if basePath exists
 *                  - watch config data if configPath exists
 *
 * callback: err, client(created zk client)
 *
 * Example:
 * var zk = require('zkHelper'),
 * options = {
 *  basePath: '/myapp';
 *  configPath: '/myapp/config',
 *  node: require('os').hostname(),
 *  servers: ['zk0:2181', 'zk1:2181', 'zk2:2181'], // zk servers
 *  clientOptons: {
 *    sessionTimeout: 10000,
 *    retries: 3
 *  }
 * };
 * zk.init(options, function (err, zkClient) {
 *   // do something
 * });
 *  
 */
function init(opt, cb) {

  if (!opt.servers) {
    return cb && cb(new Error('zk server is missing'));
  }
  if (!opt.observerOnly && !(opt.basePath && opt.node)) {
    return cb && cb(new Error('node name or basePath is missing'));
  }

  BASE_PATH = opt.basePath;
  CONFIG_PATH = opt.configPath;
  TICKET_PATH = BASE_PATH + '/votes/n_';
  logger = opt.logger ? opt.logger : console;

  var nodePath, masterAfterInit;

  if (BASE_PATH && opt.node) {
    nodePath = BASE_PATH + '/nodes/' + opt.node;
  }

  mainCluster.observerOnly = opt.observerOnly ? true : false;
  logger.info('Connecting ZooKeeper Server', JSON.stringify(opt.servers));
  client = zookeeper.createClient(opt.servers.join(','), opt.clientOptions);

  _.each(['expired', 'error', 'disconnected', 'authenticationFailed'], function (eventName) {
    client.on(eventName, appRestart.bind(null, eventName, 'expired, error or disconnected'));
  });

  client.on('state', function (state) {
    logger.debug('state: ' + state);
  });

  client.once('connected', function () {
    logger.info('on connected, session id =', client.getSessionId());
    async.series([
      function (done) {
        if (nodePath) {
          logger.debug('1. create client node');
        } else {
          logger.debug('1. create client node: skip due to missing basePath or node');
          return done();
        }
        createNode(nodePath, zookeeper.CreateMode.EPHEMERAL, function (err, path) {
          if (!err && path) {
            mainCluster.node = path;
          }
          done(err);
        });
      },
      function (done) {
        if (mainCluster.observerOnly) { 
          logger.debug('2. create vote node : skip in observerOnly mode');
          return done(); 
        }

        logger.debug('2. create vote node');
        createNode(TICKET_PATH, zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL, 
          new Buffer(PATH.basename(nodePath)), // node name
          function (err, path) {
          if (!err && path) {
            mainCluster.ticket = PATH.basename(path);
          }
          done(err);
        });
      },
      function (done) {
        logger.debug('3. start watch');
        if (!BASE_PATH) {
          logger.debug('skip watchAll() on missing basePath');
          return done();
        }
        mainMonitor.on('children', function (path, newVal/*, diff*/) {
          if (BASE_PATH && _.contains(path, BASE_PATH)) {
            mainCluster[PATH.basename(path)] = newVal;
          } else {
            logger.error('on children: unknown [%s]=%s', path, newVal.toString());
          }
        });
        mainMonitor.on('data', function (path, newVal, oldVal) {
          if (BASE_PATH && path === BASE_PATH) {
            mainCluster.master = newVal.master;
          }
        });

        watchAll(BASE_PATH, mainMonitor, mainCluster.observerOnly, done);
      },
      function (done) {
        logger.debug('3.5. start watch config');
        if (!CONFIG_PATH) {
          logger.debug('skip _watchData(config) on missing configPath');
          return done();
        }
        
        mainMonitor.on('data', function (path, newVal, oldVal) {
          if (CONFIG_PATH && path === CONFIG_PATH) {
            //logger.warn('config change=', newVal, oldVal);
            if (oldVal && !opt.noRestartOnConfigChange) {
              appRestart(-1, 'zk config change');
              logger.fatal('Restart on zk config change, newval=', newVal);
            } else {
              mainCluster.config = newVal;
            }
          }
        });

        _watchData(CONFIG_PATH, mainMonitor, done);
      },
      function (done) {
        if (mainCluster.observerOnly) { 
          logger.debug('4. wait or be a master: skip in observerOnly mode');
          return done(); 
        }

        logger.debug('4. wait or be a master');
        function _waitMaster() {
          function __doneRemoveListener(err) {
            mainMonitor.removeListener('children', _waitMaster);
            mainMonitor.removeListener('data', _waitMaster);
            return done(err);
          }
          //logger.debug('[_waitMaster]cluster=%j', mainCluster);
          if (isNo1()) {
            if (isMaster()) {
              logger.info('[wait Master] already MASTER');
              return __doneRemoveListener(); // me alone
            } else {
              return setMaster(mainCluster.node, function (err) {
                if (err) {
                  logger.error('[wait Master] fail to setMaster()');
                } else {
                  logger.info('[wait Master] set as a MASTER');
                }
                return __doneRemoveListener(err);
              });
            }
          } else { // it's master but not No.1
            if (getMaster() && mainCluster.master === PATH.basename(mainCluster.node)) {
              logger.info('not No.1 but master, waiting...', mainCluster);
              return;
            }
          }
          if (_.isEmpty(getMaster())) {
            logger.info('[wait Master] NO MASTER, waiting...', mainCluster);
            return;
          } else {
            logger.info('[wait Master] FOUND MASTER %j', getMaster());
            return __doneRemoveListener();
          }
        }

        mainMonitor.on('children', _waitMaster);
        mainMonitor.on('data', _waitMaster);
        _waitMaster();
      },
      function (done) {
        if (mainCluster.observerOnly) { 
          logger.debug('5. listen master change: skip in observerOnly mode');
          return done(); 
        }
        logger.debug('5. listen master change');

        masterAfterInit = getMaster();
        function _waitMasterChange() {
          if (!isMaster()) {
            if (isNo1()) {
              mainMonitor.removeListener('children', _waitMasterChange);
              mainMonitor.removeListener('data', _waitMasterChange);
              return appRestart(0, '[watch master]restart due to no1');
            }
            if (!_.isEqual(masterAfterInit, getMaster())) {
              logger.info('[watch master]MASTER change: %j -> ', masterAfterInit, getMaster());
              mainMonitor.removeListener('children', _waitMasterChange);
              mainMonitor.removeListener('data', _waitMasterChange);
              return appRestart(0, '[watch master]restart due to master change');
            }
          }
        }
        mainMonitor.on('children', _waitMasterChange);
        mainMonitor.on('data', _waitMasterChange);

        return done();
      }
    ],
    function (err) {
      if (err) {
        logger.error('init error', err);
        appRestart(-1, 'init error'); // restart
      } else {
        zkStatus = 'initialized';
        if (mainCluster.observerOnly) {
          logger.warn('[%s] init done', 'OBSERVER');
        } else {
          logger.warn('[%s] init done', isMaster() ? 'MASTER' : 'SLAVE');
        }
        logger.info('mainCluster', JSON.stringify(mainCluster, null, 2));
      }
      return cb(err, client);
    });
  });

  client.connect();
}

function deinit(cb) {
  zkStatus = 'deinitialized';
  client.close();
  return cb && cb();
}

//basePath or options: basePath, dataOnly(default false)
function Observer(basePathOrOption) {
  Monitor.call(this);

  var self = this,
    dataOnly = false;

  if (_.isObject(basePathOrOption)) {
    this.basePath = basePathOrOption.basePath;
    dataOnly = !!basePathOrOption.dataOnly;
  } else {
    this.basePath = basePathOrOption;
  }

  this.cluster = {};

  this.on('data', function (path, newVal/*, oldVal*/) {
    if (path === self.basePath) {
      self.cluster.master = newVal.master;
    } else {
      logger.error('on data: unknown [%s]=%s', path, JSON.stringify(newVal));
    }
  });

  if (dataOnly) {
    _watchData(this.basePath, this);
    return;
  } else {
    this.on('children', function (path, newVal/*, diff*/) {
      if (_.contains(path, self.basePath)) {
        self.cluster[PATH.basename(path)] = newVal;
      } else {
        logger.error('on children: unknown [%s]=%s', path, newVal.toString());
      }
    });
    watchAll(this.basePath, this, true /*ObserverOnly*/);
    return;
  }
}
util.inherits(Observer, Monitor);

Observer.prototype.getMaster  = function () {
  return getMaster(this.cluster);
};

Observer.prototype.getSlaves  = function () {
  return getSlaves(this.cluster);
};

Observer.prototype.getNodes  = function () {
  return getNodes(this.cluster);
};

module.exports.init = init;
module.exports.deinit = deinit;
module.exports.isMaster = isMaster; 
module.exports.getMaster = getMaster;
module.exports.getSlaves = getSlaves;
module.exports.getNodes = getNodes;
module.exports.getConfig = getConfig;
module.exports.monitor = mainMonitor;
module.exports.Observer = Observer;
module.exports.createNode = createNode;
module.exports.removeNode = removeNode;
module.exports.CreateMode = zookeeper.CreateMode;
module.exports.Exception = zookeeper.Exception;

if (require.main === module) { // run directly from node
  var opt = {};
  if (process.argv[2] === '-o') {
    opt.node = 'observer';
    opt.observerOnly = true;
    //TODO: read servers, basePath from config
  } else {
    opt.node = process.argv[2];
    opt.observerOnly = false;
    //TODO: read servers, basePath from config
  }

  init(opt, function (err) {
    if (err) {
      logger.info('zk init error', err);
    } else {
      logger.info('zk init done');
    }
    if (opt.observerOnly === true) {
      var observer = new Observer('/otherApp');
      observer.on('children', function (path, newVal, diff) {
        logger.info('Master=%j', observer.getMaster());
        logger.info('[%s] children:[%s] added=[%s] deleted=[%s]', path, newVal, diff.added, diff.deleted);
        //logger.info('cluster', observer.cluster);
      });
      observer.on('data', function (path, newVal, oldVal) {
        logger.info('Master=%j', observer.getMaster());
        logger.info('[%s] data: %j <- %j', path, newVal, oldVal);
        //logger.info('cluster', observer.cluster);
      });
    }
  });
}
