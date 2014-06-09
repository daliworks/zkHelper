zkHelper
========

Zookeeper helper utility for leader election, observe node change, and configuration change etc. 

For instance  
   - init() options:
     - basePath: 'myapps'
     - node: myhostname:123
   - every apps in this leader election will restarted repeatedly
   using forever, nodemon and etc.

zookeeper nodes structure looks like:
```
  /myapps
        /nodes/myhostname:123
        /votes/n_00001
```  

/myapps has the elected master.
```json
{
    "master": "myhostname:123"
}
```  

 - When myapp calls init(), new node is create below /myapps/nodes. master node is elected
 using an voting ticket below /myapps/votes.

 - On master change, every apps(including the master) participated in this leader
 election will restart.
 The master will not be changed after restart.

 - Any connection failure to zookeeper leads process.exit()
   - A temporary disconnection is ignored one time and check
   again after 5 secs(one tick(2sec) * 2 + 1 sec)


## init(options, callback)
   *  options
     *  basePath: zk basePath, optional if observerOnly
     *  node: host [':' + port], optional if observerOnly
     *  servers: zookeeper servers
     *  configPath: optional,
     *  logger: console(default)
     *  clientOptions: zookeeper client options
     *  observerOnly: false(default), true: no voting, read status change only
   * callback: err, client(created zk client)

# master selection mode(default)

## getMaster()
  * returns current master.

## isMaster()
  * returns ture if current app is master

## getConfig()  
  * returns configuration object
  * configuration node path is provided via init ```options.configPath```

Example

```javascript
var zk = require('zkHelper'),
options = {
  basePath: '/myapps';
  configPath: '/myapps/config',
  node: require('os').hostname(),
  servers: ['zk0:2181', 'zk1:2181', 'zk2:2181'], // zk servers
  clientOptons: {
    sessionTimeout: 10000,
    retries: 3
  }
 };
 zk.init(options, function (err, zkClient) {
   var appConfiguration;
   if (zk.isMaster()) {
     console.info('i am master')
   } else {
     console.info('master', zk.getMaster() && zk.getMaster().master)
   }
   appConfiguration = zk.getConfig();
   // do something
 });
```

#observer mode

 - init() with ```options.observerOnly``` : true
 - Do not participate in leader election, and observe status changes only.
 - data event is for master selection info. children event for node list changes.


## event
### on('data', path, newVal, oldValue)
 - master change event
 - ```newVal```: master info, {master: node_name:port }
 - ```oldValue```: null if first time retrieval.

### on('children', path, newVal, diff)
 - children change event
 - ```newVal```: node list
 - ```diff.added```: added node list
 - ```diff.deleted```: added node list
 - use _.debounce to get settle downed node list.

Example

```javascript
var zk = require('zkHelper'),
_ = require('lodash'),
options = {
  servers: ['zk0:2181', 'zk1:2181', 'zk2:2181'], // zk servers
  clientOptons: {
    sessionTimeout: 10000,
    retries: 3
  },
  observerOnly: true
 };
zk.init(options, function (err, zkClient) {
  var observer = new Observer('/otherApp');

  observer.on('children', _.debounce(function (path, newVal, diff) {
    consol.info('Master=%j', observer.getMaster());
    logger.info('[%s] children:[%s] added=[%s] deleted=[%s]', path, newVal, diff.added, diff.deleted);
  }, 500));
  observer.on('data', function (path, newVal, oldVal) {
    if (oldVal) {
      logger.info('master change:' + path);
    }
    logger.info('Master=%j', observer.getMaster());
    logger.info('[%s] data: %j <- %j', path, newVal, oldVal);
  });
});
```
