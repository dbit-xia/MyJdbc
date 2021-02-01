
'use strict';
const util=require('util');
// let logTime=true;
const _ = require('lodash');
// let ResultSet=require('./resultset'); //重写resultset的方法
// const Thenjs=require("thenjs");
// const SqlHelper=require('./SqlHelper');
let genericPool=require('runsa-generic-pool');
let TimeoutError=require('runsa-generic-pool/lib/errors').TimeoutError;
const path=require('path');
let MyUtil=require('./MyUtil');
const crypto=require('crypto');
let logger=global.logger || console;
const events=require('events');
// let emitter=new events.EventEmitter();
// let java = jinst.getInstance();
const moment=require('moment');
const nuid=require('nuid');
const java = require('java');
const poolDebug0 = require('debug')('myjdbc:pool');
/**
 * JDBC数据类型常量
 */
const SQL_TYPES= {
    BOOLEAN: 16,
    DATE: 91,
    NUMERIC: 8,
    BIGDECIMAL: 3,
    DOUBLE: 8,
    FLOAT: 8,
    INTEGER: 4,
    INT: 4,
    VARCHAR: 12,
    CHAR: 12,
    STRING: 12,
    NUMBER: 8,
    DECIMAL: 8,
    DATETIME: 12,
    TIME: 12,
    TEXT: 12
};

java.asyncOptions = {
  asyncSuffix: undefined,     // Don't generate node-style methods taking callbacks
  syncSuffix: 'Sync',              // Sync methods use the base name(!!)
  promiseSuffix: "",   // Generate methods returning promises, using the suffix Promise.
  promisify: require('util').promisify // Needs Node.js version 8 or greater, see comment below
};

/*结构:
 MyPool.Conn
 mypool.pool._pool
 conn.pool=new MyPool(mypool)
 conn.conection._conn
 * */
/**
 *
 * jconn4 支持setFetchSize,成功后则调用resultset.next时分批取数据 ,否则executeQuery时已将数据全部取出
 * 服务器:先跳过skiprow,先取出一个,在此基础上再提取Fetchrow - 1, 然后取出Fetchrow 直到maxrows
 * jtds 不支持setFetchSize,客户端:先取出maxrows指定的行数,再跳过指定行数
 * rowcount = maxrows - skiprow
 * "url": "jdbc:sybase:Tds:wgz.runsa.cn:52250/rts_xjjx?CHARSET=cp936&APPLICATIONNAME=runsa_wechat",//分销数据库连接信息
 * "drivername": "com.sybase.jdbc4.jdbc.SybDriver",
 * url: 'jdbc:jtds:sybase://wgz.runsa.cn:52250;databasename=rts_xjjx;charset=cp936;appName=runsa_wechat;',
 * "url": "jdbc:sybase:Tds:wgz.runsa.cn:51940/rts_lbl?CHARSET=cp936&APPLICATIONNAME=runsa_interface",//分销数据库连接信息
 * "drivername": "com.sybase.jdbc4.jdbc.SybDriver",
 */

/**
 *
 * @param jarList {[string]}
 */
function MyJdbc(jarList){
    java.options.push("-Xrs");
    const classpaths = _.filter(jarList.concat([
        path.join(__dirname, "../java/classes"),
        path.join(__dirname, "../java/libs/commons-codec-1.12.jar"),
        path.join(__dirname, "../java/libs/commons-collections4-4.3.jar"),
        path.join(__dirname, "../java/libs/commons-compress-1.18.jar"),
        path.join(__dirname, "../java/libs/commons-math3-3.6.1.jar"),
        path.join(__dirname, "../java/libs/curvesapi-1.06.jar"),
        path.join(__dirname, "../java/libs/hutool-log-4.6.1.jar"),
        path.join(__dirname, "../java/libs/hutool-poi-4.6.1.jar"),
        path.join(__dirname, "../java/libs/hutool-core-4.6.1.jar"),
        path.join(__dirname, "../java/libs/minimal-json-0.9.5.jar"),
        path.join(__dirname, "../java/libs/poi-4.1.0.jar"),
        path.join(__dirname, "../java/libs/poi-ooxml-4.1.0.jar"),
        path.join(__dirname, "../java/libs/poi-ooxml-schemas-4.1.0.jar"),
        path.join(__dirname, "../java/libs/protobuf-java-3.6.1.jar"),
        path.join(__dirname, "../java/libs/xmlbeans-3.1.0.jar"),
        path.join(__dirname, "../java/libs/fastjson-1.2.58.jar"),
        path.join(__dirname, "../java/libs/sqlite-jdbc-3.7.2.jar"),
    ]), (classpath) => {
        return (java["classpath"].includes(classpath) === false);
    });
    if (classpaths.length) java.classpath.push.apply(java.classpath, classpaths);
}

async function getShortError(err) {
    if (!err) return err;
    if (err.hasOwnProperty('sqlCode')) return err;
    
    const shortError = {sqlCode: -1};
    
    if (_.get(err, 'cause.getErrorCode') == null) { //不能用_.has
        shortError.Message = MyUtil.String(err);
        return shortError;
    }
    
    await Promise.all([err.cause.getErrorCode().then((result) => {
        shortError['ErrorCode'] = result;
    }), err.cause.getMessage().then((result) => {
        shortError['Message'] = result;
    }), err.cause.getSQLState().then((result) => {
        shortError['SQLState'] = result;
    }), err.cause.getCause().then((result) => {
        shortError['Cause'] = result;
    })]);
    return shortError;
}

/**
 *
 */
class MyPool extends events{
    /**
     *
     * @param config
     * @returns {*}
     */
    constructor(config) {
        if (_.isEmpty(config)) throw Error("未定义数据库连接参数:" + MyUtil.String(config));
        
        let ret = MyUtil.checkNull(config,["dbms","url","drivername"]);
        if (ret) throw ret;
        
        super();
        //noinspection JSUnresolvedVariable
        this.emitter = this;
        let _config = _.cloneDeep(config);
        this._config = _config;
        // this.onSuccess=null; //(conn,cb)=>cb();
        /**
         * 
         * @type {Map<String, MyConnection>}
         * @private
         */
        this._connectionMap = new Map();
        
        /**
         * 
         * @type {Map<String, {session,sessionKeys:[String]}>}
         * @private
         */
        this._pendingMap = new Map();
        /**
         *
         * @type {Set<MyConnection>}
         * @private
         */
        this._borrowedSet = new Set();
        
        //noinspection JSUnresolvedVariable
        this._pool = null;
        this.created = moment().format('YYYY-MM-DD HH:mm:ss');
        //noinspection JSUnresolvedVariable
        this.logger = console;
        this._status = null; //'连接池还未初始化!';
        this._lastError = null;
        this.connectionMaxPendingNum = 0;
        this.connectionMaxPending = null;
        this.retryOpenHandle = null;
        this.poolDebug = (...args) => {
            poolDebug0.apply(null, [moment().format('YYYY-MM-DD HH:mm:ss'), this.group].concat(args))
        }
    
        let {encrypt} = _config;
        if (encrypt) { //使用加密
            let logpass = process.env.logpass;
            // console.debug(logpass);
            if (!logpass) {
                this.logger.warn('请先输入启动密钥!');
            } else {
                let password = _.get(_config, 'properties.password');
                // console.debug(password);
                //解密
                try {
                    let key = Buffer.from(logpass);
                    let iv = Buffer.alloc(0);
                    let decipher = crypto.createDecipheriv(encrypt, key, iv);
                    password = decipher.update(password, 'base64', 'utf8');
                    password += decipher.final('utf8');
                } catch (e) {
                    logger.error('启动密钥错误:export logpass=' + logpass, e);
                    return;
                    //throw e.stack='启动密钥不正确:'+logpass+'\n'+e.stack;
                }
                // console.debug(password);
                //使用新密码
                _.set(_config, 'properties.password', password);
                _.set(_config, 'encrypt', '');
            }
        }
        let dbms = _.get(this, '_config.dbms', 'ase');
        this.adapter = require('./adapter/' + dbms + '/index');
        this.sqlHelper = this.adapter.sqlHelper;
        this._spidSql = this.adapter.getSpidSql();
        this.lastConnection = null;
    
        this.emitter.on('pool', ({type, costTime, error, result}) => {
            this.logger.debug('pool', MyUtil.toLine({type, costTime, error, result}));
        });
        
        this.emitter.on('connection', ({type, sid, connection, error, costTime, requestId}) => {
            if (!sid && connection) sid = connection.sid;
            this.logger.debug('connection', MyUtil.toLine({type, sid, error, costTime, requestId}));
        });
        return this;
    }

    get status() {
        return this._status;
    }
    /**
     *
     * @returns {{status, pool, reserve, pending, max, min, size}|*}
     */
    getStatus() {
        return getPoolStatus(this);
    }

    showConnections() {
        return showConnections(this);
    }
    
    /**
     * @param option {{borrowed,running,[spid]:String}}
     * @returns {{status, pool, reserve, pending, max, min, size}|*}
     */
    getAllInfo(option) {
    
        let response = this.getStatus();
    
        let {borrowed, running, spid} = option || {};
        borrowed = MyUtil.toBoolean(borrowed, 'default'); //连接的借用状态
        running = MyUtil.toBoolean(running, 'default'); //连接的执行状态
        let connections = []; //所有连接
        
        (borrowed === true ? this._borrowedSet : this._connectionMap).forEach((conn) => {
            if (spid && String(spid) !== String(conn.spid)) return;
            let result = conn.getStatus();
        
            if (borrowed === true) {
                if (!result.borrowed) return;
            }else if (borrowed === false) {
                if (result.borrowed) return;
            }
            if (running === true) {
                if (result.activeSqls.length === 0) return;
            } else if (running === false) {
                if (result.activeSqls.length !== 0) return;
            }
            
            connections.push(result);
        });
    
        response.connections = connections;
    
        let pendings = []; //挂起的会话
        this._pendingMap.forEach((row) => {
            pendings.push(Object.assign({
                time: MyUtil.DateTimeString(new Date(row.time)),
                waitMs: Date.now() - row.time
            }, _.pick(row.session, row.sessionKeys)));
        });
        response.pendingList = pendings;
    
        // let borrowedList = []; //已借用的连接
        // this._borrowedSet.forEach((conn) => {
        //     borrowedList.push(conn.getStatus());
        // });
        // result.borrowedList = borrowedList;
        //
        return response;
    }
    
    /**
     * 
     * @param conn
     * @param [cb]
     * @return {Promise<*>}
     */
    async release (conn,cb) {
        return this.releaseConn(conn, cb);
    }

    /**
     *
     * @param [cb]
     * @returns {*}
     */
    open(cb) {
        if (this._status === 'success') return cb && cb(); //已经打开功能

        let tempStatus = 'starting';
        cb && this.emitter.once('end', cb); //启动后再调用
        if (this._status === tempStatus) return;
        this._status = tempStatus; //'正在初始化连接池,请稍候...';
        // this.emitter.emit('starting',{status:tempStatus});

        let _this = this;
        let _config = this._config;
        const factory = {
            create: async function () {
                const sid = nuid.next();
                const start_create = new Date().getTime();
    
                _this.emitter.emit('connection', {type: 'createStart', sid});// console.debug('createConnection');
    
                let conn;
    
                function fin(err, conn) {
                    let costTime = ((new Date().getTime()) - start_create);
        
                    if (err) {
                        _this.logger.error(err);
                        _this.emitter.emit('connection', {type: 'createError', sid, error: err, costTime});// console.debug('createConnection');
                        if (conn) factory.destroy(conn).catch((e) => _this.logger.error(e));
                        return Promise.reject(err);
                    }
                    _this.emitter.emit('connection', {type: 'createSuccess', sid, connection: conn, costTime});// console.debug('createConnection');
                    return conn;
                }
    
                try {
                    // _this.poolDebug('createConnection');
                    /**
                     *
                     * @type {MyConnection}
                     */
                        // conn = await MyJdbc.createMyConnection({pool: _this,sid});
                        // let pool = config.pool;
                        // if (pool) config = pool._config;
                    let jdbcConnection = await _createConnection(_this._config);
                    conn = new MyConnection({pool: _this, connection: jdbcConnection, sid});
        
                    // conn["sid"] = sid;
                    _this._connectionMap.set(sid, conn);
        
                    conn.logger = _this.logger;
                    // conn.pool = _this;
        
                    // await conn.Close();
                    let isClosed = await conn._isClosed();
                    if (isClosed) {
                        let err = await getShortError('连接已关闭').catch(err => err);
                        return fin(err);
                    }
                    conn._status = 'ok';
                    conn.created = conn.getdate(); //创建时间
                    showLog(conn, 0, `spid:${conn.spid},getdate:${conn.getdate()}`);
                    await conn.setOption();
                    conn._autoCommit = await conn.getAutoCommit();
    
                    conn.once('closed',()=> {
                        if (conn.borrowed) {
                            _this.releaseConn(conn).catch(e => conn.logger.error(e));
                        }
                    });
                    
                    return fin(null, conn);
                } catch (err) {
                    err = await getShortError(err).catch(err => err);
                    return fin(err, conn);
                }
            },
            destroy: async function (conn) {
                let sid = conn.sid;
                _this.emitter.emit('connection', {type: 'closeStart', sid, connection: conn});// console.debug('createConnection');
                try {
                    await conn.Close();
                    _this.emitter.emit('connection', {type: 'closeSuccess', sid, connection: conn});// console.debug('createConnection');
                } catch (e) {
                    _this.emitter.emit('connection', {type: 'closeError', sid, connection: conn, error: e});// console.debug('createConnection');
                }
                _this._connectionMap.delete(sid);
                return null;
            },
            validate: async function (conn) {
                _this.poolDebug('validateConnection');
                return !(await conn._isClosed());
            }
        };

        let opts = {
            fifo: false, //优先使用最新获取的连接
            maxWaitingClients: 10, //最大排队数
            testOnBorrow: true, //获取连接时自动检查状态
            idleTimeoutMillis: 600000, //设置10分钟认为是空闲进程
            //numTestsPerRun:3,
            acquireTimeoutMillis: 3000, //获取连接,20秒超时
            evictionRunIntervalMillis: 10000, //每10s检查一次空闲连接
            max: _config.maxpoolsize, // maximum size of the pool
            min: _config.minpoolsize, // minimum size of the pool
            autostart: false
        };

        //libuv 线程池大小,默认为4
        let THREADPOOL = (Number(process.env.UV_THREADPOOL_SIZE) || 4);
        if (opts.max > THREADPOOL) {
            logger.warn('连接池(' + opts.max + ')大于线程池(' + THREADPOOL + '),建议调整变量UV_THREADPOOL_SIZE(>=maxPool,<=1024)');
        }

        /**
         * @type {{on:function(string,Function),available,borrowed,min,max,acquire:function(number),release:function,_waitingClientsQueue}}
         */
        let pool = genericPool.createPool(factory, opts);
        _this._pool = pool; //.__proto__

        let createErrorCount = 0;
        pool.on('factoryCreateError', async function (err) {
            createErrorCount++;
            // console.log(createErrorCount);
            err = _this["group"] + ' ' + MyUtil.DateTimeString(new Date()) + ':\n' + MyUtil.String(err);
            if (err && err.cause && err.cause.getMessage) {
                err = (await err.cause.getMessage()) || err;
            }
            err = _this["group"] + ':' + moment().format('YYYY-MM-DD HH:mm:ss') + '\n' + MyUtil.String(err);
            _this._lastError = err;
            _this.logger.error('factoryCreateError', err);
            // throw err;
            // let dequeue = pool._waitingClientsQueue.dequeue();
            // if (_.has(dequeue, 'reject')) dequeue.reject(err); //从排队中移除一个,避免bug死循环
            // logger.error(err);
            // throw err;
        });

        let _started = false;

        function start() {
            createErrorCount = 0;
            if (_started === false) {
                pool.start();
                _started = true;
            } else {
                pool._ensureMinimum(); //初始化最小连接池
            }
            _this.poolDebug('开始初始化...');
            //等待初始化完成
            let handle = setInterval(() => {
                _this.poolDebug('等待初始化完成...', pool.available + pool.borrowed + createErrorCount, pool.min);
                if ((pool.available + pool.borrowed + createErrorCount) >= pool.min) {
                    clearInterval(handle);

                    if (createErrorCount) {
                        _this._status = 'failed';
                        _this.logger.error("连接池初始化失败:", JSON.stringify(_this.getStatus()));
                        // cb && cb(_this._lastError, pool);
                        _this.emitter.emit('end', _this._lastError);

                        const delay = 60;
                        _this.poolDebug('初始化失败,等待(' + delay + 's)重试...');
                        _this.retryOpenHandle = setTimeout(() => {
                            start();
                        }, delay * 1000); //循环监测,避免网站启动时连接数据库失败

                    } else {
                        _this.poolDebug('初始化成功');
                        _this._status = 'success';
                        _this.logger.info("连接池初始化成功:", JSON.stringify(_this.getStatus()));
                        _this.emitter.emit('end', null);
                        // if (_this.onSuccess){
                        // _this.onSuccess(cb);
                        // }else{
                        //     cb && cb(null, pool);
                        // }
                    }
                }
            }, 100);
        }

        start();
    }

    async drain() {
    
        if ('drained' === this._status) {
            return "当前连接池状态已经释放:" + this._status;
        }
    
        if ('draining' === this._status) {
            return "当前连接池状态正在释放:" + this._status;
        }
    
        this._status = 'draining';
    
        if (this.retryOpenHandle) {
            clearTimeout(this.retryOpenHandle);
            this.retryOpenHandle = null;
        }
    
    
        let errors, result;
        this.emitter.emit('drain', {}); //用于通知其它事件正常退出
        let time1 = Date.now();
        await this._pool.drain().then(v => (result = v)).catch(e => (errors = e));
    
        if (!errors) this._status = 'drained'; 
        this.emitter.emit('pool', {type: 'drain', costTime: Date.now() - time1, errors, result});
        if (errors) return Promise.reject(errors);
        
        await this.clear();
        return this._status;
    }
    
    /**
     * 
     * @param sid
     * @return {Promise<MyConnection>}
     */
    async findConnection({sid}) {
        let conn = this._connectionMap.get(sid);
        if (!conn) return Promise.reject('连接不存在:' + sid);
        return conn;
    }
    
    async clear() {
        let time1 = Date.now();
        let errors, result;
        await this._pool.clear().then(v => (result = v)).catch(e => (errors = e));
    
        this.emitter.emit('pool', {type: 'clear', costTime: Date.now() - time1, errors, result});
        if (errors) return Promise.reject(errors);
    
        return result;
    }
}

MyPool.prototype.open=MyUtil.toAsync(MyPool.prototype.open);
MyPool.prototype._pool=null;

//Main.prototype.getPoolStatus=function (){
//    let pool=this.pool;
//    return getPoolStatus(pool);
//};

/**
 * 从连接池中获取一个连接
 * @param session {{requestId}}
 * @param option {{sessionKeys}}
 * @returns {*}
 */
MyPool.prototype.getConnection=async function (session,option) {
    
    let sessionKeys=[];
    if (option) sessionKeys = option.sessionKeys;
    
    // let {requestId} = session || {};
    // if (!requestId) requestId = 'r_' + nuid.next();
    let requestId = 'r_' + nuid.next();
    let _this = this;
    // let pool = _this.pool;
    
    if (!_this._pool) {
        let err = new Error('连接池未初化!');
        _this.logger.error('getConnection', err);
        return Promise.reject(err);
    }
    
    let time1 = Date.now();
    _this.emitter.emit('connection', {type: 'borrowStart', requestId, session});
    
    this._pendingMap.set(requestId, {time: Date.now(),session,sessionKeys:sessionKeys});
    
    const fin = (err, conn) => {
        
        this._pendingMap.delete(requestId);
        
        if (err) {
            _this.emitter.emit('connection', {
                type: 'borrowError',
                requestId,
                error: err,
                costTime: Date.now() - time1
            });
            return Promise.reject(err);
        }
    
        this._borrowedSet.add(conn);
        
        _this.emitter.emit('connection', {
            type: 'borrowSuccess',
            requestId,
            connection: conn,
            costTime: Date.now() - time1
        });
        return conn;
    }
    
    
    try {
        let conn = await _this._pool.acquire(0);
        if (session) conn.setAppInfo(session, sessionKeys);
    
        conn._requestId = requestId;
        // _this.emitter.emit('acquire', conn);
        // conn._reset(); //release时调用
        conn.borrowed = (new Date()).getTime();
        conn._status = 'ok';
        conn.actived = (new Date()).getTime();
    
        let holdTimeout = _.get(conn, 'pool._config.holdTimeout', (2 * 60 * 1000)); //默认两分钟
        conn.intervalHandle = setInterval(async () => {
            if ((new Date()).getTime() - conn.actived > holdTimeout) {
                clearInterval(conn.intervalHandle);
                conn.intervalHandle = null;
            
                const lastSQL = conn.lastSQL; //写到Close前,避免Close改lastSQL
                try {
                    let err = {
                        code: -1,
                        msg: '由于长时间没有活动连接已被断开:' + holdTimeout + ',' + lastSQL
                    };
                    conn.logger.warn(conn.spid, 'forceClose', err);
                    await conn.Close();
                } catch (e) {
                    conn.logger.error(conn.spid, 'forceClose', e);
                }
            }
        }, 1000);
    
        this.lastConnection = conn;
    
        return fin(null, conn);
    } catch (err) {
        if (err instanceof TimeoutError) {
            err = _this.showConnections();
            err.sqlCode = -1;
            let waitTime = (_this._pool._config.acquireTimeoutMillis / 1000).toFixed(1) + 's';
            err.Message = '获取连接超时(' + waitTime + '),请稍候再试!';
            err.lastError = _this._lastError;
            // return Promise.reject(err);// return cb(err);
        }
        // _this.logger.error('getConnection',err);
        
        return fin(err);
    }
};
// MyPool.prototype.getConnectionAsync=util.promisify(MyPool.prototype.getConnection);

/**
 * 归还一个连接
 * @param conn {MyConnection}
 * @param [cb] {function}
 * @return {*}
 */
MyPool.prototype.releaseConn=async function (conn, cb) {
    if (cb) return this.releaseConn(conn).then((result) => cb(null, result)).catch(cb);
    
    let {_requestId} = conn;
    this.emitter.emit('connection', {type: 'release', connection: conn, requestId: _requestId});
    
    let _this = this;
    // let pool = _this.pool;
    
    // showSql(conn,'Release');
    // conn._status=null;
    conn.removeAllListeners('commit');
    if (conn.intervalHandle) {
        clearInterval(conn.intervalHandle);
        conn.intervalHandle = null;
    }
    
    let {borrowed} = conn;
    let {query, execute, commit, rollback, set, other} = conn.sqlSum;
    conn.setAppInfo(null);
    conn._reset();
    await _this._pool.release(conn); //.then(function(){
    // _this.emitter.emit('release', conn["sid"]);
    _this._borrowedSet.delete(conn);
    showLog(conn, 0, 'release:' + (new Date().getTime() - borrowed) + 'ms query:' + query + ' execute:' + execute + ' commit:' + commit + ' rollback:' + rollback + ' set:' + set + ' other:' + other);
    return null;
    //     cb()
    // }).catch(function(err){
    //     cb(err);
    // });
    //pool.release(conn.connection,cb);
};
// MyPool.prototype.releaseConnAsync=util.promisify(MyPool.prototype.releaseConn);

/**
 * 连接数据库,cb是处理数据的函数,cb2是输出函数
 * 执行第一个回调函数(err,conn,(err,result,iscommit)=>{})-->提交-->再回调cb2(err,result)进行输出
 */
MyPool.prototype.autoConnect=function() {
    throw new Error('autoConnect为弃用的方法,请改用getConnection!');
    // // let isNew=true; /*默认为用新连接*/
    // let pool=this;
    // this.getConnection().then((conn)=>{
    //     connCallback(null,conn);
    // }).catch((err)=>{
    //     output && output(err);
    // });
    //
    // /**
    //  *
    //  * @param err
    //  * @param conn {MyConnection}
    //  * @returns {*}
    //  */
    // function connCallback(err, conn) {
    //     if (err) {
    //         output && output(err);
    //         return;
    //     }
    //
    //     Thenjs((cont) => {
    //         conn.setAutoCommit(false, cont);
    //     }).then((cont) => {
    //         fn(conn, cont);
    //     }).fin((cont, err0, result) => {
    //         conn._status = 'releasing';
    //         if (err0) { //存在错误回滚
    //             conn.Rollback((err2) => {
    //                 cont(err0 != null ? err0 : (err1 || err2));
    //             });
    //         } else {
    //             conn.Commit((err2) => {
    //                 cont(err2, result);
    //             });
    //         }
    //     }).fin((cont, err, result) => {
    //         return pool.releaseConn(conn).then(() => {
    //             output && output(err, result);
    //         }).catch((err2)=>{
    //             output && output(err != null ? err : err2);
    //         });
    //         // }
    //         // clearInterval(intervalHandle);
    //         // output && output(err, result);
    //     });
    // }
};
// MyPool.prototype.autoConnectAsync=util.promisify(MyPool.prototype.autoConnect);

class MyConnection extends events{
    /**
     *
     * @param option {{sid:string,pool:MyPool,connection:Connection}}
     */
    constructor(option) {
        
        let ret = MyUtil.checkNull(option,['sid','pool','connection']);
        if (ret) throw ret;
        
        super();
        
        if (!option) option={};
        this.option = option;
        //noinspection JSUnresolvedVariable
        /**
         *
         * @type {MyPool}
         */
        this.pool = option.pool;
        // this.Title = option.title;
        this.sid = option.sid;
        //连接后初始化的变量
        //noinspection JSUnresolvedVariable
        // this.logger = null; //pool.logger
        this._connection = option.connection;//=?PoolConnection();
        //noinspection JSUnresolvedVariable
        this._status = null;
        this.spid = null; //{Number}
        this.getdate = null; //{String}
        this.created = moment().format('YYYY-MM-DD HH:mm:ss');
        this.actived = (new Date()).getTime();
    
        this._autoCommit = false;
        this._numberPrecision = false;
    
        /**
         *
         * @type {Set<{sql,type,params}>}
         * @private
         */
        this._sqlSet = new Set();
    
        this._reset();
    
        /**
         * {{handle:{type:(execute|query|commit|rollback), sql, param}}
         */
        this.on('execute',(handle, {step, result, error, message}) => {
            if (step === 'start') {
                this._sqlNum++;
                handle.sqlNo = this._sqlNum;
    
                let {type, sql, param, sqlNo} = handle;
                handle.time = Date.now();
                this.actived = handle.time;
                this.lastSQL = sql;
                
                this.sqlSum[type]++;
    
                this._sqlSet.add(handle);
    
                if (['commit', 'rollback', 'set'].includes(type)) {
        
                } else {
                    // console.error(sql);
                    showSql(this, sqlNo, (sql || type));
                    if (param) showLog(this, sqlNo, 'params', (param || ''));
                }
            } else {
                let {time, type, sqlNo} = handle;
                this._sqlSet.delete(handle);
    
                showLog(this, sqlNo, (['set', 'commit', 'rollback'].includes(type) ? ((handle.sql || type) + ' ') : '') + (message || MyUtil.String(error || result)) + ' ' + String(Date.now() - time) + 'ms');
            }
        });
    
        return this;
    }

    _reset() {
        //归还连接后,需还原的变量列表
        if (['closed'].includes(this._status) === false) {
            this._status = null;
        }
        this.pendingNum = 0;
        this.lastSQL = "";
        this.session = null; //Object
        this._sessionKeys = null; //Array
        this.postEvents = null; //Array
        this.borrowed = null; //time
        this.sqlSum = {set: 0, query: 0, execute: 0, commit: 0, rollback: 0, other: 0};
        this._sqlNum = 0;
        this.logger = _.get(this.pool, 'logger') || logger;
    }
    
    /**
     *
     * @private
     */
    async _isClosed() {
        let conn=this;
        if (conn._status === 'closed') return true;
        try {
            let result = await conn.QueryGrid(conn.pool._spidSql);//.then((result) => {
            conn.spid = result[0][0];
            const a = new Date(result[0][1]);
            const b = new Date();
            const diffSSS = a - b; // 86400000
            conn.getdate = () => {
                return MyUtil.DateTimeString(new Date(new Date().getTime() + diffSSS));
            };
            return false;
        } catch (e) {
            console.error(e);
            return true;
        }
    }

    getSession(){
        return _.pick(this.session, this._sessionKeys);
    }
    
    getStatus() {
        let activeSqls = [];
    
        for (let row of this._sqlSet.values()) {
            activeSqls.push({
                type: row.type,
                sql: row.sql,
                param: row.param ? util.inspect(row.param) : undefined,
                sqlNo: row.sqlNo,
                time: MyUtil.DateTimeString(new Date(row.time)),
                waitMs: Date.now() - row.time
            });
        }
    
        return {
            sid: this.sid,
            spid: this.spid,
            pendingNum: this.pendingNum,
            created: this.created,
            borrowed: this.borrowed && moment(this.borrowed).format('YYYY-MM-DD HH:mm:ss'),
            actived: this.actived && moment(this.actived).format('YYYY-MM-DD HH:mm:ss'),
            getdate: this.getdate && this.getdate(), //{String}
            lastSQL: this.lastSQL,
            status: this._status,
            session: this.getSession(),
            activeSqls: activeSqls
        }
    }

    /**
     *
     * @param connection
     */
    // setConnection(connection) {
    //     //if (arguments.length === 0) {
    //     //    return _this.connection;
    //     //}
    //     // this.Title = this.option.title + '_' + nuid.next(); //String(uuid.v1()).substr(0, 8);
    //     this.connection = connection;
    //     this._status = 'ok';
    // }

    /**
     * 使用当前Connection进行连接
     * @param cb {function(*=,MyConnection=)}
     */
    // async Connect(cb) {
    //     try{
    //         if (this._status) {
    //             let value=await _isClosed(this); //, (err, value) => {
    //             if (!value) return this; //返回当前已连接
    //             await this.Disconnect() ;//() => { //释放
    //         }
    //         let pool = this.pool;
    //         if (!pool) return Promise.reject('连接池还未初始化!');
    //         showLog(this, "Connect");
    //         let conn=await pool.getConnection(this);//重新连接
    //         if (cb) return cb(null,conn);
    //         return conn;
    //     }catch(err){
    //         if (cb) return cb(err);
    //         return Promise.reject(err);
    //     }
    // }

    /**
     * 归还当前连接
     * @param cb
     * @return {*}
     */
    async Disconnect(cb) {
        let pool = this.pool;
        let conn = this;
        //connection.end();
        try {
            let {borrowed} = conn;
            let {usercode, sourceUrl} = conn.session;
            let {query, execute, commit, rollback, other} = conn.sqlSum;
            await pool.releaseConn(conn);
            showLog(conn, 0, usercode + ' ' + sourceUrl + ' release:' + (new Date().getTime() - borrowed) + 'ms query:' + query + ' execute:' + execute + ' commit:' + commit + ' rollback:' + rollback + ' other:' + other);
            if (cb) return cb();
            return null;
        }catch(err){
            conn.logger.error("Release:", err);
            if (cb) return cb(err);
            return Promise.reject(err);
        }

    }

    /**
     *
     * @param cb
     * @return {*}
     */
    async Close(cb) {
        if (cb) return this.Close().then((result) => cb(null, result)).catch(cb);
        showSql(this, 0, 'Close');
        if (this._status === 'closed') return null;
        // const group = _.get(this, 'pool.group');
        this._status = 'closed';
        this.emit('closed');
        try {
            return await this._connection.close();
        } catch (err) {
            this.logger.error('Connection close error:', err);
            return Promise.reject(err);
        }
    }

    /**
     *
     * @return {*}
     */
    async setOption() {
        return await this.pool.adapter.setOption(this);
    }

    /**
     * 添加应用信息 (自定义信息)
     * @param appInfo
     * @param [keys]
     */
    setAppInfo(appInfo,keys) {
        this.session = appInfo;
        this._sessionKeys = keys;
        this.postEvents = [];
    }

    /**
     * 获取应用信息
     * @returns {*}
     */
    getAppInfo() {
        return this.session;
    }

    async getInfo(key) {
        if (!this.pool.adapter.getInfo) return null;
        return await this.pool.adapter.getInfo(this, key);
    }

    /**
     * @param sql
     * @param [paramsList]
     * @param [option]
     * @param [cb] {function(*=,*=)}
     * @returns {*}
     */
    async Query(sql, paramsList, option, cb) {
        let args = Array.from(arguments);
        if (args[args.length - 1] instanceof Function) cb = args.pop();
        try {
            paramsList = args[1] || null;
            option = args[2] || {};
            let result = await queryBase(this, _.defaults({sql, paramsList}, option));
            if (cb) return cb(null, result.rows);
            return result.rows;
        } catch (err) {
            if (cb) return cb(err);
            return Promise.reject(err);
        }
    }

    /**
     * @param sql
     * @param [paramsList]
     * @param [option]
     * @param [cb] {function(*=,*=)}
     * @returns {*}
     */
    async QueryGrid(sql, paramsList, option, cb) {
        let args = Array.from(arguments);
        if (args[args.length - 1] instanceof Function) cb = args.pop();
        try {
            paramsList = args[1] || null;
            option = args[2] || {};
            let result = await queryBase(this, _.defaults({sql, paramsList,grid:true}, option));
            if (cb) return cb(null, result.rows);
            return result.rows;
        } catch (err) {
            if (cb) return cb(err);
            return Promise.reject(err);
        }
    }

    // async QueryPart(sql, paramsList, skipCount, cb) {
    //     let args = Array.from(arguments);
    //     cb = args[args.length - 1] instanceof Function ? args.pop() : null;
    //     try {
    //         await checkConnection(this);
    //         let result = await QueryPartAsync.apply(this, [this].concat(args));
    //         if (cb) return cb(null, result);
    //         return result;
    //     } catch (err) {
    //         if (cb) return cb(err);
    //         return Promise.reject(err);
    //     }
    // }

    async QueryLimit(baseSql, option, cb) {
        let args = Array.from(arguments);
        cb = args[args.length - 1] instanceof Function ? args.pop() : null;
        try {
            await checkConnection(this);
            let result = await queryLimit.apply(this, [this].concat(args));
            if (cb) return cb(null, result);
            return result;
        } catch (err) {
            if (cb) return cb(err);
            return Promise.reject(err);
        }
    }

    /**
     * @param sql
     * @param paramsList
     * @param [option]
     * @returns {{label,type,rows,rowCount}}
     */
    async QueryToFile(sql, paramsList, option) {
        let ret = MyUtil.checkNull(option, ['filename']);
        if (ret) return Promise.reject(ret);
        try {
            await checkConnection(this);
            return await queryBase(this, _.defaults({sql, paramsList}, option));
        } catch (err) {
            return Promise.reject(err);
        }
    }

    async Execute(sql, paramsList, datatypes, cb) {
        let args = Array.from(arguments);
        cb = args[args.length - 1] instanceof Function ? args.pop() : null;

        let _this = this;
        let isCallback = false;
        let executeTimeout = _.get(this, 'pool._config.executeTimeout', (2 * 60 * 1000));
        //超时断开
        let timeoutHandle;
        try {
            await checkConnection(this);
            let p1 = new Promise((resolve, reject) => {
                timeoutHandle = setTimeout(async () => {
                    if (isCallback === false) {
                        isCallback = true;
                        try{
                            await _this.Close();
                        }catch (err){
                            console.error(err);
                        }
                        let msg = '超时了:' + sql + ' ' + MyUtil.String(_.get(_this, 'session.souceUrl'));
                        _this.logger.error(_this.spid, msg);
                        reject(new Error(msg)); //返回Error对象
                    }
                }, executeTimeout);//默认2分钟
            });
            let p2 = execute.apply(this, [this].concat(args));

            let result = await Promise.race([p1, p2]);
            isCallback = true;
            if (timeoutHandle) clearTimeout(timeoutHandle); //清除超时检测
            if (cb) return cb(null, result);
            return result;
        } catch (err) {
            isCallback = true;
            if (timeoutHandle) clearTimeout(timeoutHandle); //清除超时检测
            if (cb) return cb(err);
            return Promise.reject(err);
        }
    }

    /**
     * @param sql
     * @param [cb] {function(*=,*=)}
     * @returns {*}
     */
    async ExecuteDDL(sql, cb) {
        let args = Array.from(arguments);
        cb = args[args.length - 1] instanceof Function ? args.pop() : null;
        try {
            await checkConnection(this);
            let result = await executeDDL.apply(this, [this].concat(args));
            if (cb) return cb(null, result);
            return result;
        } catch (err) {
            if (cb) return cb(err);
            return Promise.reject(err);
        }
    }

    /**
     *
     * @param [cb] {function(*=,*=)}
     * @returns {*}
     */
    async Commit(cb) {
        if (cb) return this.Commit().then(r => cb(null, r)).catch(cb);
        if (this._autoCommit === true) {
            showSql(this, 0, "Don't need to commit in non-transaction mode!");
            return;
        }
    
        // await checkConnection(this);
        const group = _.get(this, 'pool.group');
        if (!this._connection) return Promise.reject(new Error(group + ':Connection is closed!')); //已被close
    
        let _fin = this.getExecuteFin('commit');
        try {
            let result = await this._connection.commit();
            this.emit('commit', result); //保持兼容
            return _fin({result: result});
        } catch (err) {
            this.logger.error('Connection commit error:', err);
            // await this.getDBErr(err).catch(e => (err = e));//reject方式返回精简错误
            return _fin({error: err});
        }
    }

    /**
     *
     * @param [cb] {function(*=,*=)}
     * @returns {*}
     */
    async Rollback(cb) {
        if (cb) return this.Rollback().then(r => cb(null, r)).catch(cb);
        if (this._autoCommit === true) {
            showSql(this, 0, "Don't need to rollback in non-transaction mode!");
            return;
        }
        this.removeAllListeners('commit');
        const group = _.get(this, 'pool.group');
        // if (conn._status==='closed') return cb(new Error(_.get(conn,'pool.group')+':Connection is closed!'));
        if (!this._connection) return Promise.reject(new Error(group + ':Connection is closed!')); //已被close
    
        let _fin = this.getExecuteFin('rollback');
        try {
            let result = await this._connection.rollback();
            return _fin({result: result});
        } catch (err) {
            this.logger.error('Connection rollback error:', err);
            // await this.getDBErr(err).catch(e => (err = e));//reject方式返回精简错误
            await this.Close().catch(() => null);
            return _fin({error: err});
        }
    }

    /**
     *
     * @param value {boolean}
     * @param [cb] {function(*=,*=)}
     * @returns {*}
     */
    async setAutoCommit(value, cb) {
        if (cb) return this.setAutoCommit(value).then(r => cb(null, r)).catch(cb);
        // let args = Array.from(arguments);
        // cb = _.last(args);
        await checkConnection(this);
    
        const sql = 'setAutoCommit-->' + value;
        let conn = this;
        let _fin = conn.getExecuteFin('set', sql);
        
        let result = await conn._connection.setAutoCommit(value)
            .catch(err => _fin({error: err}));
        
        this._autoCommit = value;
        
        return _fin({result: result});
    
        // let result = await setAutoCommit(this, value);
        // this._autoCommit = value;
        // return result;
    }

    /**
     *
     * @param [cb] {function(*=,*=)}
     * @returns {*}
     */
    async getAutoCommit(cb) {
        if (cb) return this.getAutoCommit().then(r => cb(null, r)).catch(cb);
        return await this._connection.getAutoCommit();
        // let args = Array.from(arguments);
        // return await getAutoCommit.apply(this, [this].concat(args));
    }

    // async SelectLast(column, from, cb) {
    //     let args = Array.from(arguments);
    //     cb = args[args.length - 1] instanceof Function ? args.pop() : null;
    //     try {
    //         await checkConnection(this);
    //         let result = await SelectLastAsync.apply(this, [this].concat(args));
    //         if (cb) return cb(null, result);
    //         return result;
    //     } catch (err) {
    //         if (cb) return cb(err);
    //         return Promise.reject(err);
    //     }
    // }

    /**
     *
     * @param err {{cause}}
     * @param [cb]
     */
    async getDBErr(err, cb) {
        if (cb) return this.getDBErr(err).then((result)=>cb(null,result)).catch(cb);
        if (!err) return; //无错误
        if (!(err instanceof Error)) {
            return Promise.reject(err);
        }

        let _this=this;
        if (_.get(err, 'cause.getErrorCode')) {
            let shortError = _.pick(_this.session, ['group', 'sourceUrl', 'usercode']) || {};
            // shortError.sqlCode = -1;
            Object.assign(shortError, _.pick(_this, ['spid']));
            if (_this.actived) shortError.actived = moment(_this.actived).format('YYYY-MM-DD HH:mm:ss');
    
            Object.assign(shortError,await getShortError(err));

            if (_this.pool.adapter.isCanIgnoreError) {
                if (_this.pool.adapter.isCanIgnoreError(shortError)) {
                    _this.logger.debug('getDBErr',shortError);
                    return 0;
                }
            } else if (_this.pool.adapter.getCanIgnoreError) {
                if (_this.pool.adapter.getCanIgnoreError().includes(shortError["ErrorCode"])) {
                    _this.logger.debug('getDBErr',shortError);
                    return 0;
                }
            }
            _this.pool.adapter.getSimpleError && await _this.pool.adapter.getSimpleError(this, shortError);
            _this.logger.debug('getDBErr',shortError);
            return Promise.reject(shortError);
        }
        return Promise.reject(err);

        // let code=err.sqlDBCode || (_.get(err,'cause.getErrorCodeSync') || (()=>null)).call(err.cause) || -1;
        // let text=err.sqlErrText /*|| (_.get(err,'cause.getMessageSync') || (()=>null)).call(err.cause)*/ || toString(err);
        // return JSON.stringify({
        //     sqlDBCode:code,
        //     sqlErrText:text
        // }); //getStack:()=>(err.message || err.stack || err)
    }
    /**
     * 导出excel option指定excel参数
     * @param option {{filename,sheets:[{ sheetName, sheetHead, sql, serial, titleHeight }]}}
     */
    async exportExcel(option) {
        let {filename} = option || {};
        let parseResult = path.parse(filename);
        try {
            let connection = this._connection;
            let _fin = this.getExecuteFin('execute', 'exportExcel:' + option.sheets.map(row => row.sql).join('\nGO\n'), option);
            let result = await java.callStaticMethod("node.jdbc", "rs2Excel", connection, parseResult.base, parseResult.dir, JSON.stringify(option.sheets))
                .catch(error => _fin({error: error}));
            return _fin({result: result, message: 'exportExcel:' + MyUtil.String(result)});
        } catch (err) {
            return Promise.reject(err);
        }
    }
    
    /**
     * 
     * @param option {{filename,delimiter,sql,charset:String,placeholderTypes:[String],lineNoPlace:Number,batchNum,commitModel:String}}
     * @return {Promise<Number>}
     */
    async txt2Table(option){
        let ret = MyUtil.checkNull(option, ['filename','delimiter','sql']);
        if (ret) return Promise.reject(ret);
        // commitModel(once,withBatch)
        let {filename,delimiter,sql,charset,placeholderTypes,lineNoPlace=-1,batchNum,commitModel} = option || {};
        if(placeholderTypes && !_.isArray(placeholderTypes)) return Promise.reject('placeholderTypes必须为数组');
        if(placeholderTypes && lineNoPlace>placeholderTypes.length-1) return Promise.reject('行号下标lineNoPlace不得超过placeholderTypes下标界限');
        if(commitModel && !_.includes(['ONCE','WITHBATCH','NOT'], commitModel)) return Promise.reject('commitModel仅ONCE、NOT和WITHBATCH三种模式');
        if(!Number.isInteger(lineNoPlace)) return Promise.reject('行号下标lineNoPlace必须为整数');

        if(placeholderTypes){
            let sqlTypes = [];
            await _.map(placeholderTypes, (item)=>{
                sqlTypes.push(SQL_TYPES[item.toUpperCase()]);
            });
            placeholderTypes = sqlTypes.join(",");
        }
        lineNoPlace = lineNoPlace < 0 ? -1 : lineNoPlace;

        try{
            let connection = this._connection;
            let _fin = this.getExecuteFin('txt2Table');
            let rows = await java.callStaticMethod(
                "node.jdbc",
                "txt2Table",
                connection,
                filename,
                delimiter,
                sql,
                charset || "UTF-8",
                placeholderTypes || "",
                lineNoPlace,
                batchNum || 2000,
                commitModel || "WITHBATCH"
            ).catch(error => _fin({error: error}));
            return _fin({result: rows});
        }catch (err) {
            return Promise.reject(err);
        }
    }

    /**
     * 导出sqlite文件 option指定sqlite参数
     * @param option {{filename,tables:[{ querySourceDataSql, targetTableName, targetFields, createTargetTableSql }]}}
     * querySourceDataSql: 数据源sql
     * targetTableName: sqlite需要创建的表名
     * targetFields: sqlite需要插入的字段
     * createTargetTableSql: sqlite建表语句
     */
    async exportSqlite(option) {
        let {filename} = option || {};
        try {
            let connection = this._connection;
            // this.logger.debug(filename, option);
            let dbms = _.get(this.pool, '_config.dbms');
    
            let _fin = this.getExecuteFin('execute', 'exportSqlite:' + option.tables.map(row => row.querySourceDataSql).join('\nGO\n'), option);
    
            let result = await java.callStaticMethod("node.jdbc", "createSqliteFile", connection, filename,
                JSON.stringify({
                    dbms: dbms,
                    tables: JSON.stringify(option.tables)
                })).catch(error => _fin({error: error}));
    
            return _fin({result: result, message: 'exportSqlite:' + MyUtil.String(result)});
        } catch (err) {
            return Promise.reject(err);
        }
    }
    
    /**
     * @param type
     * @param sql
     * @param [param]
     * @param batchSize
     * @return {*}
     */
    getExecuteFin(type,sql,param,batchSize) {
    
        const sqlHandle = {type, sql, param};
        this.emit('execute', sqlHandle, {step: 'start', batchSize});
    
        return async ({step, error, result, message, batchNo}) => {
        
            if (error) {
                error = await this.getDBErr(error).catch(e => e);
                if (!error) result = error; //可忽略的错误
            }
        
            this.emit('execute', sqlHandle, {
                step: step || 'end', error: error, result, message,
                batchNo
            });
        
            if (error) return Promise.reject(error);
            return result;
        };
    }
}

exports=module.exports=MyJdbc;

/**
 *
 * @type {MyConnection}
 */
MyJdbc.Connection=MyConnection;
/**
 *
 * @type {MyPool}
 */
MyJdbc.Pool=MyPool;

/**
 *
 * @param myPool {MyPool}
 */
function getPoolStatus(myPool){
    return {
        dbms: myPool._config.dbms,
        created: myPool.created,
        url: myPool._config.url,
        status: myPool._status,
        available: myPool._pool.available,
        borrowed: myPool._pool.borrowed,
        pending: myPool._pool["pending"],
        max: myPool._pool.max,
        min: myPool._pool.min,
        size: myPool._pool["size"],
        connectionMaxPending: myPool.connectionMaxPending
    };
}

/**
 *
 * @param myPool {MyPool}
 */
function showConnections(myPool) {
    // let showList = [];
    // myPool._connections.forEach((conn, key) => {
    //     showList.push(conn.getStatus());
    // });
    return {
        poolStatus: getPoolStatus(myPool),
        lastConnection: myPool.lastConnection && myPool.lastConnection.getStatus()
    };
}

// async function setFetch(statement){
//     return await statement.setFetchSize(100);
// }

/**
 *
 * @param statement
 * @param rows {Array} 一维/二维数组
 * @param datatypes {Array}
 * @param option {{[isSelect],[numberPrecision]}}
 */
async function setParams(statement,rows,datatypes,option) {
    // console.debug(rows,datatypes);
    /**
     * 1.获取类型String数组
     * 2.获取类型函数数组
     * 3.循环取出参数,并行进行setInt,然后执行addbatch
     */

    if (!(rows instanceof Array)) return Promise.reject(new Error('第二个参数必须是数组!'));
    if (_.size(rows) === 0) return Promise.reject('第二个参数不能是空数组!');
    if (!(rows[0] instanceof Array)) rows = [rows]; //转换成二维数组
    let rowCount = _.size(rows); //计算行数
    if (!_.size(datatypes)) datatypes =[] ;//= sqlHelper.DATATYPE(rows[0]);
    let nTypes = [];
    // _.forEach(rows[0], (v, n) => {
    for (let n=0;n<rows[0].length;n++) {
        let v=rows[0][n];
        let _typeof = typeof v;

        if (!datatypes[n]) {
            // if (v!==null || v===undefined || _typeof)
            switch (_typeof) {
                case 'number':
                    datatypes[n] = 'DOUBLE';
                    break;
                case 'boolean':
                    datatypes[n] = 'int';
                    break;
                default:
                    datatypes[n] = 'string';
                    break;
            }
        }
        let jdbcType = SQL_TYPES[datatypes[n].toUpperCase()] || SQL_TYPES['STRING'];
        if (!jdbcType) {
            logger.error(rows, datatypes);
            return Promise.reject('无效的java.sql.Types:' + datatypes[n])
        }

        if (jdbcType === 12 && _typeof !== 'string') { //指定string,实际非string,则转String
            for (let i = 0; i < rowCount; i++) { //行
                rows[i][n] = (rows[i][n] == null) ? null : String(rows[i][n]);
            }
        } else if (_typeof !== 'number' && (jdbcType !== 12)) { //指定非string,转Number
            for (let i = 0; i < rowCount; i++) { //行
                rows[i][n] = (rows[i][n] == null) ? null : Number(rows[i][n]); //Number(true)==1
            }
        }

        nTypes.push(jdbcType); //转换成常量数字值
    }


    // let setFns=[];


    // for (let i = 0, total = _.size(datatypes); i < total; i++) {
    // let fn= statement._ps["set" + (datatypes[i])]; //.charAt(0).toUpperCase())+datatypes[i].slice(1).toLowerCase()
    // if (!fn){
    //     return cb('无效的方法:set'+datatypes[i]);
    // }
    // setFns.push(fn.bind(statement._ps)); //预绑定参数,必须要绑定_ps,否则node会直接退出
    // }
    // console.debug(nTypes,__filename);
    // return await java.callStaticMethod('node.jdbc', 'setParams', statement, rows, nTypes, option.isSelect);
    return await java.callStaticMethod('node.jdbc', 'setParams',
        statement,
        JSON.stringify(rows),
        JSON.stringify({
            types: nTypes,
            isSelect: option.isSelect,
            numberPrecision: option.numberPrecision
        })
    );
    // return await nodeJdbc.setParams(statement, rows, nTypes, option.isSelect);

    // let fieldsCount=_.size(datatypes) || _.size(rows[0]); //取首行点位符个数,如果传入参数个数不一致,会造成用上一次的数值;
    // let fns=[];
    // for (let i= 0,total=rows.length;i<total;i++){
    //     fns.push((cont)=>{
    //         addrow(rows[i],cont);
    //     });
    // }
    // Thenjs.series(fns).fin((cont,err,result)=>{
    //     if (err) return cb(err);
    //     cb(null,result);
    // });
    //
    // /**
    //  *
    //  * @param row {Array}
    //  * @param cb {function}
    //  */
    // function addrow(row,cb){
    //     let fns=[];
    //     for (let i= 0,total=fieldsCount;i<total;i++){
    //         if (row[i]==null){
    //             fns.push((cont)=>{
    //                 statement._ps.setNull(i + 1,nTypes[i], cont);
    //             });
    //         }else{
    //             fns.push((cont)=>{
    //                 //try不到,setObject可以指定精度,setBigDecimal却不行
    //                 statement._ps.setObject(i+1,row[i],nTypes[i],2,(err)=>{
    //                     if (err) {
    //                         err.stack = '数据类型不一致! 第' + String(i + 1) + '列(' + row[i] + ') \n' + err.stack;
    //                         return cont(err);
    //                     }
    //                     cont();
    //                 });
    //             });
    //         }
    //
    //     }
    //     Thenjs.parallel(fns).fin((cont,err)=>{
    //         if (err) return cb(err);
    //         if (option.isSelect) { //select时不能执行addbatch,否则jconn4会报错
    //             cb();
    //         }else{
    //             statement.addBatch((err)=> {
    //                 if(err) return cb(err);
    //                 cb(null);
    //             });
    //         }
    //     });
    // }
}


/**
 * 查询操作
 * @param conn {MyConnection}
 * @param option {{sql,[paramsList]:Array,[datatypes]:Array,[grid],[filename]}}
 * @return {{label:[String],type:[Number],rowCount:Number,rows:[Object]}}
 */
const queryBase=async function (conn,option) { /*paramsList SQL参数列表*/
    
    let {sql, paramsList, datatypes, grid, filename} = option;
    // let _statement;
    // const label = _.uniqueId('Query');
    let results = {};
    // showSql(conn, sql);
    // let time1 = Date.now();
    let statement, err;
    let fetchRowcount = 0;
    
    let _fin = conn.getExecuteFin('query', sql);
    
    try {
        let paramSize = _.size(paramsList);
        if (paramSize && !(paramsList[0] instanceof Array)) {
            paramsList = [paramsList];
            paramSize = 1;
        }
    
        let resultSet;
        if (paramSize) { /*有参数*/
            // conn.sqlSum.query++;
            statement = await conn._connection.prepareStatement(sql);
            // console.debug(conn.pool.group, conn.spid + ':', paramsList, datatypes || '');
            _fin({step: 'process', message: 'queryParams:' + paramSize + ' ' + MyUtil.inspect(paramsList)}); //仅打印3行日志
            await setParams(statement, paramsList, datatypes, {isSelect: true, numberPrecision: conn._numberPrecision}); //.then(()=>{
            resultSet = await statement.executeQuery();
        } else { /*无参数*/
            // conn.sqlSum.query++;
            statement = await conn._connection.createStatement();
            resultSet = await statement.executeQuery(sql);
        }
    
        // _fin({step: 'process', message: 'execute ok' + (filename ? (',to ' + filename) : '')}); //仅打印3行日志
        results = await toObjArray(resultSet, {grid, statement: statement, filename}); //,(err, results)=> {
        fetchRowcount = results.rowCount;
        // showLog(conn, "Fetch rowcount:" + results.rowCount);
    } catch (e) {
        // console.error(err);
        err = e; //await conn.getDBErr(e);
    }
    try {
        if (statement) await statement.close();
    } catch (err) {
        console.error(err);
    }
    
    if (err) return _fin({error: err});
    return _fin({result: results, message: 'queryRows:' + String(fetchRowcount)});
};

/**
 *
 * @param conn
 * @param sql
 * @param paramsList {Array|[Array]}
 * @param datatypes
 * @return {Promise<Number>}
 * @constructor
 */
async function execute(conn,sql,paramsList,datatypes) /*目前不支持点位符*/ {
    let args = Array.from(arguments);
    // cb = args.pop();
    paramsList = args[2];
    datatypes = args[3];
    
    let paramSize = _.size(paramsList);
    //SQL数组或无参数
    if (sql instanceof Array || !paramSize) { //支持sql数组顺序批量执行
        if (paramsList) conn.logger.warn('目前仅支持无参数的SQL队列!');
        return executeBatch(conn, sql);
    }
    
    //单个SQL且有参数
    // showSql(conn, sql); //paramsList
    let statement = await conn._connection.prepareStatement(sql);
    let err;
    let totalRowCount = 0;
    try {
        // let time1 = Date.now();
        if (!(paramsList[0] instanceof Array)) {
            paramsList = [paramsList];
            paramSize = 1;
        }
        
        // showLog(conn, 'executeParams:' + paramSize, MyUtil.inspect(paramsList));
        // showLog(conn, 'setParams:' + paramsList.length); //仅打印3行日志
        //分批执行
        let batchSize = 10000;
        let batchTimes = Math.ceil(paramSize / batchSize);
        
        let _fin = conn.getExecuteFin('execute', sql, paramsList, batchSize);
        
        for (let i = 0; i < batchTimes; i++) {
            let isLastBatch = (i === (batchTimes - 1));
            
            let batchData = paramsList.slice(i * batchSize, (i + 1) * batchSize);
            // console.debug(conn.session.group, conn.spid + ':', data, datatypes || '');
            await setParams(statement, batchData, datatypes, {
                isSelect: false,
                numberPrecision: conn._numberPrecision
            }).catch(error => {
                return _fin({error});
            });
            // conn.sqlSum.execute++;
            let nrows = await statement.executeBatch().catch(error => {
                return _fin({error});
            });//需要用executeBatch批量执行
            let rowcount = 0;
            if (_.isNumber(nrows)) {
                rowcount = nrows;
            } else {
                rowcount = _.sum(nrows); //注:nrow是java数组,而非instance Array
            }
            totalRowCount += rowcount;
            
            _fin({
                step: (isLastBatch ? 'end' : 'process'),
                result: totalRowCount,
                message: (isLastBatch ? 'updateRows:' : 'executeBatch:') + totalRowCount,
                batchNo: i
            });
        }
    } catch (e) {
        err = e;//await conn.getDBErr(e);
    } finally {
        if (statement) await statement.close();
    }
    if (err) return Promise.reject(err);
    // showLog(conn, 'executeRows:' + ' ' + totalRowCount + ' ' + String(Date.now() - time1) + 'ms');
    return totalRowCount;
    // });
}
// const executeAsync=util.promisify(execute);

/**
 *
 * @param conn
 * @param sql {Array|String}
 * @param option {{emit}}
 * @returns {Number}
 */
async function executeBatch(conn,sql,option) {
    let args = Array.from(arguments);
    // cb = args.pop();
    option = args[2];
    // let {emit} = option || {};
    // emit && emit('connect', conn.spid);
    let statement = await conn._connection.createStatement();
    // emit && emit('create');
    //支持sql数组顺序批量执行
    let nrows = 0;
    let err;
    
    
    try {
        if (sql instanceof Array) {
            let sqlcount = sql.length;
            for (let i = 0; i < sqlcount; i++) {
                let _sql = sql[i];
                let _fin = conn.getExecuteFin('execute', _sql);
                /**
                 * @type {Number}
                 */
                await statement.executeUpdate(_sql).then(result => {
                    nrows += result;
                    return _fin({result, message: 'updateRows:' + result})
                }).catch(error => {
                    return _fin({error});
                });
            }
        } else {
        
            let _fin = conn.getExecuteFin('execute', sql);
        
            await statement.executeUpdate(sql).then(result => {
                nrows += result;
                return _fin({result, message: 'updateRows:' + result});
            }).catch(error => {
                return _fin({error});
            });
        }
    } catch (e) {
        err = e;//await conn.getDBErr(e);
    } finally {
        if (statement) await statement.close();
    }
    if (err) return Promise.reject(err);
    return nrows;
}
// const executeBatchAsync=util.promisify(executeBatch);

async function executeDDL(conn,sql) /*目前不支持点位符*/ {
    // let args = Array.from(arguments);
    // cb = args.pop();
    let autocommit_old;
    let err;
    let ret=await conn.getAutoCommit();
    try{
        if (ret === false) { //改为自提交
            await conn.setAutoCommit(true);
            autocommit_old = false;
        }
        ret=await executeBatch(conn,sql);
    }catch(e){
        err=e;
    }finally{
        if (autocommit_old !== undefined) await conn.setAutoCommit(autocommit_old);
    }
    if (err) return Promise.reject(err);
    return ret;
}

/**
 * {selectlast-->newsql-->},result
 * 根据sort-->返回top语句-->结果-->再取top
 * @param conn {MyConnection}
 * @param baseSql {{select:string,keySort:object,where:object,aggregate,from}}
 * @param option {{select:string,where:object,sort:object,limit:[Array],[isolation],grid,into,from,[toSql],[distinct]}}
 * @constructor
 */
async function queryLimit(conn,baseSql,option) {
    //console.warn('queryLimit:',baseSql.keySort,option.sort);
    let {isolation,into,toSql,distinct}=option;
    let appendSql = '';
    if (isolation != null) appendSql = conn.pool.adapter.appendIsolation(isolation);
    //let SQL='';
    let SELECT = conn.pool.sqlHelper.SELECT(option.select || _.keys(baseSql.keySort).join(','), column);

    if (option.where) {
        if (_.isString(option.where)) {
            let where = MyUtil.parseJSON(option.where); //解析string条件
            if (_.isError(where)) {
                return Promise.reject(where);
            }
            option.where = where;
        }
        if (_.isObject(option.where) === false) {
            return Promise.reject(new Error('where需要传入Object或JSON字符串!'));
        }
    }
    let WHERE = conn.pool.sqlHelper.WHERE(option.where, column);
    let lastSort = conn.pool.sqlHelper.getSort(option.sort, baseSql.keySort);
    let SORT = conn.pool.sqlHelper.SORT(lastSort, column);

    return await selectlastSQL();
    //console.log('select',SELECT,'from',baseSql.from,'where',baseSql.where,'and',WHERE,'group by',baseSql.groupBy,'order by',SORT);
    /**
     *
     * @param key
     * @returns {string}
     */
    function column(key) {
        let expression = (baseSql.select && baseSql.select[key]) || (baseSql.aggregate && baseSql.aggregate[key]);
        if (typeof expression === 'string') {
            return expression;
        }
        if (expression instanceof Array) {
            return expression[0] || key;
        }
        //console.warn('未定义的字段:',key);
        return key;
    }


    /**
     *
     * @param key
     * @return {string}
     */
    function DATATYPE(key) {
        let expression = (baseSql.select && baseSql.select[key]) || (baseSql.aggregate && baseSql.aggregate[key]);
        return (expression instanceof Array && (expression[1] + expression[2])) || 'varchar(100)';
    }

    /**
     *
     */
    async function selectlastSQL() {

        let sql = '';
        let where = [];
        if (baseSql.where && baseSql.where.trim()) where.push(baseSql.where);
        if (WHERE) where.push(WHERE);

        let startRow = 0;
        let fetchRow = 0;
        if (option.limit instanceof Array) {
            startRow = Number(option.limit[0]) || 0;
            fetchRow = Number(option.limit[1]) || 0;
        }else {
            fetchRow = Number(option.limit) || 0;
        }
        // if (!fetchRow) fetchRow=100000; //默认限制10W行

        const DBMS=_.get(conn, 'pool._config.dbms');
        switch (DBMS) {
            case 'mysql': {
                sql = `SELECT ${distinct?'DISTINCT ':''}${SELECT} FROM ${option.from || baseSql.from} ${where.length ? ('WHERE ' + where.join(' AND ')) : ''}`;
                if (SORT) sql += ` ORDER BY ${SORT}`;
                if (fetchRow) {
                    if (!startRow) { //未指定起始行,则取top
                        sql += " LIMIT " + String(fetchRow);
                    } else {
                        sql += " LIMIT " + String(startRow - 1) + ',' + String(fetchRow);
                    }
                }
                sql += appendSql;
                if (toSql) return sql; //仅返回SQL
                return await (option.grid ? conn.QueryGrid(sql) : conn.Query(sql));
            }
            case 'pgsql': {
                sql = `SELECT ${distinct?'DISTINCT ':''}${SELECT} FROM ${option.from || baseSql.from} ${where.length ? ('WHERE ' + where.join(' AND ')) : ''}`;
                if (SORT) sql += ` ORDER BY ${SORT}`;
                if (fetchRow) {
                    if (!startRow) { //未指定起始行,则取top
                        sql += " LIMIT " + String(fetchRow);
                    } else {
                        sql += " LIMIT " + String(fetchRow) + ' OFFSET ' + String(startRow - 1);
                    }
                }
                sql += appendSql;
                if (toSql) return sql; //仅返回SQL
                return await (option.grid ? conn.QueryGrid(sql) : conn.Query(sql));
            }
            default:{

            }
        }

        //首页
        if (startRow <= 1) {
            sql = `SELECT ${fetchRow ? 'TOP ' + String(fetchRow) : ''} ${distinct?'DISTINCT ':''}${SELECT} ${into ? ("into " + into) : ''} FROM ${option.from || baseSql.from} ${where.length ? ('WHERE ' + where.join(' AND ')) : ''}`;
            if (SORT) sql += ` ORDER BY ${SORT}`;
            sql += appendSql;
            if (toSql) return sql; //仅返回SQL
            if (into) return await executeDDL(conn, sql);
            return await (option.grid ? conn.QueryGrid(sql) : conn.Query(sql));
        }

        let selectFieldStr = ''; //,selectFieldStr2=''
        let addWhere = '';
        let top = startRow - 1; //先取出top的最后一行

        let sortKeys = _.keys(lastSort);
        if (_.size(sortKeys) === 0) sortKeys = SELECT.split(','); //未传入排序,则默认select的全部

        sql = '';
        for (let i = 0; i < sortKeys.length; i++) {
            let varName = '@' + sortKeys[i];
            sql += `DECLARE ${varName} ${DATATYPE(sortKeys[i])} \n`;
            if (i > 0) {
                selectFieldStr += ',';
                //selectFieldStr2+=',';
                addWhere += ' AND ';
            }
            selectFieldStr += `${varName}=${column(sortKeys[i])}`;
            //selectFieldStr2+=`${vname} as ${sortKeys[i]}`;
            addWhere += '(' + toCompare(column(sortKeys[i]), lastSort[sortKeys[i]], varName, _.get(baseSql, 'keySort.' + sortKeys[i]));
            if (i < sortKeys.length - 1) addWhere += ` OR (${column(sortKeys[i])}=${varName} `;
        }
        if (sortKeys.length) addWhere += ')'.repeat(sortKeys.length * 2 - 1) + '';

        sql += `\nSELECT TOP ${top} ${distinct?'DISTINCT ':''}${selectFieldStr} FROM ${option.from || baseSql.from} ${where.length ? ('WHERE ' + where.join(' AND ')) : ''}`;
        if (SORT) sql += ` ORDER BY ${SORT}`;
        sql += appendSql;
        //sql+="SELECT "+selectFieldStr2+" where @@rowcount>=1 ";

        if (addWhere) where.push(addWhere);

        sql += `\nSELECT TOP ${fetchRow} ${distinct?'DISTINCT ':''}${SELECT} ${into ? ("into "+into):''} FROM ${option.from || baseSql.from} ${where.length ? ('WHERE ' + where.join(' AND ')) : ''}`;

        if (SORT) sql += ` ORDER BY ${SORT}`;
        sql += appendSql;

        function toCompare(column, sort, value, isPK) {
            if (sort && sort.trim().toLowerCase() === 'desc') {
                return `${column} < ${value} ` + (!isPK ? `OR (${value} IS NOT NULL and ${column} IS NULL)` : '');
            }
            return `${column} > ${value} ` + (!isPK ? `OR ( ${value} IS NULL and ${column} IS NOT NULL)` : '');
        }

        if (toSql) return sql; //仅返回SQL
        if (into) return await executeDDL(conn,sql);
        return await (option.grid ? conn.QueryGrid(sql) : conn.Query(sql));
    }
}
// const queryLimitAsync=util.promisify(queryLimit);

/**
 *
 * @param config {{[user],[password],[properties]:{user,password}}}
 * @returns {*}
 */
async function getJavaProps(config) {
    let Properties = java.import('java.util.Properties');
    /**
     *
     * @type {{rs2table,rs2table2,setParams}}
     */
    // nodeJdbc=java.import("node.jdbc"); //将test.class放在src/node里;
    // ResultSet.prototype.nodeJdbc=nodeJdbc;

    let properties= new Properties();
    let keys=_.keys(config.properties);
    let fns=[];

    keys.forEach((value)=>{
        fns.push(properties.put(value,config.properties[value]));
    });
    await Promise.all(fns);
    return properties;
}

/**
 *
 * @param config config {{url,drivername,[user],[password],[properties]:{user,password}}}
 */
async function _createConnection(config) {
    let url = config.url;
    let props = await getJavaProps(config);
    let driver = await java.newInstance(config.drivername);
    await java.callStaticMethod('java.sql.DriverManager', 'registerDriver', driver);
    return await java.callStaticMethod('java.sql.DriverManager', 'getConnection', url, props);
}

/**
 *
 * @param conn {MyConnection}
 * @returns {*}
 */
async function checkConnection(conn){
    if (conn._status!=='ok') { //
        let err = new Error('连接暂不可使用:' + conn._status);
        conn.logger.error(conn.spid, err);
        return Promise.reject(err);
    }

    return null;
}

function showSql(conn,sqlNo,sql,a) {
    conn.logger.info(conn.spid + (sqlNo ? ('-' + sqlNo) : ''), sql, a || ''); //group+':'+
}

function showLog(conn,sqlNo,text,a) {
    conn.logger.info(conn.spid + (sqlNo ? ('-' + sqlNo) : ''), text, a || ''); //group+':'+
}


/**
 * @param resultSet
 * @param option {{grid:Boolean,filename}}
 */
async function toObjArray(resultSet,option) {

    let {grid: dataGrid, filename} = option || {};

    // let self = resultSet;
    /**
     *
     * @type {[{label,type}]}
     */
    // let metadata = [];
    // let rsmd;
    // let rowCount, fieldsCount;
    // let gridData;

    /**
     * @type String
     */
    let result = await java.callStaticMethod('node.jdbc', 'rs2table3', resultSet, JSON.stringify({filename}));
    // let result=await nodeJdbc.rs2table3Sync(self); //self._rs,
    if (!result) return []; //callback(null, null); //无记录
    // if (typeof result==='string'){

    let [label, type, gridData] = JSON.parse(result);
    if (filename) return {label, type, rowCount: gridData}; //

    let rowCount = _.size(gridData);
    if (dataGrid) return {label, type, rowCount, rows: gridData || []}; //callback(null, gridData); //返回表格数据

    // Get some column metadata.
    // rsmd=await self.getMetaData();
    // fieldsCount=await rsmd.getColumnCount();
    // let parallelFn = [];
    // for (let i = 1; i <= fieldsCount; i++) {
    //     metadata[i - 1] = {};
    //     parallelFn.push(
    //         rsmd.getColumnLabel(i).then((result)=> {
    //             metadata[i - 1].label = result;
    //         })
    //     );
    //     parallelFn.push(
    //         rsmd.getColumnType(i).then((result)=> {
    //             metadata[i - 1].type = result;
    //         })
    //     );
    // }

    // await Promise.all(parallelFn);


    let fieldsCount = _.size(label);
    let rows = [];
    for (let r = 0; r < rowCount; r++) {
        rows[r] = {};
        for (let c = 0; c < fieldsCount; c++) {
            rows[r][label[c]] = gridData[r][c];
        }
    }
    return {label, type, rowCount, rows: rows};

}
