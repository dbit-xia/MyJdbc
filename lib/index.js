
'use strict';
const util=require('util');
// let jinst = require('../jdbc/lib/jinst');
// let dm = require('../jdbc/lib/drivermanager');
// let Connection=require('../jdbc/lib/connection');
let logTime=true;
const _ = require('lodash');
// let ResultSet=require('./resultset'); //重写resultset的方法
const Thenjs=require("thenjs");
const sqlHelper=require('./SqlHelper');
let genericPool=require('generic-pool');
let TimeoutError=require('generic-pool/lib/errors').TimeoutError;
const path=require('path');
// const uuid=require('uuid');
let nodeJdbc;
let MyUtil=require('./MyUtil');
const crypto=require('crypto');
let logger=global.logger || console;
const events=require('events');
let emitter=new events.EventEmitter();
// let java = jinst.getInstance();
const moment=require('moment');
const uuid=require('uuid');
const java = require('java');

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
    // if (!jinst.isJvmCreated()) {
        java.options.push("-Xrs");
        // let java = jinst.getInstance();
        const classpaths = _.filter(jarList.concat(path.join(__dirname, "../java/src")), (classpath) => {
            return (java["classpath"].includes(classpath) === false);
        });
        if (classpaths.length) java.classpath.push.apply(java.classpath, classpaths);
    // }
}

/**
 *
 */
class MyPool{
    /**
     *
     * @param config
     * @returns {*}
     */
    constructor(config) {
        if (!config) throw Error("未定义数据库连接参数!");
        //noinspection JSUnresolvedVariable
        this.emitter=new events.EventEmitter();
        let _config = _.cloneDeep(config);
        this._config = _config;
        // this.onSuccess=null; //(conn,cb)=>cb();
        this._connections = new Map();
        //noinspection JSUnresolvedVariable
        this.pool = null;
        this.created=moment().format('YYYY-MM-DD HH:mm:ss');
        //noinspection JSUnresolvedVariable
        this.logger=console;
        this._status=null; //'连接池还未初始化!';
        this._lastError=null;
        this.connectionMaxPendingNum=0;
        this.connectionMaxPending=null;

        let {encrypt}=_config;
        if (encrypt) { //使用加密
            let logpass = process.env.logpass;
            // console.debug(logpass);
            if (!logpass) {
                console.warn('请先输入启动密钥!');
            } else {
                let password = _.get(_config, 'properties.password');
                // console.debug(password);
                //解密
                try {
                    let key = new Buffer(logpass);
                    let iv = new Buffer(0);
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
        this.adapter=require('./adapter/'+_.get(this,'_config.dbms','ase'));
        this._spidSql=this.adapter.getSpidSql();
        return this;
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
     *
     * @param [cb]
     * @returns {*}
     */
    async open(cb) {
        if (this._status==='success') return cb && cb(); //已经打开功能

        let tempStatus='starting';
        cb && this.emitter.once('end',cb); //启动后再调用
        if (this._status===tempStatus) return;
        this._status=tempStatus; //'正在初始化连接池,请稍候...';

        let _this = this;
        let _config=this._config;
        const factory = {
            create:async function () {
                const sid=_.uniqueId();
                const start_create=new Date().getTime();
                _this.emitter.emit('create',sid);// console.debug('createConnection');
                let conn=await MyJdbc.createMyConnection(_config) ;// , (err, conn) => {

                conn.created=moment().format('YYYY-MM-DD HH:mm:ss.SSS'); //创建时间
                conn.logger=_this.logger;
                conn.pool = _this;
                let isclosed=  await _isClosed(conn); 
                if (isclosed) {
                    await factory.destroy(conn); 
                    return Promise.reject('连接已关闭!');
                }
                showLog(conn, `spid:${conn.spid},getdate:${conn.getdate()}`);
                try{
                    await conn.setOption() ;
                }catch(err){
                    conn.logger.error(err);
                    await MyJdbc.closeMyConnection(conn);
                    return Promise.reject(err);
                }
                conn["sid"] = sid;
                _this._connections.set(conn["sid"], conn);
                showLog(conn,'getConnection:'+((new Date().getTime()) - start_create)+' ms');
                return conn;
            },
            destroy: async function (conn) {
                let sid=conn.sid;
                try{
                    await MyJdbc.closeMyConnection(conn);
                    _this._connections.delete(sid);
                }catch (e){
                    console.error(e);
                    return Promise.reject(e);
                }
                return null; 
            },
            validate: async function (conn) {
                return !(await _isClosed(conn));
            }
        };

        let opts = {
            fifo: false, //优先使用最新获取的连接
            maxWaitingClients: 1000, //最大排队数
            testOnBorrow: true, //获取连接时自动检查状态
            idleTimeoutMillis: 600000, //设置10分钟认为是空闲进程
            //numTestsPerRun:3,
            acquireTimeoutMillis: 20000, //获取连接,20秒超时
            evictionRunIntervalMillis: 10000, //每10s检查一次空闲连接
            max: _config.maxpoolsize, // maximum size of the pool
            min: _config.minpoolsize // minimum size of the pool
        };

        //libuv 线程池大小,默认为4
        let THREADPOOL=(Number(process.env.UV_THREADPOOL_SIZE)||4);
        if (opts.max > THREADPOOL) {
            logger.warn('连接池(' + opts.max + ')大于线程池(' + THREADPOOL + '),建议调整变量UV_THREADPOOL_SIZE(>=maxPool,<=128)');
        }

        /**
         * @type {{on:function(string,Function),available,borrowed,min,max,acquire:function(number),release:function,_waitingClientsQueue}}
         */
        let pool = genericPool.createPool(factory, opts);
        _this.pool = pool; //.__proto__

        let createErrorCount = 0;
        pool.on('factoryCreateError', function (err) {
            createErrorCount++;
            // console.log(createErrorCount);
            _this._lastError=err;
            _this.logger.error('factoryCreateError', err);
            throw err;
            // let dequeue = pool._waitingClientsQueue.dequeue();
            // if (_.has(dequeue, 'reject')) dequeue.reject(err); //从排队中移除一个,避免bug死循环
            // logger.error(err);
            // throw err;
        });

        //等待初始化完成
        let handle = setInterval(() => {
            if ((pool.available + pool.borrowed + createErrorCount) >= pool.min) {
                clearInterval(handle);

                if (createErrorCount){
                    _this._status='failed';
                    _this.logger.error("连接池初始化失败:", JSON.stringify(_this.getStatus()));
                    // cb && cb(_this._lastError, pool);
                    _this.emitter.emit('end',_this._lastError);

                    setTimeout(_this.open.bind(_this), 60 * 1000); //循环监测,避免网站启动时连接数据库失败

                }else{
                    _this._status='success';
                    _this.logger.info("连接池初始化成功:", JSON.stringify(_this.getStatus()));
                    _this.emitter.emit('end',null);
                    // if (_this.onSuccess){
                    // _this.onSuccess(cb);
                    // }else{
                    //     cb && cb(null, pool);
                    // }
                }
            }
        }, 100);
    }

    static test(){
        return 'static';
    }
}

MyPool.prototype.pool=null;

//Main.prototype.getPoolStatus=function (){
//    let pool=this.pool;
//    return getPoolStatus(pool);
//};

/**
 * 从连接池中获取一个连接
 * @returns {*}
 */
MyPool.prototype.getConnection=async function () {

    let _this = this;
    let pool = _this.pool;
    
    if (!pool) {
        let err = new Error('连接池未初化!');
        _this.logger.error('getConnection',err);
        return Promise.reject(err);
    }
    try {
        let conn = await pool.acquire(0); //.then((conn) => {
        _this.emitter.emit('acquire', conn);
        // conn._reset(); //release时调用
        conn.borrowed = (new Date()).getTime();
        conn._status = 'ok';
        conn.actived = (new Date()).getTime();
        return conn;
    }catch(err) {
        if (err instanceof TimeoutError) {
            err=_this.showConnections();
            err.sqlCode = -1;
            err.Message = '获取连接超时,请稍候再试!';
            err.lastError=_this._lastError;
            // return Promise.reject(err);// return cb(err);
        }
        _this.logger.error('getConnection',err);
        return Promise.reject(err);
    }
};
// MyPool.prototype.getConnectionAsync=util.promisify(MyPool.prototype.getConnection);

/**
 * 归还一个连接
 * @param conn {MyConnection}
 * @param cb {function}
 * @return {*}
 */
MyPool.prototype.releaseConn=async function (conn, cb){
    if (cb) return this.releaseConn(conn).then((result)=>cb(null,result)).catch(cb);
    let _this=this;
    let pool=_this.pool;
    conn.setAppInfo(null);
    // showSql(conn,'Release');
    // conn._status=null;
    await pool.release(conn); //.then(function(){
    conn._reset();
    _this.emitter.emit('release',conn["sid"]);
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
 * @param fn {function(conn:MyConnection,cb:Function)}
 * @param output {function(*=,*=)}
 */
MyPool.prototype.autoConnect=function(fn, output) {
    // let isNew=true; /*默认为用新连接*/

    this.getConnection().then((conn)=>{
        connCallback(null,conn);
    }).catch((err)=>{
        output && output(err);
    });
    
    /**
     *
     * @param err
     * @param conn {MyConnection}
     * @returns {*}
     */
    function connCallback(err, conn) {
        if (err) {
            output && output(err);
            return;
        }

        let isCallback;
        let holdTimeout = _.get(conn, 'pool._config.holdTimeout', (2 * 60 * 1000)); //默认两分钟
        let intervalHandle = setInterval(async () => {
            if ((new Date()).getTime() - conn.actived > holdTimeout) {
                if (isCallback) return;
                isCallback = true;
                const lastSQL=conn.lastSQL; //写到Close前,避免Close改lastSQL
                let err,err2;
                try{
                    await conn.Close(); //() => {
                    err = {
                        code: -1,
                        msg: '由于长时间没有活动连接已被断开:' + holdTimeout+','+lastSQL
                    };
                    conn.logger.error(conn.spid + ':', err);
                    await conn.Disconnect(); //(err2) => {    
                }catch(e){
                    clearInterval(intervalHandle);
                    err2=e;
                }
                output && output(err != null ? err : err2);
            }
        }, 1000);
        Thenjs((cont) => {
            conn.setAutoCommit(false, cont);
        }).then((cont) => {
            fn(conn, cont);
        }).fin((cont, err0, result) => {
            conn._status = 'releasing';
            // if (_.get(opt, 'commit') || _.get(childOpt, 'commit')) { //是否提交
            Thenjs((cont) => {
                if (err0) return cont(err0); //存在错误回滚
                conn.Commit(cont);
            }).then(() => {
                cont(err0 != null ? err0 : null, result);
            }).fail((c, err1) => {
                conn.Rollback((err2) => {
                    cont(err0 != null ? err0 : (err1 || err2));
                });
            });
            // } else {
            //     return cont(err0, result);
            // }
        }).fin((cont, err, result) => {
            // if (isNew) {
            if (isCallback) return;
            isCallback = true;
            return conn.Disconnect((err2) => {
                clearInterval(intervalHandle);
                output && output(err != null ? err : err2, result);
            });
            // }
            // clearInterval(intervalHandle);
            // output && output(err, result);
        });
    }
};
MyPool.prototype.autoConnectAsync=util.promisify(MyPool.prototype.autoConnect);

class MyConnection {
    /**
     *
     * @param option {{[title]:string,[pool]:MyPool,sqlSum:{}}}
     */
    constructor(option) {
        this.option = option;
        //noinspection JSUnresolvedVariable
        /**
         *
         * @type {MyPool}
         */
        this.pool = _.get(option, 'pool');
        this.Title = _.get(option, 'title');

        //连接后初始化的变量
        //noinspection JSUnresolvedVariable
        this.logger = null; //pool.logger
        this.connection = null;//=?PoolConnection();
        //noinspection JSUnresolvedVariable
        this._status = null;
        this.spid = null; //{Number}
        this.getdate = null; //{String}
        this.created=moment().format('YYYY-MM-DD HH:mm:ss');
        this.actived=(new Date()).getTime();
        this._reset();
        return this;
    }

    _reset(){
        //归还连接后,需还原的变量列表
        this._status = null;
        this.pendingNum=0;
        this.lastSQL="";
        this.session = null; //Object
        this._sessionKeys=null; //Array
        this.postEvents = null; //Array
        this.borrowed=null; //time
        this.sqlSum={query: 0, execute: 0, commit: 0, rollback: 0, other: 0};
    }

    getStatus() {
        return {
            spid: this.spid,
            pendingNum: this.pendingNum,
            created: this.created,
            borrowed: this.borrowed && moment(this.borrowed).format('YYYY-MM-DD HH:mm:ss'),
            actived: this.actived && moment(this.actived).format('YYYY-MM-DD HH:mm:ss'),
            getdate: this.getdate && this.getdate(), //{String}
            lastSQL: this.lastSQL,
            status: this._status,
            session: _.cloneDeep(_.pick(this.session, this._sessionKeys))
        }
    }

    /**
     *
     * @param connection
     */
    setConnection(connection) {
        //if (arguments.length === 0) {
        //    return _this.connection;
        //}
        this.Title = this.option.title + '_' + String(uuid.v1()).substr(0, 8);
        this.connection = connection;
        this._status = 'ok';
    }

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
            showLog(conn,usercode+' '+sourceUrl+' release:'+ (new Date().getTime() - borrowed) + 'ms query:' + query + ' execute:' + execute + ' commit:' + commit + ' rollback:' + rollback + ' other:' + other);
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
        if (cb) return this.Close().then((result)=>cb(null,result)).catch(cb);
        if (this._status === 'closed') return null;
        this._status = 'closed';
        return await this.connection.close();
    }

    /**
     *
     * @param cb
     * @return {*}
     */
    async setOption(cb) {
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

    /**
     * @param sql
     * @param paramsList
     * @param [skipCount]
     * @param [cb] {function(*=,*=)}
     * @returns {*}
     */
    async Query(sql, paramsList, skipCount, cb) {
        let args = Array.from(arguments);
        cb = args[args.length - 1] instanceof Function ? args.pop() : null;
        try{
            await checkConnection(this);
            let result=await Query.apply(this, [this].concat(args));
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
     * @param [skipCount]
     * @param cb {function(*=,*=)}
     * @returns {*}
     */
    async QueryGrid(sql, paramsList, skipCount, cb) {
        let args = Array.from(arguments);
        cb = (args[args.length - 1] instanceof Function) ? args.pop() : null;
        try {
            let result = await QueryGrid.apply(this, [this].concat(args));
            if (cb) return cb(null, result);
            return result;
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
            let result = await QueryLimit.apply(this, [this].concat(args));
            if (cb) return cb(null, result);
            return result;
        } catch (err) {
            if (cb) return cb(err);
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
                        _this.logger.error(_this.spid + ':', msg);
                        reject(new Error(msg)); //返回Error对象
                    }
                }, executeTimeout);//默认2分钟
            });
            let p2 = Execute.apply(this, [this].concat(args));

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
     * @param cb {function(*=,*=)}
     * @returns {*}
     */
    async ExecuteDDL(sql, cb) {
        let args = Array.from(arguments);
        cb = args[args.length - 1] instanceof Function ? args.pop() : null;
        try {
            await checkConnection(this);
            let result = await ExecuteDDL.apply(this, [this].concat(args));
            if (cb) return cb(null, result);
            return result;
        } catch (err) {
            if (cb) return cb(err);
            return Promise.reject(err);
        }
    }

    /**
     *
     * @param cb {function(*=,*=)}
     * @returns {*}
     */
    async Commit(cb) {
        if (cb) return this.Commit().then(r=>cb(null,r)).catch(cb);
        // let err=await checkConnection(this);
        // if (err) return cb(err);
        let args = Array.from(arguments);
        return await Commit.apply(this, [this].concat(args));
    }

    /**
     *
     * @param cb {function(*=,*=)}
     * @returns {*}
     */
    async Rollback(cb) {
        if (cb) return this.Rollback().then(r=>cb(null,r)).catch(cb);
        // let err=await checkConnection(this);
        // if (err) return cb(err);
        let args = Array.from(arguments);
        return await Rollback.apply(this, [this].concat(args));
    }

    /**
     *
     * @param value {boolean}
     * @param cb {function(*=,*=)}
     * @returns {*}
     */
    async setAutoCommit(value, cb) {
        if (cb) return this.setAutoCommit(value).then(r=>cb(null,r)).catch(cb);
        let args = Array.from(arguments);
        // cb = _.last(args);
        await checkConnection(this);
        return await setAutoCommit.apply(this, [this].concat(args));
    }

    /**
     *
     * @param cb {function(*=,*=)}
     * @returns {*}
     */
    async getAutoCommit(cb) {
        if (cb) return this.getAutoCommit().then(r=>cb(null,r)).catch(cb);
        let args = Array.from(arguments);
        return await getAutoCommit.apply(this, [this].concat(args));
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
     * @param cb
     */
    async getDBErr(err, cb) {
        if (cb) return this.getDBErr(err).then((result)=>cb(null,result)).catch(cb);
        if (!err) return; //无错误
        if ((err instanceof Error) === false) {
            return Promise.reject(err);
        }

        let _this=this;
        if (_.get(err, 'cause.getErrorCode')) {
            let shortError = _.pick(_this.session, ['group', 'sourceUrl', 'usercode']) || {};
            shortError.sqlCode = -1;
            Object.assign(shortError, _.pick(_this, ['spid']));
            if (_this.actived) shortError.actived = moment(_this.actived).format('YYYY-MM-DD HH:mm:ss');
            
            await Promise.all([err.cause.getErrorCode().then((result)=>{
                shortError['ErrorCode'] = result;
            }), err.cause.getMessage().then((result)=>{
                shortError['Message'] = result;
            }), err.cause.getSQLState().then((result)=>{
                shortError['SQLState'] = result;
            })]);
            
            if (_this.pool.adapter.getCanIgnoreError().includes(shortError["ErrorCode"])) {
                // _this.pool.logger.warn(_this.spid + ':', shortError);
                return 0;
            }
            _this.pool.logger.error(_this.spid + ':', shortError);
            _this.pool.adapter.getSimpleError && _this.pool.adapter.getSimpleError(shortError);
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
}

exports=module.exports=MyJdbc;

//MyConnection.prototype.Query=function(sql, paramsList, skipCount, cb){
//    let args = Array.from(arguments);
//    return Query.apply(this,[this].concat(args));
//};

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
 * @param config {{url,drivername,properties:{user,password}}}
 * @param callback {function(*=,MyConnection=)}
 */
MyJdbc.createMyConnection=async function (config, callback){
    let conn=await _createConnection(config); //,(err,conn)=>{
        // if (err) return callback(err);
        let myconn=new MyConnection({});
        myconn.setConnection(conn); //new Connection(conn));
        return myconn;
        // callback(null,myconn);
    // });
};
// MyPool.createMyConnectionAsync=util.promisify(MyPool.createMyConnection);

/**
 *
 * @param conn {MyConnection}
 * @param callback {function}
 */
MyJdbc.closeMyConnection=async function(conn, callback){
    showLog(conn,'closeConnection');
    return await conn.Close();
};
// MyPool.closeMyConnectionAsync=util.promisify(MyPool.closeMyConnection);

function time(data,conn) {
    if (logTime) {
        let startTime = new Date().getTime();
        conn.pendingNum++;
        if (conn.borrowed && conn.pendingNum > conn.pool.connectionMaxPendingNum) {
            conn.pool.connectionMaxPendingNum = conn.pendingNum;
            conn.pool.connectionMaxPending = _.cloneDeep(conn.getStatus());
        }
        emitter.once(data, (conn) => {
            showLog(conn, data, (new Date().getTime() - startTime) + 'ms');
        });
    }
}
function timeEnd(data,conn) {
    if (conn.pendingNum) conn.pendingNum--;
    logTime && emitter.emit(data, conn);//console.timeEnd(data);
}

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
        pool: myPool.pool.available,
        reserve: myPool.pool.borrowed,
        pending: myPool.pool["pending"],
        max: myPool.pool.max,
        min: myPool.pool.min,
        size: myPool.pool["size"],
        connectionMaxPending: myPool.connectionMaxPending
    };
}

/**
 *
 * @param myPool {MyPool}
 */
function showConnections(myPool) {
    let showList = [];
    myPool._connections.forEach((conn, key) => {
        showList.push(conn.getStatus());
    });
    return {
        poolStatus: getPoolStatus(myPool),
        connections: showList
    };
}

/**
 *
 * @param conn {MyConnection}
 * @private
 */

async function _isClosed(conn) {
    try{
        let result=await conn.QueryGrid(conn.pool._spidSql);//.then((result) => {
        conn.spid = result[0][0];
        const a = new Date(result[0][1]);
        const b = new Date();
        const diffSSS = a - b; // 86400000
        conn.getdate = () => {
            return MyUtil.DateTimeString(new Date(new Date().getTime() + diffSSS));
        };
        return false;
    }catch(e){
        console.error(e);
        return true;
    }
}
// const _isClosedAsync=util.promisify(_isClosed);

async function setFetch(statement){
    //if (err) return getDBErr(err,cb);
    return await statement.setFetchSize(100); //, (err)=> {
        // if (err) return cb(err);
        //statement.setFetchDirection(1001, (err)=> {
        //log('setMaxRows:' + ret)
        //if (err) return cb(err);
        //statement.setMaxRows(10, (err, ret)=> {
        //if (err) return cb(err, ret);
        //statement.getFetchDirection((err,ret)=>{
        //if (err) return cb(err, ret);
        //log('getFetchDirection:'+ret);
        // cb(null);
        //});
        //});
        //});
    // });
}


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

/**
 *
 * @param statement
 * @param rows {Array} 一维/二维数组
 * @param datatypes {Array}
 * @param option {{[isSelect]}}
 */
async function setParams(statement,rows,datatypes,option) {
    // console.debug(rows,datatypes);
    /**
     * 1.获取类型String数组
     * 2.获取类型函数数组
     * 3.循环取出参数,并行进行setInt,然后执行addbatch
     */

    if ((rows instanceof Array) === false) return Promise.reject('第二个参数必须是数组!');
    if (_.size(rows) === 0) return Promise.reject('第二个参数不能是空数组!');
    if (rows[0] instanceof Array === false) rows = [rows]; //转换成二维数组
    let rowCount = _.size(rows); //计算行数
    if (!_.size(datatypes)) datatypes =[] ;//= sqlHelper.DATATYPE(rows[0]);
    let nTypes = [];
    // _.forEach(rows[0], (v, n) => {
    for (let n=0;n<rows[0].length;n++) {
        let v=rows[0][n];
        var _typeof = typeof v;

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
    return await java.callStaticMethod('node.jdbc', 'setParams', statement, rows, nTypes, option.isSelect);
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
 * @param option {{sql,[paramsList]:Array,[datatypes]:Array,[grid]}}
 * @constructor
 */
const QueryBase=async function (conn,option) { /*paramsList SQL参数列表*/
    let sql=option.sql;
    let paramsList=option.paramsList;
    let datatypes=option.datatypes;
    let grid=option.grid;
    let _statement;
    const label=_.uniqueId('Query');
    let results=[];
    showSql(conn,sql);
    time(label,conn);
    let statement,err;
    try{
        let resultSet;
        if (paramsList){ /*有参数*/
            conn.sqlSum.query++;
            statement=await conn.connection.prepareStatement(sql);
            await setParams(statement, paramsList,datatypes,{isSelect:true}); //.then(()=>{
            resultSet=await statement.executeQuery();
        }else { /*无参数*/
            conn.sqlSum.query++;
            statement=await conn.connection.createStatement();
            resultSet=await statement.executeQuery(sql);
        }
        results=await toObjArray(resultSet,{grid,statement:statement}); //,(err, results)=> {
        showLog(conn,"Fetch rowcount:" + _.size(results));
    }catch(e){
        // console.error(err);
        err=await conn.getDBErr(e);
    }
    try{
        if (statement) await statement.close();
    }catch(err){
        console.error(err);
    }
    
    timeEnd(label,conn);
    if (err) return Promise.reject(err);
    return results;
};
// const QueryBaseAsync=util.promisify(QueryBase);

async function Query(conn,sql,paramsList,datatypes) { /*paramsList SQL参数列表*/
    let args = Array.from(arguments);
    // cb = args.pop();
    paramsList=args[2];
    datatypes=args[3];
    return await QueryBase(conn,{sql,paramsList,datatypes});
}
// const QueryAsync=util.promisify(Query);

async function QueryGrid(conn,sql,paramsList,datatypes) { /*paramsList SQL参数列表*/
    let args = Array.from(arguments);
    // cb = args.pop();
    paramsList=args[2];
    datatypes=args[3];
    return await QueryBase(conn,{sql,paramsList,datatypes,grid:true});
}
// const QueryGridAsync=util.promisify(QueryGrid);

// function QueryPart(conn,sql,paramsList,skipCount,cb) { /*paramsList SQL参数列表,skipCount 忽略的行数 为可选参数*/
//     let args = Array.from(arguments);
//     cb = args.pop();
//     paramsList && paramsList.length>0 && console.warn("QueryPart不支持指定参数列表!");
//     conn.connection.createStatement(1004,1007,function(err, statement) {
//         if (err) return conn.getDBErr(err,cb);
//         setFetch(statement,(err)=> {
//             execute(err,statement);
//         });
//     });

//     function execute(err,statement) {
//         if (err) return conn.getDBErr(err,cb);
//         const _label2=_.uniqueId('executeQuery');
//         showSql(conn,sql);
//         time(_label2,conn);
//         statement.executeQuery(sql, function (err, resultset, fields) {
//             timeEnd(_label2,conn);
//             showLog(conn,"End Query:" + sql);
//             if (err) {
//                 //logger.error("query:" + err);
//                 return conn.getDBErr(err,cb);
//             }
//             const _label3=_.uniqueId('absolute');
//             time(_label3,conn);
//             resultset.absolute(skipCount,(err)=>{ /*跳转到指定index*/
//                 timeEnd(_label3,conn);
//                 if (err) return conn.getDBErr(err,cb);
//                 const _label4=_.uniqueId('toObjArray');
//                 time(_label4,conn);
//                 toObjArray(resultset,(err, results)=> {
//                     timeEnd(_label4,conn);
//                     if (err) return conn.getDBErr(err,cb);
//                     showLog(conn,"Fetch rowcount:" + results.length);
//                     cb && cb(null, results||[], fields);
//                 }, 0);
//             });
//         });
//     }
// }
// const QueryPartAsync=util.promisify(QueryPart);

/**
 *
 * @param conn
 * @param sql
 * @param paramsList
 * @param datatypes
 * @return {Promise<Number>}
 * @constructor
 */
async function Execute(conn,sql,paramsList,datatypes) /*目前不支持点位符*/ {
    let args = Array.from(arguments);
    // cb = args.pop();
    paramsList=args[2];
    datatypes=args[3];

    if (sql instanceof Array || !_.size(paramsList) ) { //支持sql数组顺序批量执行
        if (paramsList) console.warn('目前仅支持无参数的SQL队列!');
        return await ExecuteBatch(conn,sql);
    }
    // Thenjs((cont)=> {
    showSql(conn,sql); //paramsList
    let statement=await conn.connection.prepareStatement(sql);
    let err;
    let totalRowCount=0;
    try{
        showLog(conn,'executeStart:'+_.size(paramsList));
        //分批执行
        let batchCount=10000;
        let fns=[];
        for (let i=0;i<Math.ceil(_.size(paramsList) / batchCount);i++){
            let data=paramsList.slice(i * batchCount,(i + 1)  * batchCount);
            await setParams(statement,data,datatypes,{isSelect:false});
            conn.sqlSum.execute++;
            let nrows=await statement.executeBatch();//需要用executeBatch批量执行
            let rowcount=0;
            if (_.isNumber(nrows)){
                rowcount=nrows;
            }else{
                rowcount=_.sum(nrows); //注:nrow是java数组,而非instance Array
            }
            totalRowCount+=rowcount;
            showLog(conn,'executeBatch:'+rowcount);
        }
    }catch (e){
        err=await conn.getDBErr(e);
    }finally {
        if (statement) await statement.close();
    }
    if(err) return Promise.reject(err);
    showLog(conn,'Execute rowcount:'+' '+totalRowCount);
    return totalRowCount;
    // });
}
// const ExecuteAsync=util.promisify(Execute);

/**
 *
 * @param conn
 * @param sql {Array|String}
 * @param option {{emit}}
 * @returns {Number}
 */
async function ExecuteBatch(conn,sql,option) {
    let args = Array.from(arguments);
    // cb = args.pop();
    option = args[2];
    let {emit} = option || {};
    emit && emit('connect', conn.spid);
    let statement = await conn.connection.createStatement();
    // emit && emit('create');
    //支持sql数组顺序批量执行
    let nrows = 0;
    let err;
    try {
        if (sql instanceof Array) {
            let sqlcount = sql.length;
            for (let i = 0; i < sqlcount; i++) {
                showSql(conn, sql[i]);
                conn.sqlSum.execute++;
                let result = await statement.executeUpdate(sql[i]);//,(err,result)=>{
                showLog(conn, 'executeUpdate: ' + result);
                // if (emit && emit('execute',null,result,i)===false) return cont();
                nrows += result;
            }
        } else {
            showSql(conn, sql);
            conn.sqlSum.execute++;
            let result = await statement.executeUpdate(sql); //,(err,result)=>{
            showLog(conn, 'executeUpdate: ' + result);
            emit && emit('execute', null, result, 1);
            nrows += result;
        }
    } catch (e) {
        err = await conn.getDBErr(e);
    } finally {
        if (statement) await statement.close();
    }
    if (err) return Promise.reject(err);
    return nrows;
}
// const ExecuteBatchAsync=util.promisify(ExecuteBatch);

async function ExecuteDDL(conn,sql) /*目前不支持点位符*/ {
    let args = Array.from(arguments);
    // cb = args.pop();
    let autocommit_old;
    let err;
    let ret=await getAutoCommit(conn);
    try{
        if (ret === false) { //改为自提交
            await setAutoCommit(conn,true);
            autocommit_old = false;
        }
        ret=await ExecuteBatch(conn,sql);
    }catch(e){
        err=e;
    }finally{
        if (autocommit_old !== undefined) await setAutoCommit(conn,autocommit_old);
    }
    if (err) return Promise.reject(err);
    return ret;
}
// const ExecuteDDLAsync=util.promisify(ExecuteDDL);

async function Commit(conn){
    showSql(conn,'Commit');
    // if (conn._status==='closed') return cb(new Error(_.get(conn,'pool.group')+':Connection is closed!'));
    if (!conn.connection) return Promise.reject(new Error(_.get(conn,'pool.group')+':Connection is closed!')); //已被close
    return await conn.connection.commit();
}
// const CommitAsync=util.promisify(Commit);

async function Rollback(conn) {
    showSql(conn,'Rollback');
    // if (conn._status==='closed') return cb(new Error(_.get(conn,'pool.group')+':Connection is closed!'));
    if (!conn.connection) return Promise.reject(new Error(_.get(conn,'pool.group')+':Connection is closed!')); //已被close
    return await conn.connection.rollback();
}
// const RollbackAsync=util.promisify(Rollback);

/**
 *
 * @param conn {MyConnection}
 * @param value {boolean}
 */
async function setAutoCommit(conn,value) {
    // if(!cb){ //非回调
    //     return conn.connection.setAutoCommitSync(value);
    // }
    showSql(conn,'setAutoCommit-->'+value);
    return await conn.connection.setAutoCommit(value);
}
// const setAutoCommitAsync=util.promisify(setAutoCommit);

async function getAutoCommit(conn) {
    // if (!cb) return conn.connection.getAutoCommitSync(); //阻塞
    // 非阻塞
    return await conn.connection.getAutoCommit();
}
// const getAutoCommitAsync=util.promisify(getAutoCommit);

//column={top:0,fieds:['t1.outdate','t1.cusno','t1.nos'],datatype:['char(8)','varchar(10)','varchar(20)'],sort:['desc','asc','desc']};
//from="u2sale where codes=WX00000000027 ";

/**
 *
 * @param conn
 * @param column {{[top]:number,fieds:[],datatype:[],sort:[]}}
 * @param from
 * @param cb
 * @returns {*}
 * @constructor
 */
// function SelectLast(conn,column,from,cb){

//     let ls_sql="";
//     let ls_selectfieldstr='';
//     let ls_selectfieldstr2='';
//     let ls_sortstr="";
//     let ls_top="";
//     let addwhere='';
//     if (column.top) {
//         ls_top = ' top ' + String(column.top);
//         for (let i=0;i<column.fieds.length;i++){
//             ls_sql+="declare @var"+String(i)+' '+column.datatype[i]+'\n';
//             if (i>0){
//                 ls_selectfieldstr+=',';
//                 ls_selectfieldstr2+=',';
//                 ls_sortstr+=',';
//             }
//             ls_selectfieldstr +='@var'+String(i)+'='+column.fieds[i]+' ';
//             ls_selectfieldstr2+='@var'+String(i)+' as var'+String(i);
//             ls_sortstr+=column.fieds[i] +' '+ (column.sort[i] || '');
//             addwhere+=' and ({var'+String(i)+'}'+toCompare(column.sort[i])+'? ';
//             if (i<column.fieds.length - 1) addwhere+=' or ({var'+String(i)+'}=? ';
//         }
//         addwhere+=')'.repeat(column.fieds.length * 2 - 1);
//         ls_sql+="select "+ ls_top +" "+ls_selectfieldstr+' '+from+' order by '+ls_sortstr+' \n';
//         ls_sql+="select "+ls_selectfieldstr2+" where @@rowcount>=1 ";

//     }else{
//         return cb(null,[]); //top 0
//     }

//     function toCompare(sort){
//         if (sort.trim().toLowerCase()==='desc'){
//             return '<';
//         }
//         return '>';
//     }

//     Query(conn,ls_sql,(err,data)=>{
//         if (err) return conn.getDBErr(err,cb);
//         if (data.length===0){
//             return cb(null,[]);
//         }
//         cb(null,data[0],{addwhere:addwhere,orderby:ls_sortstr});
//     });
// }
// const SelectLastAsync=util.promisify(SelectLast);

/**
 * {selectlast-->newsql-->},result
 * 根据sort-->返回top语句-->结果-->再取top
 * @param conn {MyConnection}
 * @param baseSql {{select:string,keySort:object,where:object,aggregate,from}}
 * @param option {{select:string,where:object,sort:object,limit:[Array],[isolation],grid,into,from}}
 * @constructor
 */
async function QueryLimit(conn,baseSql,option) {
    //console.warn('QueryLimit:',baseSql.keySort,option.sort);
    let {isolation,into}=option;
    let appendSql = '';
    if (isolation != null) appendSql = conn.pool.adapter.appendIsolation(isolation);
    //let SQL='';
    let SELECT = sqlHelper.SELECT(option.select || _.keys(baseSql.keySort).join(','), column);

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
    let WHERE = sqlHelper.WHERE(option.where, column);
    let lastSort = sqlHelper.getSort(option.sort, baseSql.keySort);
    let SORT = sqlHelper.SORT(lastSort, column);

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

        if (_.get(conn, 'pool._config.dbms') === 'mysql') {
            sql = `SELECT ${SELECT} FROM ${option.from || baseSql.from} ${where.length ? ('WHERE ' + where.join(' AND ')) : ''}`;
            if (SORT) sql += ` ORDER BY ${SORT}`;
            if (fetchRow) {
                if (!startRow) { //未指定起始行,则取top
                    sql += " LIMIT " + String(fetchRow);
                } else {
                    sql += " LIMIT " + String(startRow - 1) + ',' + String(fetchRow);
                }
            }

            sql += appendSql;
            return await (option.grid ? QueryGrid : Query)(conn, sql);
        }

        //首页
        if (startRow <= 1) {
            sql = `SELECT ${fetchRow ? 'TOP ' + String(fetchRow) : ''} ${SELECT} ${into ? ("into "+into):''} FROM ${option.from || baseSql.from} ${where.length ? ('WHERE ' + where.join(' AND ')) : ''}`;
            if (SORT) sql += ` ORDER BY ${SORT}`;
            sql += appendSql;
            if (into) return await ExecuteDDL(conn,sql);
            return await (option.grid ? QueryGrid : Query)(conn, sql);
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

        sql += `\nSELECT TOP ${top} ${selectFieldStr} FROM ${option.from || baseSql.from} ${where.length ? ('WHERE ' + where.join(' AND ')) : ''}`;
        if (SORT) sql += ` ORDER BY ${SORT}`;
        sql += appendSql;
        //sql+="SELECT "+selectFieldStr2+" where @@rowcount>=1 ";

        if (addWhere) where.push(addWhere);

        sql += `\nSELECT TOP ${fetchRow} ${SELECT} ${into ? ("into "+into):''} FROM ${option.from || baseSql.from} ${where.length ? ('WHERE ' + where.join(' AND ')) : ''}`;

        if (SORT) sql += ` ORDER BY ${SORT}`;
        sql += appendSql;

        function toCompare(column, sort, value, isPK) {
            if (sort && sort.trim().toLowerCase() === 'desc') {
                return `${column} < ${value} ` + (!isPK ? `OR (${value} IS NOT NULL and ${column} IS NULL)` : '');
            }
            return `${column} > ${value} ` + (!isPK ? `OR ( ${value} IS NULL and ${column} IS NOT NULL)` : '');
        }

        if (into) return await ExecuteDDL(conn,sql);
        return await (option.grid ? QueryGrid : Query)(conn, sql);
    }
}
// const QueryLimitAsync=util.promisify(QueryLimit);

/**
 *
 * @param config {{[user],[password],[properties]:{user,password}}}
 * @param callback {function}
 * @returns {*}
 */
async function getJavaProps(config,callback) {
    let Properties = java.import('java.util.Properties');
    /**
     *
     * @type {{rs2table,rs2table2,setParams}}
     */
    nodeJdbc=java.import("node.jdbc"); //将test.class放在src/node里;
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
 * @param callback {function(Error=,*=)}
 */
async function _createConnection(config,callback){
    let url=config.url;
    // let props;
    // Thenjs((cont)=> {
    let props=await getJavaProps(config);
    // }).then((cont,result)=> {
        // props=result;
    let driver=await java.newInstance(config.drivername);
    // }).then((cont,driver)=>{
        
    await java.callStaticMethod('java.sql.DriverManager', 'registerDriver', driver);
        // dm.registerDriver(driver, cont);
    // }).then((cont)=>{
        // dm.getConnection(url, props,cont);
    return await java.callStaticMethod('java.sql.DriverManager', 'getConnection', url, props);
    // }).then((cont,conn)=>{
        // callback(null,conn);
    // }).fail((cont,err)=>{
        // if (err) return callback(err);
    // });
}

/**
 *
 * @param conn {MyConnection}
 * @returns {*}
 */
async function checkConnection(conn){
    if (conn._status!=='ok') { //
        let err=new Error('连接暂不可使用:'+conn._status);
        conn.logger.error(conn.spid+':',err);
        return Promise.reject(err);
    }

    return null;
}

function showSql(conn,sql,a) {
    conn.actived = (new Date()).getTime();
    // let args=Array.from(arguments);
    // let {logger}=_.get(conn,'pool'); //,group
    conn.lastSQL = sql;
    conn.logger.debug(conn.spid + ':', sql, a || ''); //group+':'+
}

function showLog(conn,text,a) {
    conn.logger.debug(conn.spid + ':', text, a || ''); //group+':'+
}


/**
 * @param resultSet
 * @param option {{grid:Boolean}}
 */
async function toObjArray(resultSet,option) {
    // let args=_.toArray(arguments);
    // callback=args.pop();
    // option=args[0];
    let dataGrid=option && option.grid;
    let self = resultSet;
    /**
     *
     * @type {[{label,type}]}
     */
    let metadata=[];
    let rsmd;
    let rowCount,fieldsCount;
    let gridData;

    let result=await java.callStaticMethod('node.jdbc', 'rs2table3', self);
    // let result=await nodeJdbc.rs2table3Sync(self); //self._rs,
    if (!result) return []; //callback(null, null); //无记录
    if (typeof result==='string'){
        gridData=JSON.parse(result);
    }else{
        gridData=result;
    }
    rowCount = _.size(gridData);
    if (dataGrid) return gridData; //callback(null, gridData); //返回表格数据
    // Get some column metadata.
    rsmd=await self.getMetaData();
    fieldsCount=await rsmd.getColumnCount();
    let parallelFn = [];
    for (let i = 1; i <= fieldsCount; i++) {
        metadata[i - 1] = {};
        parallelFn.push(
            rsmd.getColumnLabel(i).then((result)=> {
                metadata[i - 1].label = result;
            })
        );
        parallelFn.push(
            rsmd.getColumnType(i).then((result)=> {
                metadata[i - 1].type = result;
            })
        );
    }

    await Promise.all(parallelFn);
    
    let rows = [];
    for (let r = 0; r < rowCount; r++) {
        rows[r] = {};
        for (let c = 0; c < fieldsCount; c++) {
            rows[r][metadata[c].label] = gridData[r][c];
        }
    }
    return rows;

            // // console.time('fetch');
            // next((err,result)=>{
            //     // console.timeEnd('fetch');
            //     callback(err,result);
            // });
            //
            // function next(cb){
            //     fetch((err,row)=> {
            //         if (err) return cb(err);
            //         if (!row.done) {
            //             rows.push(row.value);
            //             return next(cb);
            //         }
            //         cb(null, rows);
            //     });
            // }
    //     }).fin((c, err, result)=> {
    //         cont(err,result);
    //     });
    // }).fin((c,err,result)=>callback(err,result));

    // function fetch(cb){
    //     let result = {};
    //     let rowResult=[];
    //     Thenjs((cont)=> {
    //         self._rs.next(cont);
    //     }).then((cont, nextRow)=> {
    //         if (!nextRow) return cb(null, {done: true});
    //         // option;
    //         let parallelFn = [];
    //         // loop through each column
    //         for (let i = 1; i <= fieldsCount; i++) {
    //
    //             let cmd = metadata[i - 1];
    //             let type = self._types[cmd.type] || 'String';
    //             let getter = 'get' + type; //+ 'Sync'
    //
    //             parallelFn.push((cont)=> {
    //                 // self._rs['getAsciiStream'](i,cont);
    //                 self._rs[getter](i, (err, dateVal)=> {
    //                     if (err) return cont(err);
    //                     switch (type) {
    //                         case 'BigDecimal'://nodeJava_java_math_BigDecimal
    //                             dateVal = Number(dateVal);
    //                             // result[cmd.label] = Number(dateVal);
    //                             break;
    //                         case 'Date':
    //                         case 'Time':
    //                         case 'Timestamp':
    //                             dateVal = dateVal && dateVal.toString();
    //                             // result[cmd.label] = dateVal && dateVal.toString();
    //                             break;
    //                         default:
    //                             // result[cmd.label] = dateVal;
    //                             break;
    //                     }
    //                     if (dataGrid) {
    //                         rowResult[i - 1] = dateVal;
    //                     } else {
    //                         result[cmd.label] = dateVal;
    //                     }
    //                     cont();
    //                 });
    //             });
    //         }
    //         // cont();
    //
    //         Thenjs.parallel(parallelFn).fin(cont);
    //     }).then((cont)=> {
    //         if (dataGrid){
    //             cb(null, {value: rowResult, done: false});
    //         }else {
    //             cb(null, {value: result, done: false});
    //         }
    //     }).fail((cont, err)=> {
    //         cb(err);
    //     });
    // }

}