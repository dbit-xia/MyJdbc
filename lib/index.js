
'use strict';
let jinst = require('../jdbc/lib/jinst');
let dm = require('../jdbc/lib/drivermanager');
let Connection=require('../jdbc/lib/connection');
let logTime=true;
let _ = require('lodash');
let ResultSet=require('./resultset'); //重写resultset的方法
let Thenjs=require("thenjs");
let sqlHelper=require('./SqlHelper');
let genericPool=require('generic-pool');
const path=require('path');
const uuid=require('uuid');
let nodeJdbc;
let MyUtil=require('./MyUtil');
const crypto=require('crypto');
let logger=global.logger || console;
const events=require('events');
let emitter=new events.EventEmitter();
let java = jinst.getInstance();

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
    if (!jinst.isJvmCreated()) {
        jinst.addOption("-Xrs");
        //let java=jinst.getInstance();
        jarList.push(path.join(__dirname,"../java/src"));
        jinst.setupClasspath(jarList);
    }
}

const spidSql= {
    default:'select @@spid as spid',
    mysql: "select spid=connection_id()",
    sybase: 'select spid=@@spid'
};

/**
 *
 */
class MyPool{
    constructor() {
        this._connections = [];
        this._config = null;
        this.pool = null;
        this._spidSql = 'select 1';
        return this;
    }

    getPoolStatus() {
        let pool = this.pool;
        return getPoolStatus(pool);
    }

    open(config,cb) {
        let _this = this;
        if (!config) {
            let err = new Error("未定义数据库连接参数:");
            cb && cb(err);
            return err;
        }
        this._spidSql=spidSql[config.dbms||'default'];
        let _config = _.cloneDeep(config);
        this._config = _config;
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

        const factory = {
            create: function () {
                // console.log('pool.create');
                return new Promise(function (resolve, reject) {
                    // console.debug('createConnection');
                    MyJdbc.createMyConnection(_config, (err, conn) => {
                        if (err) {
                            // throw new Error(err);
                            return reject(err);
                        }
                        conn.pool = _this;
                        Thenjs((cont)=>{
                            _isClosed(conn,cont);
                        }).then((cont)=>{
                            conn.setOption((err) => {
                                if (err) { //set失败则关闭连接
                                    MyJdbc.closeMyConnection(conn, (err) => {
                                        if (err) logger.error(err);
                                    });
                                    return cont(err);
                                }
                                resolve(conn);
                            });
                        }).fail((c,err)=>{
                            reject(err);
                        });
                    });
                })
            },
            destroy: function (conn) {
                return new Promise(function (resolve) {
                    MyJdbc.closeMyConnection(conn, (err) => {
                        if (err) return reject(err);
                        resolve();
                    });
                });
            },
            validate: function (conn) {
                return new Promise(function (resolve) {
                    _isClosed(conn, (err, value) => { //检查连接是否正常
                        return resolve(value === false); //正常返回true,否则返回false
                    });
                });
            }
        };

        let opts = {
            fifo: false, //优先使用最新获取的连接
            maxWaitingClients: 1000, //最大排队数
            testOnBorrow: true, //获取连接时自动检查状态
            idleTimeoutMillis: 600000, //设置10分钟认为是空闲进程
            //numTestsPerRun:3,
            acquireTimeoutMillis: 10000, //获取连接,30秒超时
            evictionRunIntervalMillis: 1000, //每1s检查一次空闲连接
            max: config.maxpoolsize, // maximum size of the pool
            min: config.minpoolsize // minimum size of the pool
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

        // //console.info(config.url);
        // let pool = new Jdbc(config);
        // pool.initialize(function (err) {
        //     //logger.error(err);
        //     if (err) return cb(err);
        //     console.info(config.url,"连接池初始化成功:", _this.getPoolStatus(pool));
        //     cb && cb(null, pool);
        // });
        _this.pool = pool; //.__proto__

        let createErrorCount = 0;
        pool.on('factoryCreateError', function (err) {
            createErrorCount++;
            // console.log(createErrorCount);
            logger.error('factoryCreateError', err);
            let dequeue = pool._waitingClientsQueue.dequeue();
            if (_.has(dequeue, 'reject')) dequeue.reject(err); //从排队中移除一个,避免bug死循环
            // logger.error(err);
            // throw err;
        });

        //等待初始化完成
        let handle = setInterval(() => {
            if ((pool.available + pool.borrowed + createErrorCount) >= pool.min) {
                // _this.getConnection((err,conn)=>{
                //     console.log(_this.getPoolStatus(pool));
                // });
                // logger.info(config.url, "连接池初始化成功:", _this.getPoolStatus(pool));
                clearInterval(handle);
                cb(null, pool);
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
 * @param outConn
 * @param cb {function(*=,MyConnection=)}
 * @returns {*}
 */
MyPool.prototype.getConnection=function (outConn, cb){
    // console.debug('getConnection');
    let args = _.toArray(arguments);
    cb = args.pop();
    outConn=args[0];
    let _this=this;
    let pool=_this.pool;

    if (outConn && outConn._status) return cb && cb(null,outConn); //原有连接

    if (!pool) return cb('连接池未初化!');
    // Thenjs((cont)=>{
    pool.acquire(0).then((conn)=> {
        _this._connections.push(conn);
        conn._status='ok';
        cb(null, conn); //conn {MyConnection}
        // Thenjs((cont)=> {
        //     _isClosed(conn, cont);
        // }).then((cont,value)=>{
        //     if (!value) return cb(null,conn); //连接正常
        //     createConnection(_this._config,cont); //重新获取基础连接,/*已关闭,重连*/
        // }).then((cont,result)=>{
        //     conn.setConnection(new Connection(result));
        //     cb(null, conn);
        // }).fail((cont,err)=>{
        //     cb(err);
        // });
    }).catch((err)=>{
        cb(err);
    });
    // }).fin((c,err,result)=>{
    //     cb(err,result);
    // });

    // if (outConn && outConn._status) return cb && cb(null,outConn); //原有连接
    // return pool.reserve(function(err,connection){
    //     if (err){
    //         return cb && cb(getDBErr(err));
    //     }
    //     if (_isClosed(connection)){ /*已关闭,重连*/
    //         return pool.delete(connection,()=>{ //弃用
    //             return _this.getConnection(outConn,cb); //重连
    //         });
    //     }
    //     if (!outConn) outConn=new MyConnection({pool:_this,title:'new'}); //创建一个connect
    //     //outconn.pool=pool;
    //     outConn.setConnection(connection); //
    //     return cb && cb(null,outConn);
    // });
};

/**
 * 归还一个连接
 * @param conn {MyConnection}
 * @param cb {function}
 */
MyPool.prototype.releaseConn=function (conn, cb){
    let _this=this;
    let pool=_this.pool;
    conn.setAppInfo(null);
    // showSql(conn,'Release');
    // conn._status=null;
    pool.release(conn).then(function(){
        for (let i=0;i<_.size(_this._connections);i++){
            if (_this._connections[i]===conn) {
                _this._connections.splice(i,1);
                break;
            }
        }
        cb()
    }).catch(function(err){
        cb(err);
    });
    //pool.release(conn.connection,cb);
};

/**
 * 连接数据库,cb是处理数据的函数,cb2是输出函数
 * 执行第一个回调函数(err,conn,(err,result,iscommit)=>{})-->提交-->再回调cb2(err,result)进行输出
 * @param fn {function(conn:MyConnection,cb:Function)}
 * @param output {function(*=,*=)}
 * @param [opt] {{[commit],[connection]: MyConnection}}
 */
MyPool.prototype.autoConnect=function(fn, output,opt) {
    let isNew=true; /*默认为用新连接*/

    this.getConnection(connCallback);
    /**
     *
     * @param err
     * @param conn {MyConnection}
     * @returns {*}
     */
    function connCallback(err,conn) {
        if (err) {
            // if (opt && opt.connection && opt.connection != conn) {
            //     console.warn("连接失败" , err , "使用默认连接!");
            //     conn = opt.connection;
            //     isNew=false;
            //     return conn.Connect(connCallback);
            // } else {
            output && output(err);
            return;
            //}
        }

        Thenjs((cont)=> {
            conn.setAutoCommit(false, cont);
        }).then((cont)=>{
            fn( conn, cont);
        }).fin((cont,err0,result)=> {
            conn._status='releasing';
            // if (_.get(opt, 'commit') || _.get(childOpt, 'commit')) { //是否提交
            Thenjs((cont)=> {
                if (err0) return cont(err0); //存在错误回滚
                conn.Commit(cont);
            }).then(()=> {
                cont(err0 != null ? err0:null, result);
            }).fail((c, err1)=> {
                conn.Rollback((err2)=> {
                    cont(err0 != null ? err0 : (err1 || err2));
                });
            });
            // } else {
            //     return cont(err0, result);
            // }
        }).fin((cont,err,result)=>{
            if (isNew) {
                return conn.Disconnect((err2)=>{
                    output && output(err!=null ? err : err2,result);
                });
            }
            output && output(err, result);
        });
    }
};

class MyConnection {
    /**
     *
     * @param option {{[title]:string,[pool]:MyPool}}
     */
    constructor(option) {
        this._appInfo=null;
        this._status = null;
        this.connection = {};//=?PoolConnection();
        this.option=option;
        /**
         *
         * @type {MyPool}
         */
        this.pool=_.get(option,'pool');
        this.postEvents=[];
        this.spid=null; //{Number}
        this.Title = _.get(option,'title');
        return this;
    }

    /**
     *
     * @param connection
     */
    setConnection(connection) {
        //if (arguments.length === 0) {
        //    return _this.connection;
        //}
        this.Title = this.option.title + '_' + String(connection.uuid).substr(0, 8);
        this.connection = connection;
        this._status = 'ok';
    }

    /**
     * 使用当前Connection进行连接
     * @param cb {function(*=,MyConnection=)}
     */
    Connect(cb) {
        let _this=this;
        let pool = this.pool;
        if (this._status) {
            _isClosed(this,(err,value)=> {
                if (!value) return cb(null, _this); //返回当前已连接
                _this.Disconnect(()=>{ //释放
                    return _this.Connect(cb); //重连
                });
            });
            return ;
        }
        if (!pool) return cb('连接池还未初始化!');
        showSql(this,"Connect");
        pool.getConnection(this, cb); //重新连接
    }

    /**
     * 归还当前连接
     * @param cb
     * @constructor
     */
    Disconnect(cb) {
        let pool = this.pool;
        let conn = this;
        //connection.end();
        pool.releaseConn(conn, (err)=> {

            if(err) {
                logger.error("Release:", err);
            }else{
                showSql(conn,"Release:ok");
            }
            conn._status = null;
            cb && cb(err);
        });
    }

    Close(cb){
        this._status='closed';
        this.connection.close(cb);
    }

    setOption(cb){
        let dbms=_.get(this,'pool.config.dbms') || '';
        switch (dbms) {
            case '':
            case 'sybase':
            case 'sql':
                let sqls = ["set forceplan on"];
                MyUtil.eachLimit(sqls, (cont, sql) => {
                    this.ExecuteDDL(sql, cont);
                }, 10, cb);
                break;
            default:
                cb();
                break;
        }
    }

    /**
     * 添加应用信息 (自定义信息)
     * @param appInfo {*}
     */
    setAppInfo(appInfo) {
        this._appInfo = appInfo;
        this.postEvents=[];
    }

    /**
     * 获取应用信息
     * @returns {*}
     */
    getAppInfo(){
        return this._appInfo;
    }

    Query(sql, paramsList, skipCount, cb) {
        let args = _.toArray(arguments);
        cb = _.last(args);
        let err = checkConnection(this);
        if (err) return cb(err);
        return Query.apply(this, [this].concat(args));
    }

    QueryGrid(sql, paramsList, skipCount, cb){
        // let err=checkConnection(this);
        // if (err) return cb(err);
        let args = _.toArray(arguments);
        return QueryGrid.apply(this,[this].concat(args));
    }

    QueryPart(sql, paramsList, skipCount, cb){
        let args = _.toArray(arguments);
        cb = _.last(args);
        let err=checkConnection(this);
        if (err) return cb(err);
        return QueryPart.apply(this,[this].concat(args));
    }
    QueryLimit(baseSql,option,cb){
        let args = _.toArray(arguments);
        cb = _.last(args);
        let err=checkConnection(this);
        if (err) return cb(err);
        return QueryLimit.apply(this,[this].concat(args));
    }
    Execute(sql, paramsList,datatypes, cb){
        let args = _.toArray(arguments);
        cb = args.pop();
        let err=checkConnection(this);
        if (err) return cb(err);

        let _this=this;
        let isCallback=false;
        //超时断开
        setTimeout(()=>{
            if (isCallback===false) {
                _this.Close(()=>{
                    isCallback=true;
                    cb('超时了:'+sql+' '+MyUtil.String(_.get(_this,'session.souceUrl')));
                });
            }
        },2 * 60 * 1000);

        return Execute.apply(this,[this].concat(args,(err,result)=>{
            if (_this.status==='closed') return logger.error('超时已被关闭!');
            isCallback=true;
            cb(err,result);
        }));
    }
    ExecuteDDL(sql, cb){
        let args = _.toArray(arguments);
        cb = _.last(args);
        let err=checkConnection(this);
        if (err) return cb(err);
        return ExecuteDDL.apply(this,[this].concat(args));
    }
    Commit(cb){
        // let err=checkConnection(this);
        // if (err) return cb(err);
        let args = _.toArray(arguments);
        return Commit.apply(this,[this].concat(args));
    }
    Rollback(cb){
        // let err=checkConnection(this);
        // if (err) return cb(err);
        let args = _.toArray(arguments);
        return Rollback.apply(this,[this].concat(args));
    }

    /**
     *
     * @param value {boolean}
     * @param cb {function(*=,*=)}
     * @returns {*}
     */
    setAutoCommit(value, cb){
        let args = _.toArray(arguments);
        cb = _.last(args);
        let err=checkConnection(this);
        if (err) return cb(err);
        return setAutoCommit.apply(this,[this].concat(args));
    }
    getAutoCommit(cb){
        let args = _.toArray(arguments);
        return getAutoCommit.apply(this,[this].concat(args));
    }
    SelectLast(column, from, cb){
        let err=checkConnection(this);
        if (err) return cb(err);
        let args = _.toArray(arguments);
        return SelectLast.apply(this,[this].concat(args));
    }
}

//MyConnection.prototype.Query=function(sql, paramsList, skipCount, cb){
//    let args = _.toArray(arguments);
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
MyJdbc.createMyConnection=function (config, callback){
    createConnection(config,(err,conn)=>{
        if (err) return callback(err);
        let myconn=new MyConnection({});
        myconn.setConnection(new Connection(conn));
        callback(null,myconn);
    });
};

/**
 *
 * @param conn {MyConnection}
 * @param callback {function}
 */
MyJdbc.closeMyConnection=function(conn, callback){
    showSql(conn,'closeConnection');
    conn.Close(callback);
};

module.exports=MyJdbc;

function time(conn,data) {
    if (logTime) {
        let startTime=new Date().getTime();
        emitter.once(data,()=>{
            showSql(conn,data,(new Date().getTime() - startTime)+'ms');
        });
    }
}
function timeEnd(data) {
    logTime && emitter.emit(data);//console.timeEnd(data);
}

function getPoolStatus(pool){
    return {
        pool: pool.available,
        reserve: pool.borrowed,
        pending: pool.pending,
        max: pool.max,
        min: pool.min,
        size:pool.size
    };
}
/**
 *
 * @param conn {MyConnection}
 * @param cb {function}
 * @private
 */
function _isClosed(conn,cb){
    try{
        conn.QueryGrid(conn.pool._spidSql,(err,result)=>{
            if (err) return cb(null,true); //已断开 或 getSQLStateSync()==='08S01'
            conn.spid=result[0][0];
            // showSql(conn);
            cb(null,false);//未断开
        });
    }catch (err){
        cb(null,true); //已断开
    }
    //return myConnection && myConnection.conn && connection._conn && connection._conn.isClosedSync();
}

/**
 *
 * @param err {{cause}}
 * @param cb
 */
function getDBErr(err,cb){
    if (!err) return cb(); //无错误
    if(_.isError(err)===false) {
        return cb(err);
    }

    if (_.get(err,'cause.getErrorCode')) {
        let shortError={};
        Thenjs.each(['getErrorCode', 'getMessage','getSQLState'], (cont, name) => {
            err.cause[name]((err,result)=>{
                if (err) return cont(err);
                shortError[name.slice(3)]=result;
                cont();
            });
        }).fin((c, err) => {
            logger.error(err||shortError);
            cb(err || shortError);
        });
        return;
    }
    return cb(err);

    // let code=err.sqlDBCode || (_.get(err,'cause.getErrorCodeSync') || (()=>null)).call(err.cause) || -1;
    // let text=err.sqlErrText /*|| (_.get(err,'cause.getMessageSync') || (()=>null)).call(err.cause)*/ || toString(err);
    // return JSON.stringify({
    //     sqlDBCode:code,
    //     sqlErrText:text
    // }); //getStack:()=>(err.message || err.stack || err)
}

function setFetch(statement,cb){
    //if (err) return getDBErr(err,cb);
    statement.setFetchSize(100, (err)=> {
        if (err) return getDBErr(err,cb);
        //statement.setFetchDirection(1001, (err)=> {
        //log('setMaxRows:' + ret)
        //if (err) return cb(err);
        //statement.setMaxRows(10, (err, ret)=> {
        //if (err) return cb(err, ret);
        //statement.getFetchDirection((err,ret)=>{
        //if (err) return cb(err, ret);
        //log('getFetchDirection:'+ret);
        cb(null);
        //});
        //});
        //});
    });
}


/**
 * JDBC数据类型常量
 */
const SQL_TYPES={
    BOOLEAN:16,
    DATE:91,
    NUMERIC:2,
    BIGDECIMAL:3,
    DOUBLE:8,
    FLOAT:6,
    INTEGER:4,
    INT:4,
    VARCHAR:12,
    CHAR:12,
    STRING:12,
    NUMBER:6
};

/**
 *
 * @param statement {PreparedStatement}
 * @param rows {Array} 一维/二维数组
 * @param datatypes {Array}
 * @param option {{[isSelect]}}
 * @param cb {function}
 */
function setParams(statement,rows,datatypes,option,cb) {
    // console.debug(rows,datatypes);
    /**
     * 1.获取类型String数组
     * 2.获取类型函数数组
     * 3.循环取出参数,并行进行setInt,然后执行addbatch
     */

    if ((rows instanceof Array)===false) return cb('第二个参数必须是数组!');
    if (_.size(rows)===0) return cb('第二个参数不能是空数组!');
    if (rows[0] instanceof Array===false) {
        rows=[rows]; //转换成二维数组
    }

    if (!_.size(datatypes)) datatypes=sqlHelper.DATATYPE(rows[0]);

    // let setFns=[];
    let nTypes=[];
    for (let i= 0,total=_.size(datatypes);i<total;i++){
        // let fn= statement._ps["set" + (datatypes[i])]; //.charAt(0).toUpperCase())+datatypes[i].slice(1).toLowerCase()
        // if (!fn){
        //     return cb('无效的方法:set'+datatypes[i]);
        // }
        // setFns.push(fn.bind(statement._ps)); //预绑定参数,必须要绑定_ps,否则node会直接退出

        let types = SQL_TYPES[datatypes[i].toUpperCase()];
        if (!types) {
            console.error(rows,datatypes);
            return cb('无效的java.sql.Types:'+datatypes[i])
        }
        nTypes.push(types); //转换成常量数字值
    }
    // console.debug(nTypes,__filename);
    return nodeJdbc.setParams(statement._ps,rows,nTypes,option.isSelect,cb);

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
 * @param cb
 * @constructor
 */
function QueryBase(conn,option,cb) { /*paramsList SQL参数列表*/
    let sql=option.sql;
    let paramsList=option.paramsList;
    let datatypes=option.datatypes;
    let grid=option.grid;
    let _statement;
    let label='Query:'+uuid.v1().slice(0,8);
    time(conn,label);

    Thenjs((cont)=> {
        showSql(conn,sql);
        if (paramsList){ /*有参数*/
            Thenjs((cont)=> {
                conn.connection.prepareStatement(sql, cont);
            }).then((cont,statement)=>{
                _statement=statement;
                setParams(_statement, paramsList,datatypes,{isSelect:true}, (err)=> {
                    if (err) return cont(err);
                    _statement.executeQuery(cont);
                });
                // }).then((cont)=>{
                //     _statement._ps.getResultSet(cont);
                // }).then((cont,_rs)=>{
                //     cont(null,new ResultSet(_rs));
            }).fin(cont);
        }else { /*无参数*/
            Thenjs((cont)=> {
                //jtds 用1005有问题
                //jconn4 可以用1005
                conn.connection.createStatement(cont);
            }).then((cont,statement)=> {
                _statement = statement;
                cont();
                // setFetch(_statement,cont);
            }).then((cont)=>{
                _statement.executeQuery(sql,cont);
            }).fin(cont);
        }
    }).then((cont,rows, fields)=> {
        // let _label='toObjArray:'+uuid.v1().slice(0,8);
        // time(conn,_label);
        rows.toObjArray({grid,statement:_statement},(err, results)=> {
            // timeEnd(_label);
            if (err) return cont(err);
            showSql(conn,"Fetch rowcount:" + _.size(results));
            cont(null, results||[], fields);
        });
    }).fin((cont,err,a, b)=>{
        if (_statement) return _statement.close((err2)=> {
            if (err || err2) return cont(err || err2);
            cont(null,a, b);
        });
        cont(err,a, b);
    }).fin((cont,err,ret)=>{
        timeEnd(label);
        if (err) return getDBErr(err,cb);
        cb(null, ret);
    });
}

function Query(myConn,sql,paramsList,datatypes,cb) { /*paramsList SQL参数列表*/
    let args = _.toArray(arguments);
    cb = args.pop();
    paramsList=args[2];
    datatypes=args[3];
    QueryBase(myConn,{sql,paramsList,datatypes},cb);
}

function QueryGrid(myConn,sql,paramsList,datatypes,cb) { /*paramsList SQL参数列表*/
    let args = _.toArray(arguments);
    cb = args.pop();
    paramsList=args[2];
    datatypes=args[3];
    QueryBase(myConn,{sql,paramsList,datatypes,grid:true},cb);
}

function QueryPart(conn,sql,paramsList,skipCount,cb) { /*paramsList SQL参数列表,skipCount 忽略的行数 为可选参数*/
    let args = _.toArray(arguments);
    cb = args.pop();
    paramsList && paramsList.length>0 && console.warn("QueryPart不支持指定参数列表!");
    conn.connection.createStatement(1004,1007,function(err, statement) {
        if (err) return getDBErr(err,cb);
        setFetch(statement,(err)=> {
            execute(err,statement);
        });
    });

    function execute(err,statement) {
        if (err) return getDBErr(err,cb);
        time('executeQuery');
        statement.executeQuery(sql, function (err, resultset, fields) {
            timeEnd('executeQuery');
            showSql(conn,"End Query:" + sql);
            if (err) {
                //logger.error("query:" + err);
                return getDBErr(err,cb);
            }
            time(conn,'absolute');
            resultset.absolute(skipCount,(err)=>{ /*跳转到指定index*/
                timeEnd('absolute');
                if (err) return getDBErr(err,cb);
                time(conn,'toObjArray');
                resultset.toObjArray((err, results)=> {
                    timeEnd('toObjArray');
                    if (err) return getDBErr(err,cb);
                    showSql(conn,"Fetch rowcount:" + results.length);
                    cb && cb(null, results||[], fields);
                }, 0);
            });
        });
    }
}
function Execute(myConn,sql,paramsList,datatypes,cb) /*目前不支持点位符*/ {
    let args = _.toArray(arguments);
    cb = args.pop();
    paramsList=args[2];
    datatypes=args[3];

    if (sql instanceof Array || !_.size(paramsList) ) { //支持sql数组顺序批量执行
        if (paramsList) console.warn('目前仅支持无参数的SQL队列!');
        return ExecuteBatch(myConn,sql,cb);
    }
    let _statement;
    Thenjs((cont)=> {
        showSql(myConn,sql); //paramsList
        myConn.connection.prepareStatement(sql, cont);
    }).then((cont,statement)=> {
        _statement=statement;

        //无参数时
        if (!_.size(paramsList)){
            return statement._ps.executeUpdate(cont); //executeUpdate
        }
        showSql(myConn,_.size(paramsList));
        //分批执行
        let batchCount=10000;
        let fns=[];
        for (let i=0;i<Math.ceil(_.size(paramsList) / batchCount);i++){
            fns.push((cont)=>{
                Thenjs((cont)=>{
                    let data=paramsList.slice(i * batchCount,(i + 1)  * batchCount);
                    showSql(myConn,(i * batchCount)+' '+((i + 1)  * batchCount)+' '+_.size(data));
                    setParams(statement,data,datatypes,{isSelect:false},cont);
                }).then((cont)=>{
                    statement._ps.executeBatch(cont);//需要用executeBatch批量执行
                }).then((c,nrows)=>{
                    let rowcount=0;
                    if (_.isArray(nrows)){
                        rowcount=_.sum(nrows);
                    }else{
                        rowcount=nrows;
                    }
                    cont(null,rowcount);
                }).fail(cont); //同一个statement只能用串行
            });
        }
        //分批后顺序执行
        Thenjs.series(fns).then((c,nrows)=>{
            let rowcount=0;
            if (_.isArray(nrows)){
                rowcount=_.sum(nrows);
            }else{
                rowcount=nrows;
            }
            cont(null,rowcount);
        }).fail(cont);
    }).fin((cont,err,ret)=> {
        if (_statement) return _statement.close((err2)=> {
            if (err || err2) return cont(err || err2);
            cont(null,ret);
        });
        cont(err,ret);
    }).fin((cont,err,ret)=>{
        if (err) {
            logger.error(sql);
            return getDBErr(err,cb);
        }
        showSql(myConn,'Execute rowcount:'+' '+ret);
        cb(null, ret);
    });
}

/**
 *
 * @param myConn
 * @param sql {Array|String}
 * @param cb
 * @constructor
 */
function ExecuteBatch(myConn,sql,cb){
    let args = _.toArray(arguments);
    cb = args.pop();

    let _statement;
    Thenjs((cont)=> {
        myConn.connection.createStatement(cont);
    }).then((cont,statement)=> {
        _statement=statement;
        //支持sql数组顺序批量执行
        if (_.isArray(sql)){
            let sqlcount=sql.length;
            let series=[];
            for (let i=0;i<sqlcount;i++){
                series.push((cont)=>{
                    showSql(myConn,sql[i]);
                    _statement.executeUpdate(sql[i],(err,result)=>{
                        if (err) {
                            logger.error(sql[i]);
                            return getDBErr(err,cont);
                        }
                        showSql(myConn,'ExecuteBatch rowcount: '+result);
                        cont(null,result);
                    });
                });
            }
            Thenjs.series(series).fin((c,err,results)=>{
                if (err) return cont(err);
                return cont(null,_.sum(results)); //取影响行数之和
            }); //串行
        }else{
            showSql(myConn,sql);
            _statement.executeUpdate(sql,cont); //单条SQL
        }
    }).fin((cont,err,ret)=> {
        if (_statement) {
            return _statement.close((err2)=> {
                if (err || err2) return cont(err || err2);
                return cont(null,ret);
            });
        }
        cont(err,ret);
    }).fin((cont,err,ret)=>{
        if (err) return getDBErr(err,cb);
        cb(null, ret);
    });
}


function ExecuteDDL(myConn,sql,cb) /*目前不支持点位符*/ {
    let args = _.toArray(arguments);
    cb = args.pop();
    let autocommit_old;

    Thenjs((cont)=> {
        getAutoCommit(myConn,cont);
    }).then((cont,ret)=> {
        if (ret === false) { //改为自提交
            setAutoCommit(myConn,true, (err)=> {
                if (err) return cont(err);
                autocommit_old = false;
                cont(null);
            });
            return;
        }
        cont(null); //传递statement
    }).then((cont)=> {
        // console.info(sql);
        ExecuteBatch(myConn,sql,cont);
    }).fin((cont,err,ret)=> {
        if (autocommit_old !== undefined) {
            setAutoCommit(myConn,autocommit_old, (err2)=> {
                cb(err||err2);
            });
            return;
        }
        cb(err,ret);
    });
}
function Commit(myConn,cb){
    showSql(myConn,'Commit');
    myConn.connection.commit(function(err,ok) {
        if (err) {
            cb && getDBErr(err,cb);
            return;
        }
        cb && cb(null, ok);
    });
}
function Rollback(myConn,cb) {
    showSql(myConn,'Rollback');
    if (this._status==='closed') return cb();
    myConn.connection.rollback(function(err,ok) {
        if (err) {
            cb && getDBErr(err,cb);
            return;
        }
        cb && cb(null,ok);
    });
}
/**
 *
 * @param myConn {MyConnection}
 * @param value {boolean}
 * @param cb {function(*=,*=)}
 */
function setAutoCommit(myConn,value,cb) {
    if(!cb){ //非回调
        return myConn.connection._conn.setAutoCommitSync(value);
    }
    myConn.connection.setAutoCommit(value,function(err,ok) {
        if (err) {
            cb && getDBErr(err,cb);
            return;
        }
        cb && cb(null,ok);
    });
}
function getAutoCommit(myConn,cb) {
    if(!cb) return myConn.connection._conn.getAutoCommitSync(); //阻塞
    // 非阻塞
    return myConn.connection._conn.getAutoCommit((err,ret)=>{
        if (err) return getDBErr(err,cb);
        cb(null,ret);
    });
}

//column={top:0,fieds:['t1.outdate','t1.cusno','t1.nos'],datatype:['char(8)','varchar(10)','varchar(20)'],sort:['desc','asc','desc']};
//from="u2sale where codes=WX00000000027 ";

/**
 *
 * @param myConn
 * @param column {{[top]:number,fieds:[],datatype:[],sort:[]}}
 * @param from
 * @param cb
 * @returns {*}
 * @constructor
 */
function SelectLast(myConn,column,from,cb){

    let ls_sql="";
    let ls_selectfieldstr='';
    let ls_selectfieldstr2='';
    let ls_sortstr="";
    let ls_top="";
    let addwhere='';
    if (column.top) {
        ls_top = ' top ' + String(column.top);
        for (let i=0;i<column.fieds.length;i++){
            ls_sql+="declare @var"+String(i)+' '+column.datatype[i]+'\n';
            if (i>0){
                ls_selectfieldstr+=',';
                ls_selectfieldstr2+=',';
                ls_sortstr+=',';
            }
            ls_selectfieldstr +='@var'+String(i)+'='+column.fieds[i]+' ';
            ls_selectfieldstr2+='@var'+String(i)+' as var'+String(i);
            ls_sortstr+=column.fieds[i] +' '+ (column.sort[i] || '');
            addwhere+=' and ({var'+String(i)+'}'+toCompare(column.sort[i])+'? ';
            if (i<column.fieds.length - 1) addwhere+=' or ({var'+String(i)+'}=? ';
        }
        addwhere+=')'.repeat(column.fieds.length * 2 - 1);
        ls_sql+="select "+ ls_top +" "+ls_selectfieldstr+' '+from+' order by '+ls_sortstr+' \n';
        ls_sql+="select "+ls_selectfieldstr2+" where @@rowcount>=1 ";

    }else{
        return cb(null,[]); //top 0
    }

    function toCompare(sort){
        if (sort.trim().toLowerCase()==='desc'){
            return '<';
        }
        return '>';
    }

    Query(myConn,ls_sql,(err,data)=>{
        if (err) return getDBErr(err,cb);
        if (data.length===0){
            return cb(null,[]);
        }
        cb(null,data[0],{addwhere:addwhere,orderby:ls_sortstr});
    });
}

/**
 * {selectlast-->newsql-->},result
 * 根据sort-->返回top语句-->结果-->再取top
 * @param myConn {MyConnection}
 * @param baseSql {{select:string,keySort:object,where:object,aggregate}}
 * @param option {{select:string,where:object,sort:object,limit:[Array]}}
 * @param cb
 * @constructor
 */
function QueryLimit(myConn,baseSql,option,cb){
    //console.warn('QueryLimit:',baseSql.keySort,option.sort);

    //let SQL='';
    let SELECT=sqlHelper.SELECT(option.select||Object.keys(baseSql.keySort).join(','),column);

    if (option.where) {
        if (_.isString(option.where)) {
            let where=MyUtil.parseJSON(option.where); //解析string条件
            if (_.isError(where)) {
                return cb(where);
            }
            option.where=where;
        }
        if (_.isObject(option.where) === false) {
            return cb(new Error('where需要传入Object或JSON字符串!'));
        }
    }
    let WHERE=sqlHelper.WHERE(option.where,column);
    let lastSort=sqlHelper.getSort(option.sort,baseSql.keySort);
    let SORT=sqlHelper.SORT(lastSort,column);

    selectLastSql();
    //console.log('select',SELECT,'from',baseSql.from,'where',baseSql.where,'and',WHERE,'group by',baseSql.groupBy,'order by',SORT);
    /**
     *
     * @param key
     * @returns {string}
     */
    function column(key){
        let expression=(baseSql.select && baseSql.select[key]) || (baseSql.aggregate && baseSql.aggregate[key]);
        if (typeof expression==='string'){
            return expression;
        }
        if (expression instanceof Array){
            return expression[0] || key;
        }
        //console.warn('未定义的字段:',key);
        return key;
    }

    /**
     * @return {string}
     */
    function DATATYPE(key){
        let dataType='varchar(100)';
        let expression=(baseSql.select && baseSql.select[key]) || (baseSql.aggregate && baseSql.aggregate[key]);
        if (expression instanceof Array){
            return expression[1] || dataType;
        }
        return dataType;
    }

    /**
     *
     */
    function selectLastSql(){

        let sql='';
        let where=[];

        if (baseSql.where && baseSql.where.trim()) where.push(baseSql.where);
        if (WHERE) where.push(WHERE);

        let startRow=0;
        let fetchRow=0;
        if (typeof option.limit==='number') {
            fetchRow=option.limit;
        }else if (option.limit instanceof Array){
            if (option.limit.length>=2 ) {
                startRow = option.limit[0] || 0;
                fetchRow = option.limit[1] || 0;
            }else{
                startRow = option.limit[0] || 0;
            }
        }

        if (_.get(myConn,'pool.config.dbms')==='mysql'){
            sql=`SELECT ${SELECT} FROM ${baseSql.from} ${where.length ? ('WHERE ' + where.join(' AND ')) : ''}`;
            if (SORT) sql+=` ORDER BY ${SORT}` ;
            if (fetchRow) {
                if (!startRow) { //未指定起始行,则取top
                    sql+=" LIMIT "+String(fetchRow);
                }else{
                    sql+=" LIMIT "+String(startRow - 1)+','+String(fetchRow);
                }
            }

            return Query(myConn,sql,cb);
        }

        //首页
        if (startRow<=1){
            sql=`SELECT ${fetchRow?'TOP '+String(fetchRow):''} ${SELECT} FROM ${baseSql.from} ${where.length ? ('WHERE ' + where.join(' AND ')) : ''}`;
            if (SORT) sql+=` ORDER BY ${SORT}` ;
            return Query(myConn,sql,cb);
        }

        let selectFieldStr=''; //,selectFieldStr2=''
        let addWhere='';
        let top=startRow - 1; //先取出top的最后一行

        let sortKeys= _.keys(lastSort);
        if (_.size(sortKeys)===0) sortKeys=SELECT.split(','); //未传入排序,则默认select的全部

        sql='';
        for (let i=0;i<sortKeys.length;i++){
            let varName='@'+sortKeys[i];
            sql+=`DECLARE ${varName} ${DATATYPE(sortKeys[i])} \n`;
            if (i>0){
                selectFieldStr+=',';
                //selectFieldStr2+=',';
                addWhere+=' AND ';
            }
            selectFieldStr +=`${varName}=${column(sortKeys[i])}` ;
            //selectFieldStr2+=`${vname} as ${sortKeys[i]}`;
            addWhere+='('+toCompare(column(sortKeys[i]),lastSort[sortKeys[i]],varName,_.get(baseSql,'keySort.'+sortKeys[i]));
            if (i<sortKeys.length - 1) addWhere+=` OR (${column(sortKeys[i])}=${varName} `;
        }
        if(sortKeys.length) addWhere+=')'.repeat(sortKeys.length * 2 - 1)+'';

        sql+=`\nSELECT TOP ${top} ${selectFieldStr} FROM ${baseSql.from} ${where.length ? ('WHERE ' + where.join(' AND ')) : ''}`;
        if (SORT) sql+=` ORDER BY ${SORT}` ;
        //sql+="SELECT "+selectFieldStr2+" where @@rowcount>=1 ";

        if (addWhere) where.push(addWhere);

        sql+=`\nSELECT TOP ${fetchRow} ${SELECT} FROM ${baseSql.from} ${where.length ? ('WHERE ' + where.join(' AND ')) : ''}`;

        if (SORT) sql+=` ORDER BY ${SORT}` ;

        function toCompare(column,sort,value,isPK){
            if (sort && sort.trim().toLowerCase()==='desc'){
                return `${column} < ${value} `+ (!isPK ? `OR (${value} IS NOT NULL and ${column} IS NULL)` : '');
            }
            return `${column} > ${value} ` + (!isPK ? `OR ( ${value} IS NULL and ${column} IS NOT NULL)` : '');
        }

        //console.log(sql);
        Query(myConn,sql,cb);

    }
}


/**
 *
 * @param config {{[user],[password],[properties]:{user,password}}}
 * @param callback {function}
 * @returns {*}
 */
function getJavaProps(config,callback) {
    let Properties = java.import('java.util.Properties');
    /**
     *
     * @type {{rs2table,rs2table2,setParams}}
     */
    nodeJdbc=java.import("node.jdbc"); //将test.class放在src/node里;
    ResultSet.prototype.nodeJdbc=nodeJdbc;

    let properties= new Properties();
    let keys=Object.keys(config.properties);
    Thenjs.each(keys,(cont,value)=>{
        properties.put(value,config.properties[value],cont);
    }).fin((cont,err)=>{
        callback(err,properties);
    });
}

/**
 *
 * @param config config {{url,drivername,[user],[password],[properties]:{user,password}}}
 * @param callback {function(Error=,*=)}
 */
function createConnection(config,callback){
    let url=config.url;
    let props;
    Thenjs((cont)=> {
        getJavaProps(config, cont);
    }).then((cont,result)=> {
        props=result;
        java.newInstance(config.drivername, cont);
    }).then((cont,driver)=>{
        dm.registerDriver(driver, cont);
    }).then((cont)=>{
        dm.getConnection(url, props,cont);
    }).then((cont,conn)=>{
        callback(null,conn);
    }).fail((cont,err)=>{
        if (err) return callback(err);
    });
}

/**
 *
 * @param conn {MyConnection}
 * @returns {*}
 */
function checkConnection(conn){
    if (conn._status!=='ok') { //
        let err='连接暂不可使用:'+conn._status;
        logger.error(_.get(conn,'pool.group'),err);
        return err;
    }
    return null;
}

function showSql(conn,sql,a){
    // let args=_.toArray(arguments);
    console.debug(_.get(conn,'pool.group')+':'+conn.spid,sql,a||'');
}
