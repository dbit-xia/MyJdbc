
'use strict';
//let Jdbc=require("jdbc");
let jinst = require('jdbc/lib/jinst');
var dm = require('jdbc/lib/drivermanager');
let Connection=require('jdbc/lib/connection');
//let conf=require("./../../data/conf");
//let conf_jdbc=conf.jdbc;
let conf_log={log:true};
let _ = require('lodash');
//let Pool=require('./pool'); //重写resultset的方法
let ResultSet=require('./resultset'); //重写resultset的方法
let Thenjs=require("thenjs");
let sqlHelper=require('./SqlHelper');
let genericPool=require('generic-pool');
const path=require('path');
let nodeJdbc;

var java = jinst.getInstance();

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
        jarList.push(path.join(__dirname,"../java/bin"));
        jinst.setupClasspath(jarList);
    }
}

/**
 *
 */
class MyPool{
    constructor() {
        let _this = this;
        //this.config=config;
        //if (config) _this.open(config,cb);
        return _this;
    }
    getPoolStatus() {
        let pool = this.pool;
        return getPoolStatus(pool);
    }

    open(config,cb){
        let _this = this;
        //let config = conf_jdbc.list[dbName];

        if (!config) {
            let err = new Error("未定义数据库连接参数:" );
            cb && cb(err);
            return err;
        }
        this.config=config;

        const factory = {
            create: function(){
                return new Promise(function(resolve, reject){
                    MyJdbc.createMyConnection(config,(err, conn)=>{
                        if (err) return reject(err);
                        conn.pool=_this;
                        if (_.get(this,'config.dbms')==='mysql'){

                        }else {

                        }
                        conn.setOption((err)=>{
                            if (err) { //set失败则关闭连接
                                MyJdbc.closeMyConnection(conn, (err)=> {
                                    if (err) console.error(err);
                                });
                                return reject(err);
                            }
                            resolve(conn);
                        });
                    });
                })
            },
            destroy: function(conn){
                return new Promise(function(resolve){
                    MyJdbc.closeMyConnection(conn,(err)=>{
                        if (err) return reject(err);
                        resolve();
                    });
                });
            },
            validate:function(conn){
                return new Promise(function(resolve){
                    _isClosed(conn,(err,value)=>{ //检查连接是否正常
                        return resolve(value===false); //正常返回true,否则返回false
                    });
                });
            }
        };

        let opts = {
            fifo:false, //优先使用最新获取的连接
            maxWaitingClients:1000, //最大排队数
            testOnBorrow:true, //获取连接时自动检查状态
            idleTimeoutMillis:600000, //设置10分钟认为是空闲进程
            //numTestsPerRun:3,
            evictionRunIntervalMillis:1000, //每1s检查一次空闲连接
            max: config.maxpoolsize, // maximum size of the pool
            min: config.minpoolsize // minimum size of the pool
        };

        /**
         * @type {{on:function(string,Function),available,borrowed,min,max,acquire:function(number),release:function}}
         */
        let pool = genericPool.createPool(factory, opts);

        // //console.info(config.url);
        // let pool = new Jdbc(config);
        // pool.initialize(function (err) {
        //     //console.error(err);
        //     if (err) return cb(err);
        //     console.info(config.url,"连接池初始化成功:", _this.getPoolStatus(pool));
        //     cb && cb(null, pool);
        // });
        _this.pool = pool; //.__proto__

        let finishCount=0;
        pool.on('factoryCreateError', function (err) {
            finishCount++;
            console.error(err);
        });

        //等待初始化完成
        let handle=setInterval(()=> {
            if ((pool.available + pool.borrowed + finishCount) >= pool.min) {
                console.info(config.url, "连接池初始化成功:", _this.getPoolStatus(pool));
                clearInterval(handle);
                cb(null, pool);
            }
        },100);
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
    let args = _.toArray(arguments);
    cb = args.pop();
    outConn=args[0];
    let _this=this;
    let pool=_this.pool;

    if (outConn && outConn.connected) return cb && cb(null,outConn); //原有连接

    pool.acquire(0).then(
        /**
         *
         * @param conn {MyConnection}
         * @returns {*}
         */
        (conn)=>{
            cb(null,conn); //
            // Thenjs((cont)=> {
            //     _isClosed(conn, cont);
            // }).then((cont,value)=>{
            //     if (!value) return cb(null,conn); //连接正常
            //     createConnection(_this.config,cont); //重新获取基础连接,/*已关闭,重连*/
            // }).then((cont,result)=>{
            //     conn.setConnection(new Connection(result));
            //     cb(null, conn);
            // }).fail((cont,err)=>{
            //     cb(err);
            // });
        }
    ).catch((err)=>{
        cb(err);
    });
    // if (outConn && outConn.connected) return cb && cb(null,outConn); //原有连接
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
    let pool=this.pool;
    pool.release(conn).then(function(){
        cb()
    }).catch(function(err){
        cb(err);
    });
    //pool.release(conn.connection,cb);
};

/**
 * 连接数据库,cb是处理数据的函数,cb2是输出函数
 * 执行第一个回调函数(err,conn,(err,result,iscommit)=>{})-->提交-->再回调cb2(err,result)进行输出
 * @param fn {function(MyConnection,function)}
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
        }).then((cont,result, childOpt)=>{
            if (_.get(opt,'commit') || _.get(childOpt,'commit')) { //是否提交
                Thenjs((cont)=>{
                    conn.Commit(cont);
                }).then(()=>{
                    cont(null,result);
                }).fail((c,err)=>{
                    conn.Rollback((err2)=>{
                        cont(err||err2);
                    });
                });
            }else{
                return cont(null,result);
            }
        }).fin((cont,err,result)=>{
            if (isNew) {
                return conn.Disconnect((err2)=>{
                    output(err!=null ? err : err2,result);
                });
            }
            output(err, result);
        });
    }
};

class MyConnection {
    /**
     *
     * @param option {{[title]:string,[pool]:MyPool}}
     */
    constructor(option) {
        console.log("new Conn");
        let _this = this;
        _this.option=option;
        _this.Title = option.title;
        /**
         *
         * @type {MyPool}
         */
        _this.pool=option.pool;
        _this.connection = {};//=?PoolConnection();
        _this.connected = false;
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
        this.connected = true;
    }

    /**
     * 使用当前Connection进行连接
     * @param cb {function(*=,MyConnection=)}
     */
    Connect(cb) {
        let pool = this.pool;
        if (this.connected) {
            _isClosed(this,(err,value)=> {
                if (!value) return cb(null, this); //返回当前已连接
                this.connected = false;
                return this.Connect(cb); //重连
            });
            return ;
        }
        if (!pool) return cb('连接池还未初始化!');
        console.info("Connect");
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
                console.error("Disconnect:", err);
            }else{
                console.info("Disconnect:ok");
            }
            // conn.connection = {};
            // conn.connected = false;
            cb && cb(err);
        });
    }

    setOption(cb){
        let dbms=_.get(this,'pool.config.dbms') || '';
        switch (dbms) {
            case '':
            case 'sybase':
            case 'sql':
                this.ExecuteDDL("set forceplan on", cb);
                break;
            default:
                cb();
                break;
        }
    }

    Query(sql, paramsList, skipCount, cb){
        let args = _.toArray(arguments);
        return Query.apply(this,[this].concat(args));
    }

    QueryGrid(sql, paramsList, skipCount, cb){
        let args = _.toArray(arguments);
        return QueryGrid.apply(this,[this].concat(args));
    }

    QueryPart(sql, paramsList, skipCount, cb){
        let args = _.toArray(arguments);
        return QueryPart.apply(this,[this].concat(args));
    }
    QueryLimit(baseSql,option,cb){
        let args = _.toArray(arguments);
        return QueryLimit.apply(this,[this].concat(args));
    }
    Execute(sql, paramsList, cb){
        let args = _.toArray(arguments);
        return Execute.apply(this,[this].concat(args));
    }
    ExecuteDDL(sql, cb){
        let args = _.toArray(arguments);
        return ExecuteDDL.apply(this,[this].concat(args));
    }
    Commit(cb){
        let args = _.toArray(arguments);
        return Commit.apply(this,[this].concat(args));
    }
    Rollback(cb){
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
        return setAutoCommit.apply(this,[this].concat(args));
    }
    getAutoCommit(cb){
        let args = _.toArray(arguments);
        return getAutoCommit.apply(this,[this].concat(args));
    }
    SelectLast(column, from, cb){
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
    console.log('closeConnection');
    conn.connection.close(callback);
};

module.exports=MyJdbc;

/**
 * JDBC数据类型常量
 * @type {{BOOLEAN: number, DATE: number, DECIMAL: number, DOUBLE: number, FLOAT: number, INTEGER: number, VARCHAR: number, STRING: number}}
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
    NUMBER:8
};

function log(data) {
    conf_log.log && console.info(data);
}
function time(data) {
    if (conf_log.log) {console.time(data) ; log(data);}
}
function timeEnd(data) {
    conf_log.log && console.timeEnd(data);
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
 * @param myConnection {MyConnection}
 * @param cb {function}
 * @private
 */
function _isClosed(myConnection,cb){
    try{
        myConnection.Query("select 1",(err)=>{
            if (err) return cb(null,true); //已断开 或 getSQLStateSync()==='08S01'
            cb(null,false);//未断开
        });
    }catch (err){
        cb(null,true); //已断开
    }
    //return myConnection && myConnection.conn && connection._conn && connection._conn.isClosedSync();
}

/**
 *
 * @param err
 * @returns {string|null}
 */
function getDBErr(err){
    if (!err) return null; //无错误
    if(err instanceof Error) {
        return err;
    }
    let code=err.sqlDBCode || (_.get(err,'cause.getErrorCodeSync') || (()=>null)).call(err.cause) || -1;
    let text=err.sqlErrText /*|| (_.get(err,'cause.getMessageSync') || (()=>null)).call(err.cause)*/ || toString(err);
    return JSON.stringify({
        sqlDBCode:code,
        sqlErrText:text
    }); //getStack:()=>(err.message || err.stack || err)
}

function setFetch(statement,cb){
    //if (err) return cb(getDBErr(err));
    statement.setFetchSize(100, (err)=> {
        if (err) return cb(getDBErr(err));
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
 *
 * @param statement {PreparedStatement}
 * @param rows {Array} 一维/二维数组
 * @param datatypes {Array}
 * @param option {{[isSelect]}}
 * @param cb {function}
 */
function setParams(statement,rows,datatypes,option,cb) {

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

    let fieldsCount=_.size(datatypes) || _.size(rows[0]); //取首行点位符个数,如果传入参数个数不一致,会造成用上一次的数值;

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
        if (!types === undefined) {
            return cb('无效的java.sql.Types:'+datatypes[i])
        }
        nTypes.push(types); //转换成常量数字值
    }

    return nodeJdbc.setParams(statement._ps,rows,nTypes,option.isSelect,cb);

    let fns=[];
    for (let i= 0,total=rows.length;i<total;i++){
        fns.push((cont)=>{
            addrow(rows[i],cont);
        });
    }
    Thenjs.series(fns).fin((cont,err,result)=>{
        if (err) return cb(err);
        cb(null,result);
    });

    /**
     *
     * @param row {Array}
     * @param cb {function}
     */
    function addrow(row,cb){
        let fns=[];
        for (let i= 0,total=fieldsCount;i<total;i++){
            if (row[i]==null){
                fns.push((cont)=>{
                    statement._ps.setNull(i + 1,nTypes[i], cont);
                });
            }else{
                fns.push((cont)=>{
                    //try不到,setObject可以指定精度,setBigDecimal却不行
                    statement._ps.setObject(i+1,row[i],nTypes[i],4,(err)=>{
                        if (err) {
                            err.stack = '数据类型不一致! 第' + String(i + 1) + '列(' + row[i] + ') \n' + err.stack;
                            return cont(err);
                        }
                        cont();
                    });
                });
            }

        }
        Thenjs.parallel(fns).fin((cont,err)=>{
            if (err) return cb(err);
            if (option.isSelect) { //select时不能执行addbatch,否则jconn4会报错
                cb();
            }else{
                statement.addBatch((err)=> {
                    if(err) return cb(err);
                    cb(null);
                });
            }
        });
    }
}


/**
 * 查询操作
 * @param myConn {MyConnection}
 * @param option {{sql,[paramsList]:Array,[datatypes]:Array,[grid]}}
 * @param cb
 * @constructor
 */
function QueryBase(myConn,option,cb) { /*paramsList SQL参数列表*/
    let sql=option.sql;
    let paramsList=option.paramsList;
    let datatypes=option.datatypes;
    let grid=option.grid;
    let _statement;
    let label='Query:'+new Date().getTime();
    console.time(label);

    Thenjs((cont)=> {
        console.info(sql);
        if (paramsList){ /*有参数*/
            Thenjs((cont)=> {
                myConn.connection.prepareStatement(sql, cont);
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
                myConn.connection.createStatement(cont);
            }).then((cont,statement)=> {
                _statement = statement;
                cont();
                // setFetch(_statement,cont);
            }).then((cont)=>{
                _statement.executeQuery(sql,cont);
            }).fin(cont);
        }
    }).then((cont,rows, fields)=> {
        let _label='toObjArray:'+new Date().getTime();
        console.time(_label);
        rows.toObjArray({grid,statement:_statement},(err, results)=> {
            console.timeEnd(_label);
            if (err) return cont(err);
            console.info("Fetch rowcount:" , _.size(results));
            cont(null, results, fields);
        });
    }).fin((cont,err,a, b)=>{
        if (_statement) return _statement.close((err2)=> {
            if (err || err2) return cont(err || err2);
            cont(null,a, b);
        });
        cont(err,a, b);
    }).fin((cont,err,ret)=>{
        console.timeEnd(label);
        if (err) return cb(getDBErr(err));
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

function QueryPart(myConn,sql,paramsList,skipCount,cb) { /*paramsList SQL参数列表,skipCount 忽略的行数 为可选参数*/
    let args = _.toArray(arguments);
    cb = args.pop();
    paramsList && paramsList.length>0 && console.warn("QueryPart不支持指定参数列表!");
    myConn.connection.createStatement(1004,1007,function(err, statement) {
        if (err) return cb(getDBErr(err));
        setFetch(statement,(err)=> {
            execute(err,statement);
        });
    });

    function execute(err,statement) {
        if (err) return cb(getDBErr(err));
        time('executeQuery');
        statement.executeQuery(sql, function (err, resultset, fields) {
            timeEnd('executeQuery');
            console.log("End Query:" + sql);
            if (err) {
                //console.error("query:" + err);
                return cb(getDBErr(err));
            }
            time('absolute');
            resultset.absolute(skipCount,(err)=>{ /*跳转到指定index*/
                timeEnd('absolute');
                if (err) return cb(getDBErr(err));
                time('toObjArray');
                resultset.toObjArray((err, results)=> {
                    timeEnd('toObjArray');
                    if (err) return cb(getDBErr(err));
                    console.log("Fetch rowcount:" + results.length);
                    cb && cb(null, results, fields);
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

    if (sql instanceof Array ) { //支持sql数组顺序批量执行
        if (paramsList) console.warn('目前仅支持无参数的SQL队列!');
        return ExecuteBatch(myConn,sql,cb);
    }
    let _statement;
    Thenjs((cont)=> {
        console.info(sql); //paramsList
        myConn.connection.prepareStatement(sql, cont);
    }).then((cont,statement)=> {
        _statement=statement;

        //无参数时
        if (!_.size(paramsList)){
           return statement._ps.executeUpdate(cont); //executeUpdate
        }

        Thenjs((cont)=>{
            setParams(statement,paramsList,datatypes,{isSelect:false},cont);
        }).then((cont)=>{
            statement._ps.executeBatch(cont);//需要用executeBatch批量执行
        }).then((c,nrows)=>{
            let rowcount=0;
            if (nrows instanceof Array){
                _.forEach(nrows,(n)=>rowcount+=n);
            }else{
                rowcount=nrows;
            }
            cont(null,rowcount);
        }).fail(cont); //同一个statement只能用串行
    }).fin((cont,err,ret)=> {
        if (_statement) return _statement.close((err2)=> {
            if (err || err2) return cont(err || err2);
            cont(null,ret);
        });
        cont(err,ret);
    }).fin((cont,err,ret)=>{
        if (err) {
            err=getDBErr(err);
            console.error(sql,err);
            return cb(err);
        }
        cb(null, ret);
    });
}

function ExecuteBatch(myConn,sql,cb){
    let args = _.toArray(arguments);
    cb = args.pop();

    let _statement;
    Thenjs((cont)=> {
        myConn.connection.createStatement(cont);
    }).then((cont,statement)=> {
        _statement=statement;
        sql=[].concat(sql); //支持sql数组顺序批量执行
        let sqlcount=sql.length;
        let series=[];
        for (let i=0;i<sqlcount;i++){
            series.push((cont)=>{
                console.info(sql[i]);
                _statement.executeUpdate(sql[i],(err,result)=>{
                    if (err) {
                        err = getDBErr(err);
                        console.error(sql[i], err);
                        return cont(err);
                    }
                    cont(null,result);
                });
            });
        }
        Thenjs.series(series).fin(cont); //串行
    }).fin((cont,err,ret)=> {
        if (_statement) return _statement.close((err2)=> {
            if (err || err2) return cont(err || err2);
            cont(null,ret);
        });
        cont(err,ret);
    }).fin((cont,err,ret)=>{
        if (err) return cb(getDBErr(err));
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
            return setAutoCommit(myConn,true, (err)=> {
                if (err) return cont(err);
                autocommit_old = false;
                return cont(null);
            })
        }
        cont(null); //传递statement
    }).then((cont)=> {
        // console.info(sql);
        ExecuteBatch(myConn,sql,cont);
    }).fin((cont,err,ret)=> {
        if (autocommit_old !== undefined) {
            setAutoCommit(myConn,autocommit_old, (err)=> {
                if (err) console.error('setAutoCommit->' + autocommit_old + ':' + JSON.stringify(err));
            });
        }
        if (err) return cb(getDBErr(err));
        cb(null, ret);
    });
}
function Commit(myConn,cb){
    //if (cb===undefined) {cb=function(){}}
    //Execute(myConn,"commit",
    myConn.connection.commit(function(err,ok) {
        if (err) {
            cb && cb(getDBErr(err));
            return;
        }
        cb && cb(null, ok);
    });
}
function Rollback(myConn,cb) {
    //if (cb===undefined) {cb=function(){}}
    myConn.connection.rollback(function(err,ok) {
        if (err) {
            cb && cb(getDBErr(err));
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
            cb && cb(getDBErr(err));
            return;
        }
        cb && cb(null,ok);
    });
}
function getAutoCommit(myConn,cb) {
    if(!cb) return myConn.connection._conn.getAutoCommitSync(); //阻塞
    // 非阻塞
    return myConn.connection._conn.getAutoCommit((err,ret)=>{
        if (err) return cb(getDBErr(err));
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
        if (err) return cb(getDBErr(err));
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

    if (option.where && _.isObject(option.where)===false){
        return cb(new Error('where需要传入数组!'));
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
            sql=`SELECT ${SELECT}
            FROM ${baseSql.from}
            ${where.length ? ('WHERE ' + where.join(' AND ')) : ''}`;

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
            sql=`SELECT ${fetchRow?'TOP '+String(fetchRow):''} ${SELECT}
            FROM ${baseSql.from}
            ${where.length ? ('WHERE ' + where.join(' AND ')) : ''}`;

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

        sql+=`\nSELECT TOP ${top} ${selectFieldStr}
                FROM ${baseSql.from}
                ${where.length ? ('WHERE ' + where.join(' AND ')) : ''}`;
        if (SORT) sql+=` ORDER BY ${SORT}` ;
        //sql+="SELECT "+selectFieldStr2+" where @@rowcount>=1 ";

        if (addWhere) where.push(addWhere);

        sql+=`\nSELECT TOP ${fetchRow} ${SELECT}
            FROM ${baseSql.from}
            ${where.length ? ('WHERE ' + where.join(' AND ')) : ''}`;

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
 * @param a {object|Error|string}
 * @returns {string}
 */
function toString(a){
    if (typeof a==='object'){
        if (a instanceof Error) return a.stack || a.message || JSON.stringify(a);
        return JSON.stringify(a);
    }else if(typeof a==='string'){
        return a;
    }
    return String(a);
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

    var properties= new Properties();
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
