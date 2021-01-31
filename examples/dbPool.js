const myjdbc=require('../lib/index');
const debug=require('debug')('test');
const MyUtil=require('../lib/MyUtil');
// const MyUtil=require('../lib/MyUtil');

process.on('uncaughtException', (err)=>{
    console.error('未捕获的异常:',err);
});

process.on('unhandledRejection', (reason, p) => {
    console.log('Unhandled Rejection at: Promise', p, 'reason:', reason);
    // application specific logging, throwing an error, or other logic here
});

//设置驱动程序
myjdbc(['../../../drivers/jconn4.jar']);

(async function run() {

    debug('start');
    //创建连接池
    let newPool = new myjdbc.Pool({
        name: 'myjdbc测试库',
        dbms: 'ase', //ase,sybase,mysql,pgsql,mssql,asa
        encrypt:'des-ecb',
        holdTimeout: 10 * 60 * 1000, //单条语句超时时间
        url: process.env.url,//数据库连接字符串
        drivername: "com.sybase.jdbc4.jdbc.SybDriver",
        minpoolsize: 2,//最小连接池
        maxpoolsize: 10,//最大连接数
        properties: { //数据库账号,//数据库口令
            user: process.env.user,
            password: process.env.password
        }
    });

    newPool.group = "test";

    try {
        //打开连接池
        await newPool.open();
        debug('pool opened!');
        await Promise.all([autoConnectAndExecute(), autoConnectAndExecute(), autoConnectAndExecute(), autoConnectAndExecute()]);

        while (true){
            await Promise.all([autoConnectAndExecute(), autoConnectAndExecute(), autoConnectAndExecute()]);
            await MyUtil.sleep(1000);
        }
    } catch (err) {
        debug('catch',err);
    } finally {
        debug('finally');
        //归还连接
        // if (conn) await newPool.releaseConn(conn);
        //释放连接池
        // if (newPool.pool) await newPool.clear();

    }

    async function autoConnectAndExecute(){
        let conn;
        try{
            conn = await newPool.getConnection();
            debug('status',newPool.getStatus());
            conn.setAppInfo({});
            await conn.Query("select a=123");
        }catch (e) {
            debug('catch',e);
        }finally {
            if (conn) await newPool.releaseConn(conn);
        }
    }


})();


