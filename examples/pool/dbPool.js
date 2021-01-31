const myjdbc=require('../../lib/index');
// const MyUtil=require('../lib/MyUtil');

//设置驱动程序
myjdbc(['../../../../drivers/jconn4.jar']);

async function run(fn) {

    //创建连接池
    let newPool = new myjdbc.Pool({
        name: 'myjdbc测试库',
        dbms: 'ase', //ase,sybase,mysql,pgsql,mssql,asa
        // encrypt:'des-ecb',
        holdTimeout: 10 * 60 * 1000, //单条语句超时时间
        url: process.env.url,//数据库连接字符串
        drivername: "com.sybase.jdbc4.jdbc.SybDriver",
        minpoolsize: 1,//最小连接池
        maxpoolsize: 127,//最大连接数
        properties: { //数据库账号,//数据库口令
            user: process.env.user,
            password: process.env.password
        }
    });

    let conn;
    try {
        //打开连接池
        await newPool.open();

        //获取连接
        conn = await newPool.getConnection();
        conn.setAppInfo({sourceUrl: __filename}, ['sourceUrl']);
        await fn(conn);
        await conn.Commit();
    } catch (err) {
        console.error(err);
        await conn.Rollback().catch(console.error);
    } finally {
        //归还连接
        if (conn) {
            await newPool.releaseConn(conn).catch(console.error);
        }
        //释放连接池
        if (newPool.pool) await newPool.clear();

    }

}

module.exports=run;


