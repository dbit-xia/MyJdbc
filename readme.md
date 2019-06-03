#MyJdbc

### update:
* model & connection 支持async+await

# Quick Start

## Install 
* java模块安装步骤请参考:https://github.com/joeferner/node-java#readme
```shell
$ npm install myjdbc
$ npm install java
```

## Basic Usage
```javascript
    let MyJdbc=require('myjdbc');
    MyJdbc(['./drivers/jconn4.jar']); //初始化驱动
    
    //create DBConnection pool
    let pool=new MyJdbc.Pool({
         name:'runsatest',
         dbms: 'sybase',
         //encrypt:'des-ecb', //密码加密选项,使用环境变量进行解密
         "drivername": "com.sybase.jdbc4.jdbc.SybDriver",
         "url": "jdbc:sybase:Tds:192.168.15.194:51940/rts_xjjx?APPLICATIONNAME=runsa_interface&useUnicode&charset=cp936",//数据库连接信息
         "minpoolsize": 1,//最小连接池
         "maxpoolsize": 4,//最大连接数
         "properties": { 
             user: "zgsybase", //数据库账号
             password:"EOkFn1zK/Tg/NyHKYxHpAw==" //数据库口令
         }
     });
    
    //open DBConnection pool
    pool.open((err)=>{
        if (err) {
            console.error(err);
        }else{
            console.log(pool.getStatus()); //输出连接池信息
        }
    });
    
    //getConnection from DBConnection pool
    pool.getConnection((err,conn)=>{
    
        //查询结果输出为对象数组
        await conn.Query('select 1');
        await conn.Query('select 1 where 1=?',[1]);
        
        //查询结果输出为二维数组
        await conn.QueryGrid('select 1');
        await conn.QueryGrid('select 1 where 1=?',[1]);
        
        //Run DML
        await conn.Execute('delete table where 1=1');
        await conn.Execute('delete table where 1=?',[1]);
        await conn.Execute(['delete table where ...','delete table where ...']);
        
        //Run DDL
        await conn.ExecuteDDL('drop table ');
        await conn.ExecuteDDL(['drop table ','drop table ']);
        
        //Commit/Rollback transction
        await conn.Commit((err)=>{});
        await conn.Rollback((err)=>{});
        
        //set/get autocommit mode
        await conn.setAutoCommit(true/false);
        await conn.getAutoCommit((err,value)=>{});
        
        pool.releaseConn(conn);
    });
```

## Model Usage
```javascript
    let myjdbc=require('myjdbc');
    
    /**
    * @param {{conn:connection,table:String,[fields]:{},[sort]:{},[where]:{}}}
    */
    let studentModel=new myjdbc.Model({conn,table:'student'});
    
    /**
    * @param option {{select:String,where:{},[sort]:{},limit:[],[commit]:Boolean}}
    * @param callback
    */
    await studentModel.find({select:'id,name',where:{id:1}});
    await studentModel.findOne({select:'id,name',where:{id:1}});
    
    /**
    * @param option {{data:[{}],[fields]:[String],[values]:[String],[types]:[String],[commit]:Boolean}}
    * @param callback
    */
    await studentModel.insert({
        data:[{id:1,name:'hello'},{id:2,name:'world'}]
    });
    
    /**
    * @param option {{set:{},where:{},[commit]:Boolean}}
    * @param callback
    */
    await studentModel.update({set:{name:'hi'},where:{id:1}});
    
    /**
    * @param option {{where:{},[commit]:Boolean}}
    * @param callback
    */
    await studentModel.remove({where:{id:1}});
```