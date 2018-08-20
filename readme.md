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
        conn.Query('select 1',(err,rows)=>{});
        conn.Query('select 1 where 1=?',[1],(err,rows)=>{});
        
        //查询结果输出为二维数组
        conn.QueryGrid('select 1',(err,rows)=>{});
        conn.QueryGrid('select 1 where 1=?',[1],(err,rows)=>{});
        
        //Run DML
        conn.Execute('delete table where 1=1',(err,rows)=>{});
        conn.Execute('delete table where 1=?',[1],(err,rows)=>{});
        conn.Execute(['delete table where ...','delete table where ...'],(err,rows)=>{});
        
        //Run DDL
        conn.ExecuteDDL('drop table ',(err,rows)=>{});
        conn.ExecuteDDL(['drop table ','drop table '],(err,rows)=>{});
        
        //Commit/Rollback transction
        conn.Commit((err)=>{});
        conn.Rollback((err)=>{});
        
        //set/get autocommit mode
        conn.setAutoCommit(true/false,(err)=>{});
        conn.getAutoCommit((err,value)=>{});
        
        pool.releaseConn(conn,(err)=>{});
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
    studentModel.find({select:'id,name',where:{id:1}},console.log);
    studentModel.findOne({select:'id,name',where:{id:1}},console.log);
    
    /**
    * @param option {{data:[{}],[fields]:[String],[values]:[String],[types]:[String],[commit]:Boolean}}
    * @param callback
    */
    studentModel.insert({
        data:[{id:1,name:'hello'},{id:2,name:'world'}]
    },console.log);
    
    /**
    * @param option {{set:{},where:{},[commit]:Boolean}}
    * @param callback
    */
    studentModel.update({set:{name:'hi'},where:{id:1}},console.log);
    
    /**
    * @param option {{where:{},[commit]:Boolean}}
    * @param callback
    */
    studentModel.remove({where:{id:1}},console.log);
```