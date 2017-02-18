#MyJdbc

###Method List:
#####getConnection
#####Query
#####QueryGrid
#####QueryLimit
#####Execute
#####ExecuteDDL
#####Commit
#####Rollback
#####setAutoCommit
#####getAutoCommit

```
    var MyJdbc=require('./MyJdbc');
    MyJdbc(['./drivers/jconn4.jar','./drivers/jtds.jar']); //初始化驱动
    var pool=new MyJdbc.Pool();
    pool.getConnection((err,conn)=>{
        conn.Query('select 1',(err,rows)=>{});
        conn.Query('select 1 where 1=?',[1],(err,rows)=>{});
        
        conn.QueryGrid('select 1',(err,rows)=>{});
        conn.QueryGrid('select 1 where 1=?',[1],(err,rows)=>{});
        
        conn.Execute('delete table where 1=1',(err,rows)=>{});
        conn.Execute('delete table where 1=?',[1],(err,rows)=>{});
        conn.Execute(['delete table where ...','delete table where ...'],(err,rows)=>{});
        
        conn.ExecuteDDL('drop table ',(err,rows)=>{});
        conn.ExecuteDDL(['drop table ','drop table '],(err,rows)=>{});
        
        conn.Commit((err)=>{});
        conn.Rollback((err)=>{});
        
        conn.setAutoCommit(true/false,(err)=>{});
        conn.getAutoCommit((err,value)=>{});
        
        pool.releaseConn(conn,(err)=>{});
    });
```