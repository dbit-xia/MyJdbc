/**
 * Created by Dbit on 2017/9/9.
 */
const Thenjs=require('thenjs');
process.chdir('../../../');
var config=require('../../../data/conf');
var myjdbc=require('../index');
myjdbc(config.jdbc.driver); //初始化驱动
var pool=new myjdbc.Pool(require(config.path.data+'/db/runsa'));

Thenjs((cont)=> {
    pool.open(cont);
}).then((cont)=>{
    pool.getConnection(cont);
}).then((cont,conn)=>{
    Thenjs((cont)=> {
        conn.getAutoCommit(cont);
    },true).then((cont)=>{
        conn.setAutoCommit(false, cont);
    }).then((cont)=>{
        conn.ExecuteDDL([`if exists(select 1 where object_id('student')<>null ) drop table student `,
                    `create table student(
                        code char(10) null,
                        name varchar(30) null,
                        age int null,
                        height decimal(11,2) null,
                        birthday datetime null,
                        valids int null
                    )`,'create index i_student_code on student(code)'],cont);
    }).then((cont,result)=> {
        conn.Execute(`insert into student(code,name,age,height,birthday,valids) values(?,?,?,?,?,?) `, ['S001', '大明', 13, 100.01, '2000-01-01',true], cont);
    }).then((cont,result)=> {
        conn.Execute(`insert into student(code,name,age,height,birthday,valids) values(?,?,?,?,?,?) `,
            [
                ['S002', '大明', 14, undefined, null,true],
                ['S003', 123, '15', 123456789.01, '2000-01-01',false],
                ['S003', 123.45, '15', '100.01', '2000-01-01',false],
                ['S003', ()=>{}, '15', true, '2000-01-01',false]
            ],
            ['', '', 'int', 'decimal', 'datetime','int'], cont);
    }).then((cont)=>{
        conn.Commit(cont);
    }).then((cont,nrows)=>{
        conn.Query(`select * from student where code>=? `,['S002'],cont);
    }).then((cont,rows)=>{
        conn.QueryGrid(`select * from student where code>=? `,['S002'],cont);
    }).then((cont,rows)=> {
        conn.Execute(["delete student where code='S001' ", 'delete student '], cont);
    }).then((cont)=>{
        conn.Rollback(cont);
    }).then((cont)=> {
        conn.ExecuteDDL(`drop table student`, cont);
    // }).then((cont)=> {
        // Thenjs((cont)=>{
        //     conn.Close(cont);
        // }).then((cont)=>{
        //     conn.Commit(cont);
        // }).fin(cont);

    }).fin((c,err)=>{
        pool.releaseConn(conn,console.log);
    });
});