/**
 * Created by Dbit on 2017/9/9.
 */
const Thenjs=require('thenjs');
var config=require('../../../data/conf').jdbc;
var myjdbc=require('../index');
myjdbc(config.driver); //初始化驱动
var pool=new myjdbc.Pool(config.list['runsa']);

Thenjs((cont)=>{
    pool.getConnection(cont);
}).then((cont,conn)=> {
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
                        height decimal(11,2),
                        birthday datetime null,
                        valids int null
                    )`, 'create index i_student_code on student(code)'], cont);
    }).then((cont) => {
        let student=new myjdbc.Model({conn,table:'student',fields: {
            age: ['', 'int'],valids: ['', 'int'],
            height: ['', 'decimal', '(11,2)']
        }});
        Thenjs((cont) => {
            student.insert({
                values: ['code', 'age', 'height', 'birthday', 'valids'],
                fields: ['code', 'age', 'height', 'birthday', 'valids'],
                types:['','','','',''],
                data: [{
                    code: 'S002',
                    name: '二明',
                    age: '14',
                    height: '100.01',
                    birthday: '2000-01-01',
                    valids: false
                },
                    {
                        code: 'S003', name: '三明', age: '15', height: '100.01', birthday: '2000-01-01', valids: '123'
                    }
                ],
                commit:true
            }, cont);
        },true).then((cont) => {
            student.insert({
                fields: ['code','name','age','height','birthday','valids'],
                data: ['S001', '大明', 13, 100.01, '2000-01-01', true],
                commit:true
            }, cont);
        }).then((cont, result) => {
            student.findOne({select:'code,name,birthday,valids,age,height',where:{}}, cont);
        }).then((cont, rows) => {
            student.find({select:'code,name,birthday,valids,age,height',where:{},limit:[2,2]}, cont);
        }).then((cont, rows) => {
            student.remove({where:{}}, cont);
        }).then((cont) => {
            conn.Rollback(cont);
        }).then((cont) => {
            conn.ExecuteDDL(`drop table student`, cont);
        }).fin((c, err) => {
            pool.releaseConn(conn, console.log);
        });
    });
});