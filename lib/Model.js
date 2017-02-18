/**
 * Created by Dbit on 2016/10/25.
 */

'use strict';

let sqlHelper=require('./SqlHelper');
let Thenjs=require('thenjs');
let _=require('lodash');

/**
 * @type {function}
 */
class Model {
    /**
     *
     * @param option {{table,[fields],[where],[keySort],[conn]:MyConnection,[connection]:MyConnection}}
     */
    constructor(option) {
        //this.option = option;
        /**
         *
         * @type {MyConnection}
         */
        this.connection = option.conn || option.connection;
        this.table = option.table;
        this.fields = option.fields;
        this.where = option.where;
        this.baseSql = {
            select: this.fields,
            from: this.table,
            where: this.where,
            keySort: option.keySort
        };

        this.delete = this.remove;
        this.select = this.find;
        this.selectOne = this.findOne;
    }

    /**
     *
     * @param key
     * @returns {string}
     */
    column(key) {
        let baseSql = this.baseSql;
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

    find(option, cb) {
        let args = Array.prototype.slice.call(arguments);
        return find.apply(this, [this].concat(args));
    }

    findOne(option, cb) {
        let args = Array.prototype.slice.call(arguments);
        return findOne.apply(this, [this].concat(args));
    }

    insert(option, cb) {
        let args = Array.prototype.slice.call(arguments);
        return insert.apply(this, [this].concat(args));
    }

    update(option, cb) {
        let args = Array.prototype.slice.call(arguments);
        return update.apply(this, [this].concat(args));
    }

    remove(option, cb) {
        let args = Array.prototype.slice.call(arguments);
        return remove.apply(this, [this].concat(args));
    }

    setConnection(connection){
        this.connection = connection;
    }
    setTable(table){
        this.table = table;
    }

    /**
     *
     * @param option {object}
     * @return {string}
     */
    getWhere(option){
        return sqlHelper.WHERE(option,this.column.bind(this));
    }
}

// function getSql(){
//
// }

/**
 *
 * @param model {Model}
 * @param option {{select,where,sort,limit}}
 * @param cb
 */
//basic.prototype.find=
function find(model,option,cb){
    //let args = Array.prototype.slice.call(arguments);
    //cb = args.pop();
    //option=args[1];
    let conn=model.connection;
    if (!option.select) option.select=_.keys(model.fields).join(',');
    conn.QueryLimit(model.baseSql,option,cb);
}
//basic.prototype.select=basic.prototype.find;

/**
 * 根据条码获取一条记录
 * @param model {Model}
 * @param option
 * @param cb {function(err=,row=)}
 */
//basic.prototype.findOne=
function findOne(model,option,cb){
    option.limit=1; //只检索第一行
    find(model,option,(err,rows)=>{
        if(err) return cb(err);
        if (_.size(rows)===0) return cb();
        cb(null,rows[0]);
    })
}
//basic.prototype.selectOne=basic.prototype.findOne;

/**
 *
 * @param model
 * @param option {object|{where:object,commit:boolean}}
 * @param cb
 */
function remove(model,option,cb){
    //let args = Array.prototype.slice.call(arguments);
    //cb = args.pop();
    //where=args[1];
    let conn=model.connection;

    Thenjs((cont)=> {
        let WHERE;
        WHERE = model.getWhere(option.where||option);
        let sql = `delete ${model.table} ` + (WHERE ? 'WHERE ' + WHERE : '');
        conn.Execute(sql, cont);
    }).then((cont,result)=>{
        if (option.commit){
            conn.Commit((err)=>{
                if (err) return cont(err);
                cb(null,result);
            });
            return;
        }
        cb(null,result);
    }).fail((cont,err)=>{
        conn.Rollback((err2)=>{
            cb(err || err2);
        });
    });
}

/**
 *
 * @param model {Model}
 * @param option {Array|{fields:Array,values:Array,data:Array,types:Array,commit:boolean}}
 * @param cb
 */
function insert(model,option,cb){
    let args = Array.prototype.slice.call(arguments);
    cb = args.pop();
    option=args[1];
    let conn=model.connection;
    Thenjs((cont)=>{
        let define={};
        if (option instanceof Array){ //Array表示全是数据
            _.merge(define,{table:model.table},{data:option}); //
        }else{
            if (!option.data) return cont('需要data属性');
            _.merge(define,{table:model.table},option);
        }
        sqlHelper.INSERT(define,cont);
    }).then((cont,sql,values,types)=>{
        // console.debug(JSON.stringify(values));
        conn.Execute(sql,values,types,cont);
    }).then((cont,result)=>{
        if (option.commit){
            conn.Commit((err)=>{
                if (err) return cont(err);
                cb(null,result);
            });
            return;
        }
        cb(null,result);
    }).fail((cont,err)=>{
        conn.Rollback((err2)=>{
            cb(err || err2);
        });
    });
}

/**
 *
 * @param model {Model}
 * @param option {{where,set,upsert,commit:boolean}}
 * @param cb
 */
//basic.prototype.update=
function update(model,option,cb){
    //let args = Array.prototype.slice.call(arguments);
    //cb = args.pop();
    //option=args[1];
    let conn=model.connection;

    let fields=[];
    let values=[];
    Thenjs((cont)=>{
        //let parms=[];
        let set=option.set ;//|| option;
        let keys=_.keys(set);
        let sets=[];
        for (let i=0;i<keys.length;i++){
            if (keys[i].charAt(0)==='$') continue; //where条件
            fields.push(keys[i]);
            values.push(set[keys[i]]);
            sets.push(keys[i]+'='+sqlHelper.VALUE(set[keys[i]],this.column.bind(this)))
        }
        let WHERE=model.getWhere(option.where);
        let sql;
        sql = `UPDATE ${model.table} SET ${sets.join(',') +' '+ (WHERE ? 'WHERE '+WHERE : '')} `;
        conn.Execute(sql,cont);
    }).then((cont,nrows)=>{
        if (nrows===0 && option.upsert) {
            fields=fields.concat(_.keys(option.where));
            values=values.concat(_.values(option.where));
            let sql = `INSERT INTO ${model.table}(${fields.join(',')})
            VALUES (${_.fill(new Array(fields.length), '?').join(',')}) `;
            conn.Execute(sql, values, cont);
        } else {
            cont(null,nrows); //更新0行
        }
    }).then((cont,result)=>{
        if (option.commit){
            conn.Commit((err)=>{
                if (err) return cont(err);
                cb(null,result);
            });
            return;
        }
        cb(null,result);
    }).fail((cont,err)=>{
        conn.Rollback((err2)=>{
            cb(err || err2);
        });
    });
}

module.exports=Model;
