/**
 * Created by Dbit on 2016/10/25.
 */

'use strict';
const util=require('util');
let sqlHelper=require('./SqlHelper');
let Thenjs=require('thenjs');
let _=require('lodash');

/**
 * @type {function}
 */
class Model {
    /**
     *
     * @param option {{table,[fields]:{[name]:String|[String,String,Number]},[where],[keySort],[conn]:MyConnection,[connection]:MyConnection}}
     */
    constructor(option) {
        //this.option = option;
        /**
         *
         * @type {MyConnection}
         */
        this.connection = option.conn || option.connection;
        this.conn=this.connection;

        this.table = option.table;
        this.where = option.where;
        this.fields = {};
        _.forIn(option.fields, (value, key) => {
            if (_.isString(value)) {
                this.fields[key] = [value, 'varchar', '(100)'];
            } else if (_.size(value) === 2) {
                let _pos = value[1].indexOf('(');
                let _len;
                if (_pos > -1) {
                    _len = value[1].slice(_pos); //(#,#)
                    value[1] = value[1].slice(0, _pos);
                }
                this.fields[key] = [value[0], value[1], _len || ''];
            } else {
                this.fields[key] = value;
            }
        });

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

    async find(option, cb) {
        if (cb) return this.find(option).then((r)=>cb(null,r)).catch(cb);
        let args = Array.from(arguments);
        return await find.apply(this, [this].concat(args));
    }

    async findOne(option, cb) {
        if (cb) return this.findOne(option).then((r)=>cb(null,r)).catch(cb);
        let args = Array.from(arguments);
        return await findOne.apply(this, [this].concat(args));
    }

    async insert(option, cb) {
        if (cb) return this.insert(option).then((r)=>cb(null,r)).catch(cb);
        let args = Array.from(arguments);
        return await insert.apply(this, [this].concat(args));
    }

    async update(option, cb) {
        if (cb) return this.update(option).then((r)=>cb(null,r)).catch(cb);
        let args = Array.from(arguments);
        return await update.apply(this, [this].concat(args));
    }

    async remove(option, cb) {
        if (cb) return this.remove(option).then((r)=>cb(null,r)).catch(cb);
        let args = Array.from(arguments);
        return await remove.apply(this, [this].concat(args));
    }

    async setConnection(connection){
        this.connection = connection;
    }
    async setTable(table){
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
 */
//basic.prototype.find=
async function find(model,option){
    //let args = Array.prototype.slice.call(arguments);
    //cb = args.pop();
    //option=args[1];
    let conn=model.connection;
    if (!option.select) option.select=_.keys(model.fields).join(',');
    return await conn.QueryLimit(model.baseSql,_.clone(option));
}
//basic.prototype.select=basic.prototype.find;

/**
 * 根据条码获取一条记录
 * @param model {Model}
 * @param option
 */
//basic.prototype.findOne=
async function findOne(model,option){
    option.limit=1; //只检索第一行
    let rows=await find(model,option);
    if (_.size(rows)===0) return;
    return rows[0];
}
//basic.prototype.selectOne=basic.prototype.findOne;

/**
 *
 * @param model
 * @param option {object|{where:object,commit:boolean}}
 */
async function remove(model,option) {
    //let args = Array.prototype.slice.call(arguments);
    //cb = args.pop();
    //where=args[1];
    let conn = model.connection;
    let {commit} = option;
    delete option["commit"];

    try {
        let WHERE;
        WHERE = model.getWhere(option.where || option);
        let sql = `delete ${model.table} ` + (WHERE ? 'WHERE ' + WHERE : '');
        let result = await conn.Execute(sql);

        if (commit) await conn.Commit();
        return result;
    } catch (err) {
        try {
            await conn.Rollback();
        } catch (err2) {
            console.error(err2);
        }
        return Promise.reject(err);
    }
}

/**
 *
 * @param model {Model}
 * @param option {[Object]|{fields:Array,values:Array,data:Array,[types]:Array,[commit]:boolean}}
 */
async function insert(model,option) {
    let args = Array.prototype.slice.call(arguments);
    // cb = args.pop();
    option = args[1];
    let conn = model.connection;

    let define = {};
    if (option instanceof Array) { //对象数组[{},...]
        _.merge(define, {data: option, model}); //
    } else {
        if (!option.data) return Promise.reject('需要data属性'); //二维数组
        _.merge(define, {model}, option);
    }
    let callbackResult = [];
    sqlHelper.INSERT(define, (err, sql, values, types) => {
        callbackResult = [err, sql, values, types];
    });

    let [err, sql, values, types] = callbackResult;

    if (err) return Promise.reject(err);
    if (err === 0) return 0;

    try {
        let result = await conn.Execute(sql, values, types);
        if (option.commit) await conn.Commit();
        return result;
    } catch (err) {
        return Promise.reject(err);
    }

}

/**
 *
 * @param model {Model}
 * @param option {{where,set,upsert,commit:boolean}}
 */
//basic.prototype.update=
async function update(model,option){
    //let args = Array.prototype.slice.call(arguments);
    //cb = args.pop();
    //option=args[1];
    let conn=model.connection;

    let fields=[];
    let values=[];
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

    try {
        let sql;
        sql = `UPDATE ${model.table} SET ${sets.join(',') + ' ' + (WHERE ? 'WHERE ' + WHERE : '')} `;
        let nrows = await conn.Execute(sql);

        if (nrows === 0 && option.upsert) {
            fields = fields.concat(_.keys(option.where));
            values = values.concat(_.values(option.where));
            let sql = `INSERT INTO ${model.table}(${fields.join(',')}) VALUES (${_.fill(new Array(fields.length), '?').join(',')}) `;
            nrows = await conn.Execute(sql, values);
        } else {
            // cont(null,nrows); //更新0行
        }

        if (option.commit) await conn.Commit();
        return nrows;
    }catch (err){
        try{
            await conn.Rollback();
        }catch (err2){
            console.error(err2);
        }
        return Promise.reject(err);
    }
}

module.exports=Model;
