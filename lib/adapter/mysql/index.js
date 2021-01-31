'use strict';

// const MyUtil=require('../../MyUtil');

class index {
    constructor() {

    }

    /**
     *
     * @param conn {MyConnection}
     */
    static async setOption(conn){

    }

    /**
     *
     * @param isolation {number|string}
     * @return {string}
     */
    static appendIsolation(isolation){
        return '';
    }

    /**
     *
     * @return {string}
     */
    static getSpidSql(){
        return "select connection_id() as spid,now() as getdate";
    }

    // static isCanIgnoreError(shortError){
    //     console.log(shortError);
    //     return true;
    // }
    /**
     *
     * @return {[]}
     */
    static getCanIgnoreError(){
        return [1051];  //创建表已存在,索引已存在,增加列已存在,列未修改,删除表(1051)/索引列不存在()
    }

    static getSimpleError(conn,sqlError){
        return sqlError;
    }
}

exports=module.exports=index;
exports.sqlHelper=require('./sqlHelper');
