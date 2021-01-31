'use strict';

const MyUtil=require('../../MyUtil');

class index {
    constructor() {

    }

    /**
     *
     * @return {string}
     */
    static getdate() {
        return 'now()';
    }

    /**
     *
     * @return {string}
     */
    static spid() {
        return 'pg_backend_pid()';
    }

    /**
     *
     * @param conn
     * @param key
     * @return {Promise<*>}
     */
    static async getInfo(conn,key) {
        let rows;
        switch (key) {
            case 'dbname':
                rows = await conn.QueryGrid('select current_database()');
                break;
            case 'spid':
                rows = await conn.QueryGrid('select pg_backend_pid()');
                break;
            case 'current_time':
                rows = await conn.QueryGrid('select now()');
                break;
            default:
                rows = [[null]];
        }
        return rows[0][0];
    }

    /**
     *
     * @param conn {MyConnection}
     */
    static async setOption(conn) {
        conn._numberPrecision = true;
        let sqls = ["set default_transaction_isolation ='read committed'"];
        return await MyUtil.eachLimit(sqls,async (sql) => {
            return await conn.ExecuteDDL(sql);
        }, 10);
    }

    /**
     *
     * @param isolation {number|string}
     * @return {string}
     */
    static appendIsolation(isolation){
        return ' ';//at isolation ' + isolation;
    }

    /**
     *
     * @return {string}
     */
    static getSpidSql(){
        return 'select pg_backend_pid() as spid,now() as getdate';
    }

    /**
     *
     * @param shortError
     * @return {boolean}
     */
    static isCanIgnoreError(shortError) {
        //创建表已存在2714,索引已存在1913,增加列已存在,列未修改,删除表(3701)/索引列不存在(1911)
        // return ['42P01'].includes(shortError.SQLState);
        return false;
    }

    static getSimpleError(conn,sqlError){
        if (sqlError.SQLState === '40001') sqlError.Message = '发生并发操作，请重试!';
        return sqlError;
    }
}

exports=module.exports=index;
exports.sqlHelper=require('./sqlHelper');
