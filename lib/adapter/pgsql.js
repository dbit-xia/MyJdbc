/**
 * Created by Dbit on 2017/12/3.
 */
const MyUtil=require('../MyUtil');

class pgsql {
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
     * @param conn {MyConnection}
     */
    static async setOption(conn){
        // let sqls = [];
        // return await MyUtil.eachLimit(sqls,async (sql) => {
        //     return await conn.ExecuteDDL(sql);
        // }, 10);
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
     * @return {[]}
     */
    static getCanIgnoreError(){
        return [];  //创建表已存在2714,索引已存在1913,增加列已存在,列未修改,删除表(3701)/索引列不存在(1911)
    }

    static getSimpleError(conn,sqlError){
        return sqlError;
    }
}

module.exports=pgsql;
