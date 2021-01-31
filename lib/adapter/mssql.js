/**
 * Created by Dbit on 2017/12/3.
 */
const MyUtil=require('../MyUtil');

class mssql {
    constructor() {

    }

    /**
     *
     * @return {string}
     */
    static getdate() {
        return 'getdate()';
    }

    /**
     *
     * @return {string}
     */
    static spid() {
        return '@@spid';
    }

    /**
     *
     * @param conn {MyConnection}
     */
    static async setOption(conn){
        let sqls = ["set forceplan on"];
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
        return ' at isolation ' + isolation;
    }

    /**
     *
     * @return {string}
     */
    static getSpidSql(){
        return 'select @@spid as spid,getdate() as getdate';
    }

    /**
     *
     * @return {[]}
     */
    static getCanIgnoreError(){
        return [2714,1913,2705,3701,13925,13900,13905];  //创建表已存在2714,索引已存在1913,增加列已存在,列未修改,删除表(3701)/索引列不存在(1911)
    }

    static getSimpleError(conn,sqlError){
        return sqlError;
    }
}

module.exports=mssql;
