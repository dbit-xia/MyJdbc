/**
 * Created by Dbit on 2017/12/3.
 */
const MyUtil=require('../MyUtil');

class mysql {
    constructor() {

    }

    /**
     *
     * @param conn {MyConnection}
     * @param cb
     */
    static async setOption(conn,cb){
        cb();
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

module.exports=mysql;
