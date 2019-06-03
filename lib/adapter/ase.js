/**
 * Created by Dbit on 2017/12/3.
 */
const MyUtil=require('../MyUtil');
const Thenjs=require('thenjs');
const _=require('lodash');
class ase {
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
     * @param cb
     */
    static async setOption(conn,cb) {
        // Thenjs((cont) => {
           let rows= await conn.Query("SELECT top 1 DB_NAME(dbid) as dbname FROM master..sysprocesses WHERE spid=@@spid");
        // }).then((cont, rows) => {
            if (_.get(rows, '0.dbname') === 'master') return Promise.reject('DatabaseName not connect!');
            let sqls = ["set forceplan on","set plan optgoal allrows_oltp","set arithabort numeric_truncation off","set arithabort arith_overflow off","set table count 100"];
            let fns=[];

            // sqls.forEach((sql) => {
                fns.push(conn.ExecuteDDL(sqls.join(' ')));
            // });
            return await Promise.all(fns);
        // }).fin((c, err) => cb(err));
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
    static getCanIgnoreError() {
        /**
         * 207 查询时列不存在
         * 208 select/insert/update/delete 时表不存在
         * 12822 //创建临时表时,表已存在
         * 2714, //创建永久表时,表已存在
         * 1911 列未修改/索引列不存在(1911)
         */
        return [
            1913, //索引已存在1913
            2705, //增加列已存在
            3701, //删除索引时表不存在,删除表不存在
            13925, //列未修改(?)
            13900,
            13905,
            4963, //锁类型
            1906, //创建索引从属表不存在
            17782, //字段无需重复rename
            4902, //修改字段时表不存在
            18228 //修改字段名时表不存在
        ];
    }

    /**
     * @param conn {MyConnection}
     * @param sqlError {{ErrorCode,Message,SQLState,ErrorLevel}}
     * @return {*}
     */
    static async getSimpleError(conn,sqlError){
        if (!sqlError) return sqlError;
        // sqlError.ErrorCode=2714;
        if (sqlError.ErrorCode===0 && sqlError.Message.includes('Attempt to insert duplicate key row in object')) sqlError.ErrorCode = 2601;
        if (sqlError.ErrorCode===0 && sqlError.Message.includes('Incorrect syntax near the keyword')) sqlError.ErrorCode = 102;
        switch (sqlError.ErrorCode) {
            case 2601: { //[Error Code: 2601, SQL State: 23000]  Attempt to insert duplicate key row in object 'db' with unique index 'dbno1_index_db'
                let table = MyUtil.posValue(sqlError.Message, "in object '", "'");
                let index = MyUtil.posValue(sqlError.Message, "unique index '", "'");
                if (index && table) sqlError.Message = '不允许有重复行,约束:' + table + '.' + index;
                break;
            }
            case 2714: {
                if (conn.lastSQL && conn.lastSQL.startsWith('create table #')) { //数据库异常,临时表已存在应是:12822
                    try {
                        await conn.Close();
                    } catch (err) {
                        conn.logger.error(err);
                    }
                } else {
                    sqlError.ErrorLevel = 'WARN';
                }
                break;
            }
            case 233: {// [Error Code: 233, SQL State: 23000]  The column dbno in table db does not allow null values.
                let column = MyUtil.posValue(sqlError.Message, " column ", " in ");
                let table = MyUtil.posValue(sqlError.Message, " in table ", " does ");
                if (column && table) sqlError.Message = '不允许为空的字段:' + table + '.' + column;
                break;
            }
            case 257: {// [Error Code: 257, SQL State: 42000]  Implicit conversion from datatype 'INT' to 'VARCHAR' is not allowed.  Use the CONVERT function to run this query.
                let datatype = MyUtil.posValue(sqlError.Message, " to '", "' is ");
                let insertType = MyUtil.posValue(sqlError.Message, " datatype '", "' to ");
                if (insertType && datatype) sqlError.Message = '不允许向' +datatype+'类型的列中写入'+ insertType+'类型的数据.'; //存在数据类型不匹配的记录,
                break;
            }
            case 208:{
                //执行DML时,表不存在,如:[Error Code: 208, SQL State: 42000]  db20 not found. Specify owner.objectname or use sp_help to check whether the object exists (sp_help may produce lots of output).
                //执行增加字段时表不存在
                let table = MyUtil.posValue(sqlError.Message, "", " not found");
                if (table) sqlError.Message = '不存在的表或对象:' + table;
                break;
            }
            case 207:{  //[Error Code: 207, SQL State: ZZZZZ]  Invalid column name 'aaaa'.
                let column = MyUtil.posValue(sqlError.Message, "column name '", "'");
                if (column) sqlError.Message = '不存在的列名:' + column;
                break;
            }
            case 102: {  //Incorrect syntax near ','. //Incorrect syntax near the keyword \'key\'.\n'
                let words = MyUtil.posValue(sqlError.Message, "'", "'");
                if (words) sqlError.Message = '在 ' + words + ' 附近存在语法错误';
                break;
            }
        }
        return sqlError;
    }
}

module.exports=ase;
