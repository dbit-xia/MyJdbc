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
    static setOption(conn,cb) {
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
        return 'select @@spid as spid,getdate() as getdate';
    }

    /**
     *
     * @return {[]}
     */
    static getCanIgnoreError(){
        return [12006,1921,12006,2706,6226];  //创建表已存在12006,索引已存在1921,增加列已存在12006,列未修改,删除表(2706)/索引不存在6226
    }

    /**
     *
     * @param sqlError {{ErrorCode,Message}}
     * @return {*}
     */
    static getSimpleError(sqlError){
        if (!sqlError) return sqlError;
        switch (sqlError.ErrorCode) {
            case 548: {
                let table = MyUtil.posValue(sqlError.Message, "for table '", "'");
                let index = MyUtil.posValue(sqlError.Message, "Index '", "'");
                if (index && table) sqlError.Message = '不允许有重复行,约束:' + table + '.' + index;
                break;
            }
            case 233: {
                let column = MyUtil.posValue(sqlError.Message, " Column '", "'");
                let table = MyUtil.posValue(sqlError.Message, " in table '", "'");
                if (column && table) sqlError.Message = '不允许为空的字段:' + table + '.' + column;
                break;
            }
            case 257: {
                let datatype = MyUtil.posValue(sqlError.Message, " to a ", "");
                let insertType = MyUtil.posValue(sqlError.Message, " convert ", " to ");
                if (datatype) sqlError.Message = '不允许向' +datatype+'类型的列中写入['+ insertType+'].'; //存在数据类型不匹配的记录,
                break;
            }
            case 2706:{
                let table = MyUtil.posValue(sqlError.Message, "Table '", "'");
                if (table) sqlError.Message = '不存在的表:' + table;
                break;
            }
            case 504:{
                let table = MyUtil.posValue(sqlError.Message, "Procedure '", "'");
                if (table) sqlError.Message = '不存在的存储过程:' + table;
                break;
            }
            case 207:{
                let column = MyUtil.posValue(sqlError.Message, "Column '", "'");
                if (column) sqlError.Message = '不存在的列名:' + column;
                break;
            }
            case 102: {
                let words = MyUtil.posValue(sqlError.Message, "Syntax error near ", "");
                if (words) sqlError.Message = '在 ' + words + ' 附近存在语法错误';
                break;
            }
        }
        return sqlError;
    }
}

module.exports=ase;
