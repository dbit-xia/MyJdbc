/**
 * Created by Dbit on 2016/4/16.
 */

'use strict';

let _=require('lodash');
let qs=require('querystring');

exports.VALUE=VALUE;
exports.IN=IN;
exports.WHERE=json2Where;
exports.SELECT=str2Select;
exports.getSort=getSort;
exports.SORT=json2Sort;
exports.INSERT=getInsert;
exports.DATATYPE=getDataType;
exports.ifExpression=ifExpression;
exports.buildFIFO=buildFIFO;

let fns={
    getdate() {
        return 'getdate()';
    },
    substring(text,start,end){
        return `substring(${text},${start},${end})`;
    },
    concat(){
        let args = Array.prototype.slice.call(arguments);
        return args.join(' + ');
    },
    isnull(fields,truevalue){
        return `isnull(${fields},${truevalue})`;
    }
};

exports.fns=fns;

/**
 *
 * @param obj {*}
 * @param [column] {function}
 * @returns {string|number}
 * @constructor
 */
function VALUE(obj,column){
    if (typeof obj==='string'){
        return `'${obj.replace(/'/g,"''")}'`; //防止SQL注入
    }else if (typeof obj==='number') {
        return `${obj}`;
    }else if(typeof obj==='boolean') {
        return obj ? 1 : 0;
    }else if (obj instanceof Array){

        let first=obj.shift();
        /**
         * @type {function}
         */
        let fn;
        if (typeof obj[0]==='function') {
            fn=first; //取出函数
        }else if ((typeof(first) ==='string') && first.slice(-2)==='()') {
            fn = _.get(fns,first.slice(0,-2));
        }
        if (fn){
            let parms=_.map(obj,(value)=>{
                if (value instanceof Array) {
                    return column(value[0]); //取字段
                }else{
                    return VALUE(value); //常量
                }
            });
            return fn.apply(null,parms); //函数
        }
        return column(first); //取字段
    }
    return String(obj);
}

/**
 *
 * @param obj
 * @param column
 * @returns {string}
 * @constructor
 */
function IN(obj,column){
    if(Array.isArray(obj)===false) {
        throw new Error("必须传入数组");
    }
    obj=_.map(obj,(o)=>{
        // if (typeof o==='string') return VALUE(o); //防注入
        // if (o instanceof Array) return VALUE(o,column);
        // return o;
        return VALUE(o,column);
    });
    return `(${obj.join(',')}) `;
}

/**
 *
 * @param where
 * @param column
 * @returns {string}
 */
function json2Where(where,column){

    if (!column){
        column=function(field){
            return field;
        }
    }

    /**
     *
     * @param obj
     * @returns {string}
     * @constructor
     */
    function AND(obj){
        if (!obj) return '';
        let keys= Object.keys(obj);
        if (keys.length===0) return '';
        let results=[];
        for (let i=0;i<keys.length;i++){
            if (keys[i]==='$or') {
                results.push(OR(obj[keys[i]]));
            }else if (keys[i]==='$not'){
                results.push(NOT(obj[keys[i]]));
            }else if (keys[i]==='$where'){
                results.push(obj[keys[i]]);
            }else{
                if (obj[keys[i]]!==undefined){
                    results.push(EQUALS(keys[i],obj[keys[i]]));
                }
            }
        }
        return `(${results.join(' AND ')})`;
    }

    //数组
    /**
     * @return {string}
     */
    function OR(obj){
        let results=[];
        for (let i=0;i<obj.length;i++){
            results.push(AND(obj[i]));
        }
        return `(${results.join(' OR ')})`;
    }

    /**
     *
     * @param obj
     * @returns {string}
     * @constructor
     */
    function NOT(obj){
        return 'NOT ('+AND(obj)+')';
    }

    //字符串,数字,数组,对象
    /**
     * @return {string|Array}
     */
    function EQUALS(key,obj){
        if(obj instanceof Array){
            return column(key)+' = '+VALUE(obj,column); //支持字段1=字段2
        }else if(typeof obj==='object' && obj!==null){
            return COMPARE(key,obj);
        }else{
            return column(key)+' = '+VALUE(obj);
        }
    }

    //数组
    /**
     *
     * @param key
     * @param obj
     * @returns {string}
     * @constructor
     */
    function IN(key,obj){
        return column(key)+` IN ${exports.IN(obj,column)} `;
    }

    /**
     *
     * @param key
     * @param obj
     * @returns {string}
     * @constructor
     */
    function NIN(key,obj){
        if(Array.isArray(obj)===false) {
            throw new Error(key + " not in 必须传入数组");
        }
        if (typeof obj[0]==='string'){
            return column(key)+` NOT IN ('${obj.join(`','`)}') `;
        }else if (typeof obj[0]==='number') {
            return column(key)+` NOT IN (${obj.join(',')}) `;
        }
    }

    //对象
    /**
     *
     * @param key
     * @param obj
     * @returns {string}
     * @constructor
     */
    function COMPARE(key,obj){
        let keys= _.keys(obj);
        let results=[];
        for (let i=0;i<keys.length;i++){
            switch (keys[i]){
                case 'IN':
                case 'in': ///IN
                    results.push(IN(key,obj[keys[i]]));
                    break;
                case 'NIN':
                case 'nin': //NOT IN
                    results.push(NIN(key,obj[keys[i]]));
                    break;
                default: //<>,>,<,=,like,exists //此处会有SQL注入**
                    results.push(column(key) + ' ' + keys[i] + ' ' + VALUE(obj[keys[i]],column));
            }
        }
        return `(${results.join(' AND ')})`;
    }
    return AND(where);
}
/**
 * @return {string}
 */
function str2Select(selectStr,column){
    let fields=selectStr.split(',');
    let newFields=[];
    let temp;
    for (let i=0;i<fields.length;i++){
        fields[i]=fields[i].trim();
        temp=column(fields[i]); //
        newFields.push(temp===fields[i] ? temp : temp + ' AS "'+fields[i]+'"');
    }
    return newFields.join(',');
}

//加入固定排序,计算出最终sort
/**
 * @return {object}
 */
function getSort(newSort,fixSort){
    let lastSort={};
    _.merge(lastSort,fixSort);
    _.merge(lastSort,newSort);
    return lastSort;
}

/**
 * Object转sort字符串
 * @param lastSort
 * @param column
 * @returns {Object}
 */
function json2Sort(lastSort,column){
    let newSort=_.mapKeys(lastSort,(value,key)=>{
        return column(key);
    });
    return qs.stringify(newSort,',',' ',{encodeURIComponent:(a)=>a})
}

/**
 * 获取insert SQL, 不支持传入[字段名]的形式
 * @param option {object|{fields:Array,values:array,types:[],data:Array 二维数组或Array[Object],table:string}}
 * @param callback
 * @returns {Array}
 */
function getInsert(option,callback){
    //option={
    //    table:'itemcates',
    //    data:[{
    //        a:1,
    //        b:2
    //    }],
    //    fields:[],
    //    values:[]
    //};

    try {
        // let sqlList=[];
        let data=[].concat(option.data); //强制转换为数组
        let rowCount=data.length; //行数
        if (rowCount===0) {
            return callback(0); //无记录
        }

        let fields=option.fields; //传入的字段优先
        let values=option.values;

        if (data[0] instanceof Array) { //纯数据

            // let fieldStr;

            if ((fields instanceof Array === false) || _.size(fields) === 0) {
                return callback('fields必须要传入一个非空数组');
            }
            let fieldCount;
            fieldCount = fields.length;
            // fieldStr=fields.join(',');

            if (data[0].length !== fieldCount) return callback('data列数与指定fields个数不一致!');

            let sql = `insert into ${option.table}(${fields}) values(${new Array(fieldCount).fill('?').join(',')}) `;
            let _types = [];
            _.forEach(data[0], (v)=> {
                _types.push(typeof v);
            }); //取首行的数据类型
            _.forEach(_types, (v, n)=> { //遍历列
                if (option.types && option.types[n].toLowerCase() === 'string' && v !== 'string') { //指定string,实际非string,则转String
                    for (let i = 0; i < rowCount; i++) { //遍历行
                        if (data[i][n] != null) data[i][n] = String(data[i][n]);
                    }
                } else if (v === 'string' && (option.types && option.types[n].toLowerCase() !== 'string')) { //指定非string,实际string,则转Number
                    for (let i = 0; i < rowCount; i++) { //行
                        if (data[i][n] != null) data[i][n] = Number(data[i][n]);
                    }
                } else if (v === 'boolean') {
                    for (let i = 0; i < rowCount; i++) { //遍历行
                        data[i][n] = data[i][n] ? 1 : 0;
                    }
                }
            });
            callback(null, sql, data, option.types || _types);
        }else{ //values支持传入常量

            // let fieldStr;

            if ((fields instanceof Array) && _.size(fields)===0) return callback('fields需要传入非空数组');
            if ((values instanceof Array) && _.size(values)===0) return callback('values需要传入非空数组');

            if (!fields) fields=_.keys(data[0]);

            let fieldCount;
            fieldCount=fields.length;
            // fieldStr=fields.join(',');
            if (!values) values=fields;
            if (values.length!==fields.length) return callback('values与fields长度不一致!');
            if (option.types && _.size(option.types)!==fields.length) return callback('types与fields长度不一致!');
            //for (let i=0;i<count;i++){
            //    let _values=[];
            //    for (let n=0;n<fieldCount;n++){
            //        _values.push(VALUE(data[i][values[n]]));
            //    }
            //    sqlList.push(`insert into ${option.table}(${fields}) values(${_values.join(',')}) `);
            //}
            //callback(null,sqlList);
            let newData=[]; //.fill([]);//这样填充的都是指向同一个对象
            // let sql=`insert into ${option.table}(${fields}) values(${new Array(fieldCount).fill('?').join(',')}) `;
            // let sql=`insert into ${option.table}(${fields}) values(`;
            let constValues=[];
            let newValues=[];
            let newTypes=[];
            let _types=[];
            let wordsCheck = /^[A-Za-z0-9_]*$/;
            _.forEach(values,(v,n)=> {
                //是数字,或是常量字符串,
                // if (isNaN(v) === false || (typeof v === 'string' && (v.charAt(0) === "'" || v.charAt(0) === '"'))) {
                if (wordsCheck.test(v)===false){ //常量或表达式
                    constValues.push(v);
                } else {
                    constValues.push('?');
                    newValues.push(values[n]);
                    newTypes.push(option.types && option.types[n].toLowerCase() || (typeof data[0][v]));
                    _types.push(typeof data[0][v]);
                }
            }); //取首行的数据类型

            let sql=`insert into ${option.table}(${fields}) values(${constValues.join(',')}) `;

            for (let i=0;i<rowCount;i++){
                newData[i]=[];
            }

            _.forEach(_types,(v,n)=>{ //列
                if(newTypes[n]==='string' && v!=='string') { //指定string,实际非string,则转String
                    for (let i = 0; i < rowCount; i++) { //行
                        newData[i][n] = data[i][newValues[n]]==null ? null : String(data[i][newValues[n]]);
                    }
                }else if (v==='string' && (newTypes[n]!=='string')) { //指定非string,实际string,则转Number
                    for (let i = 0; i < rowCount; i++) { //行
                        newData[i][n] = data[i][newValues[n]]==null ? null : Number(data[i][newValues[n]]);
                    }
                }else if (v==='boolean') {
                    for (let i = 0; i < rowCount; i++) { //行
                        newData[i][n] = data[i][newValues[n]] ? 1 : 0;
                    }
                }else{
                    for (let i=0;i<rowCount;i++) { //行
                        newData[i][n] = data[i][newValues[n]];
                    }
                }
            });
            callback(null,sql,newData,newTypes);
        }

    }catch (err){
        callback(err);
    }

}


/**
 * 传入数组参数,返回数据类型的数组 用于setString|setInt|...
 * @param paramsList {Array}
 */
function getDataType(paramsList){
    let length = paramsList.length;
    // let finish = 0;
    let datatypes=[];
    let dataErr;
    let dataType;
    let data;
    for (let i = 0 ; i < length; i++) {
        data=null;
        dataErr=null;
        dataType = null;
        //dataerr=undefined;
        if ((paramsList[i] instanceof Array) === true) { /*做数组处理*/
            data = paramsList[i][0];
            dataType = paramsList[i][1];
            /*指定的类型*/
            if (dataType === undefined) { /*未传入类型*/
                dataType = typeof(data);
            }
        } else{
            data = paramsList[i];
            dataType = typeof(data);
        }

        switch (dataType) {
            case 'object'://null,日期等
                if (data === null) {
                    console.warn(i,'空值应指定数据类型');
                }
                dataType = "String";
                break;
            case 'string':
            case 'varchar':
            case 'char':
                dataType = 'String';
                break;
            case 'number':
                //BigDecimal 0.03会变成0.029
                dataType = 'Double';// 'BigDecimal/Double/Float';
                break;
            case 'boolean':
                dataType='Int';
                data=data?1:0;
                break;
            //dataType = 'Boolean';
            //break;
            case 'undefined':
            case 'function':
                dataErr = 'dataType不应为' + dataType+',作为String处理';
                dataType = "String";
                break;
            default:
                dataType = dataType.charAt(0).toUpperCase() + dataType.substr(1); //全小写转首字母大写
                break;
        }
        if(dataErr) console.warn(dataErr);

        datatypes.push(dataType);
    }
    return datatypes;
}

/**
 *
 * @param value
 * @param expression
 * @param dataType
 * @returns {*}
 */
function ifExpression(value,expression,dataType) {
    if (value == null) {
        return expression;
    } else {
        if (dataType === 'string') {
            return "'" + value + "'";
        } else {
            return value;
        }
    }
}

/**
 * 获取先进先出计算SQL
 * @param option
 * @returns {Array}
 */
function buildFIFO(option){

    // let option= {
    //     source: [{
    //         select: 'dbname,colthno,color,daynum',
    //         table: 'easytemp',
    //         where: {recno: 0},
    //         sort: {daynum: 'desc', dbname: 'asc'},
    //         nbFields: 'daynum,flags1,flags2',
    //         group: 'colthno,color'
    //     }, {
    //         select: 'odno,kh,fwd,sl,xhnb',
    //         table: 'huanankc',
    //         where: {recno: 0},
    //         sort: {xhnb: 'desc', odno: 'asc'},
    //         nbFields: 'sl,a1,a2',
    //         group: 'kh,fwd,odno'
    //     }],
    //     join: [
    //         [['colthno'], ['kh']],
    //         [['color'], ['fwd']]
    //     ]
    // };

    let sqls=[];

    sqls.push(buildUpdate(option.source[0]));
    sqls.push(buildUpdate(option.source[1]));

    let nbFields=[];
    nbFields[0] = option.source[0].nbFields.split(',');
    nbFields[1] = option.source[1].nbFields.split(',');

    sqls.push(`SELECT ${exports.SELECT(option.source[0].select,column.bind(null,'t1.'))+','+exports.SELECT(option.source[1].select,column.bind(null,'t2.'))}
        ,nb=(
            CASE WHEN t1.${nbFields[0][2]}<=t2.${nbFields[1][2]} AND t2.${nbFields[1][2]}<=t1.${nbFields[0][1]} AND t1.${nbFields[0][1]}<=t2.${nbFields[1][1]} THEN t1.${nbFields[0][1]} - t2.${nbFields[1][2]}
            WHEN t1.${nbFields[0][2]}<=t2.${nbFields[1][2]} AND t2.${nbFields[1][2]}<=t2.${nbFields[1][1]} AND t2.${nbFields[1][1]}<=t1.${nbFields[0][1]} THEN t2.${nbFields[1][1]} - t2.${nbFields[1][2]}
            WHEN t2.${nbFields[1][2]}<=t1.${nbFields[0][2]} AND t1.${nbFields[0][2]}<=t1.${nbFields[0][1]} AND t1.${nbFields[0][1]}<=t2.${nbFields[1][1]} THEN t1.${nbFields[0][1]} - t1.${nbFields[0][2]}
            WHEN t2.${nbFields[1][2]}<=t1.${nbFields[0][2]} AND t1.${nbFields[0][2]}<=t2.${nbFields[1][1]} AND t2.${nbFields[1][1]}<=t1.${nbFields[0][1]} THEN t2.${nbFields[1][1]} - t1.${nbFields[0][2]}
            ELSE 0 END)
        FROM ${option.source[0].table} t1,${option.source[1].table} t2
        WHERE ${exports.WHERE(option.source[0].where,column.bind(null,'t1.'))} AND ${exports.WHERE(option.source[1].where,column.bind(null,'t2.'))} 
            AND ${_.map(option.join,(row)=>{
                return _.map(row[0],(row2)=>'t1.'+row2).join('+') +'=' + _.map(row[1],(row2)=>'t2.'+row2).join('+');
            }).join(' AND ')}
            AND not(t1.${nbFields[0][2]}>t2.${nbFields[1][1]} OR t2.${nbFields[1][2]}>t1.${nbFields[0][1]})
    `);

    return sqls;

    function column(prefix,field) {
        if (field && field.slice(0,2)==='@@') return field;
        return prefix + field;
    }

    function buildUpdate(source) {
        let whereStr = exports.WHERE(source.where);
        let nbFields = source.nbFields.split(',');
        let groupArray = source.group.split(',');
        let sortKeys = _.keys(source.sort);
        return `UPDATE ${source.table}
            SET ${nbFields[1]}=(SELECT SUM(t2.${nbFields[0]}) FROM ${source.table} t2 
            WHERE ${whereStr}
             AND ${_.map(groupArray, (a) => {
                return 't2.' + a + '=t1.' + a;
            }).join(' AND ')}
             AND ${_.map(sortKeys, (sortKey) => {
                return '(t2.' + sortKey + (source.sort[sortKey] === 'desc' ? '>' : '<') + 't1.' + sortKey + ' OR (t2.' + sortKey + '=t1.' + sortKey;
            }).join(' AND ') + (')'.repeat(_.size(sortKeys) * 2))
                })
            FROM ${source.table} t1
            WHERE ${whereStr}
            UPDATE ${source.table} set ${nbFields[2]}=${nbFields[1]} - ${nbFields[0]} FROM ${source.table} t1 WHERE ` + whereStr;
    }
}