/**
 * Created by Dbit on 2016/4/16.
 */

'use strict';

let _=require('lodash');
let qs=require('querystring');

exports.VALUE=VALUE;
exports.WHERE=json2Where;
exports.SELECT=str2Select;
exports.getSort=getSort;
exports.SORT=json2Sort;
exports.INSERT=getInsert;
exports.DATATYPE=getDataType;

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
        /**
         * @type {function}
         */

        let first=obj.shift();
        let fn;
        if (typeof obj[0]==='function') {
            fn=first; //取出函数
        }
        if (!fn && (typeof(first) ==='string') && first.slice(-2)==='()') {
            try{
                fn = _.get(fns,first.slice(0,-2));
            }catch (e){

            }
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
        if(Array.isArray(obj)===false) {
            throw new Error(key + " in 必须传入数组");
        }
        if (typeof obj[0]==='string'){
            return column(key)+` IN ('${obj.join(`','`)}') `;
        }else if (typeof obj[0]==='number') {
            return column(key)+` IN (${obj.join(',')}) `;
        }
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
        temp=column(fields[i]);
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
    return qs.stringify(newSort,',',' ')
}

/**
 * 获取insert SQL
 * @param option {object|{fields:Array,values:array,data:Array 二维数组或Array[Object],table:string}}
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
        let count=data.length; //行数
        if (count===0) {
            return callback(0); //无记录
        }

        let fields=option.fields; //传入的字段优先
        let values=option.values;

        if (data[0] instanceof Array){ //纯数据

            // let fieldStr;

            if ((fields instanceof Array===false) || _.size(fields)===0) {
                return callback('fields必须要传入一个非空数组');
            }
            let fieldCount;
            fieldCount=fields.length;
            // fieldStr=fields.join(',');

            if (data[0].length!==fieldCount) return callback('data列数与指定fields个数不一致!');

            let sql=`insert into ${option.table}(${fields}) values(${Array(fieldCount).fill('?').join(',')}) `;
            let _types=[];
             _.forEach(data[0],(v)=>{
                 _types.push(typeof v);
            }); //取首行的数据类型
            _.forEach(_types,(v,n)=>{ //遍历列
                if(option.types && option.types[n].toLowerCase()==='string' && v!=='string'){ //指定string,实际非string,则转String
                    for (let i=0;i<count;i++){ //遍历行
                        if (data[i][n]!=null) data[i][n]=String(data[i][n]);
                    }
                }else if (v==='string' && (option.types && option.types[n].toLowerCase()!=='string')) { //指定非string,实际string,则转Number
                    for (let i = 0; i < count; i++) { //行
                        if (data[i][n]!=null) data[i][n]=Number(data[i][n]);
                    }
                }else if (v==='boolean'){
                    for (let i=0;i<count;i++){ //遍历行
                        data[i][n]=data[i][n] ? 1 :0;
                    }
                }
            });
            callback(null,sql,data,_types);
        }else{

            // let fieldStr;

            if ((fields instanceof Array) && _.size(fields)===0) return callback('fields需要传入非空数组');
            if ((values instanceof Array) && _.size(values)===0) return callback('values需要传入非空数组');

            if (!fields) fields=_.keys(data[0]);

            let fieldCount;
            fieldCount=fields.length;
            // fieldStr=fields.join(',');
            if (!values) values=fields;
            if (values.length!==fields.length) return callback('values与fields长度不一致!');
            //for (let i=0;i<count;i++){
            //    let _values=[];
            //    for (let n=0;n<fieldCount;n++){
            //        _values.push(VALUE(data[i][values[n]]));
            //    }
            //    sqlList.push(`insert into ${option.table}(${fields}) values(${_values.join(',')}) `);
            //}
            //callback(null,sqlList);
            let newData=[]; //.fill([]);//这样填充的都是指向同一个对象
            let sql=`insert into ${option.table}(${fields}) values(${Array(fieldCount).fill('?').join(',')}) `;
            let _types=[];
            _.forEach(values,(v)=>{
                _types.push(typeof data[0][v]);
            }); //取首行的数据类型
            for (let i=0;i<count;i++){
                newData[i]=[];
            }

            _.forEach(_types,(v,n)=>{ //列
                if(option.types && option.types[n].toLowerCase()==='string' && v!=='string') { //指定string,实际非string,则转String
                    for (let i = 0; i < count; i++) { //行
                        newData[i][n] = data[i][values[n]]==null ? null : String(data[i][values[n]]);
                    }
                }else if (v==='string' && (option.types && option.types[n].toLowerCase()!=='string')) { //指定非string,实际string,则转Number
                    for (let i = 0; i < count; i++) { //行
                        newData[i][n] = data[i][values[n]]==null ? null : Number(data[i][values[n]]);
                    }
                }else if (v==='boolean') {
                    for (let i = 0; i < count; i++) { //行
                        newData[i][n] = data[i][values[n]] ? 1 : 0;
                    }
                }else{
                    for (let i=0;i<count;i++) { //行
                        newData[i][n] = data[i][values[n]];
                    }
                }
            });
            callback(null,sql,newData,_types);
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
                dataType = 'BigDecimal';// 'Double/Float';
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
