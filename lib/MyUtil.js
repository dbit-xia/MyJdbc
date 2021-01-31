/**
 * Created by Dbit on 2016/4/16.
 */

'use strict';
let _=require('lodash');
let Thenjs=require('thenjs');
const async=require('async');
const util=require('util');
const domain=require('domain');
let logger=global.logger || console;
/**
 *
 * @param args {object}
 * @returns {String}
 */
String.prototype.format = function(args) {
    let result = this;
    if (arguments.length > 0) {
        if (arguments.length === 1 && typeof (args) === "object") {
            for (let key in args) {
                if(args[key]!=undefined){
                    let reg = new RegExp("({" + key + "})", "g");
                    result = result.replace(reg, args[key]);
                }
            }
        }
        else {
            for (let i = 0; i < arguments.length; i++) {
                if (arguments[i] != undefined) {
                    //let reg = new RegExp("({[" + i + "]})", "g");//这个在索引大于9时会有问题，谢谢何以笙箫的指出
                    let reg= new RegExp("({)" + i + "(})", "g");
                    result = result.replace(reg, arguments[i]);
                }
            }
        }
    }
    return result;
};


class MyUtil {

    /**
     * 将占位符,替换为最终字符串
     * @param args {string,...}
     * @returns {string}
     */
    static str_format(args) {
        if (arguments.length <= 2) {
            return arguments[0].format(arguments[1]);
        }
        else {
            //let args_array = [];
            //for (let i = 1; i < arguments.length; i++) { //取除第一位的全部参数
            //    args_array[i - 1] = arguments[i];
            //}
            //暂支持5个参数
            return arguments[0].format(arguments[1], arguments[2], arguments[3], arguments[4], arguments[5]);
        }
    };

    /**
     *
     * @param date {Date}
     * @returns {string}
     * @constructor
     */
    static DateString(date) {
        //return new Date(Number(n)).toLocaleDateString();
        //let date = new Date(Number(n));
        let year = ('0000' + String(date.getFullYear())).slice(-4);
        let month = ('00' + String(date.getMonth() + 1)).slice(-2);
        let day = ('00' + String(date.getDate())).slice(-2);
        return year + '-' + month + '-' + day;
    }

    /**
     *
     * @param date {Date}
     * @returns {string}
     * @constructor
     */
    static TimeString(date) {
        //return new Date(Number(n)).toLocaleDateString();
        //let date = new Date(Number(n));
        let hh = MyUtil.str_right('00' + String(date.getHours()), 2);
        let mm = MyUtil.str_right('00' + String(date.getMinutes()), 2);
        let ss = MyUtil.str_right('00' + String(date.getSeconds()), 2);
        let fff = MyUtil.str_right('000' + String(date.getMilliseconds()), 3);
        return hh + ':' + mm + ':' + ss + '.' + fff;
    }

    /**
     *
     * @param date
     * @returns {string}
     * @constructor
     */
    static DateTimeString(date) {
        return MyUtil.DateString(date) + ' ' + MyUtil.TimeString(date);
    }

    /**
     *
     * @param text
     * @param n
     * @returns {string}
     */
    static str_right(text, n) {
        return text.substr(text.length - n);
    }

    /**
     *
     * @returns {string}
     */
    static str_rmInvalidchars(text) {
        if (_.isString(text) === false) return text;
        return text.replace(/[^0-9a-zA-Z０-９ａ-ｚＡ-Ｚ　\n\r\t\b\f\u4e00-\u9fa5\`\~\!\@\#\$\%\^\&\*\(\)\_\+\-\=\{\}\|\:\"\<\>\?\[\]\\\;\'\,\.\/ ～！￥…×（）—·「」、；‘，。《》？：“]/g, '?');
    }

    /**
     *
     * @param obj {string|object}
     * @param [keys] {Array}
     * @returns {*}
     */
    static parseJSON(obj, keys) {

        if (obj === null) return null;
        if (obj === undefined) return undefined;
        let ret;
        if (typeof obj === 'object') {
            if (!keys) return obj;
            if (Array.isArray(keys)) {
                for (let i = 0, count = keys.length; i < count; i++) {
                    if (obj[keys[i]] === undefined) continue;
                    ret = MyUtil.parseJSON(obj[keys[i]]);
                    if (ret instanceof Error) return ret;
                    obj[keys[i]] = ret;
                    ret = null;
                }
            }
        }

        if (typeof obj !== 'string') {
            return obj; //"parseJSON请传入string类型的参数:"+(typeof obj);
        }

        // if (!obj){
        //     return "parseJSON参数不能为空:"+String(obj);
        // }
        //if (text.trim()==='null' || text.trim()==='0' ){
        //    return "JSON解析失败:"+String(text);
        //}
        try {
            ret = JSON.parse(obj); //'0','null'都不会报错
            //if (typeof ret!=='object') return "JSON解析失败:"+obj;
            return ret;
        } catch (e) {
            e.stack = "JSON解析失败:" + ' ' + obj + ' \n' + MyUtil.String(e);
            console.debug(e);
            return e;
        }
    }

    /**
     *
     * @param protocol
     * @param host
     * @param port
     * @param baseurl
     * @returns {string}
     */
    static joinUrl(protocol, host, port, baseurl) {
        return (protocol || 'http') + '://' + host + ((port || 80) === 80 ? "" : ":" + port) + (baseurl || '');
    }

    /**
     *
     * @param _this
     * @param propstr
     * @returns {*}
     */
    static getValue(_this, propstr) {
        let array = propstr.split('.');
        if (!_this) return _this;
        for (let i = 0; i < array.length; i++) {
            _this = eval('_this.' + array[i]);
            if (_this === undefined || _this === null) return _this;
        }
        return _this;
    }

    /**
     *
     * @param a {object|Error|string}
     * @returns {string}
     */
    static String(a) {
        if (a == null) return '';
        if (typeof a === 'object') {
            if (a instanceof Array) {
                return _.map(a, (o, index) => index + ':' + MyUtil.String(o)).join(';'); //递归调用
            }
            if (a instanceof Error) {
                // console.error(Error(a));
                return a["stack"] || a.message || util.inspect(a); //尽量精简错误信息
            }
            return JSON.stringify(a);
        } else if (typeof a === 'string') {
            return a;
        }
        return String(a);
    }

    /**
     *
     * @param obj {object}
     * @param keys {Array}
     */
    static getKeys(obj, keys) {
        if (keys instanceof Array === false) return {};
        let newObj = {};
        for (let i = 0; i < keys.length; i++) {
            if (obj[keys[i]] !== undefined) {
                newObj[keys[i]] = obj[keys[i]];
            }
        }
        return newObj;
    }

    /**
     * 并行执行函数,无论是否出现错误,均等待一组全部执行结束才返回
     * 而thenjs.parallelLimit是一旦出现错误,则直接返回
     * @param parallel {Array}
     * @param limit {number}
     * @param cb {function(*=,*=)}
     */
    static parallelLimit(parallel, limit, cb) {

        //let limit=2;
        if (parallel instanceof Array === false) return cb('第一个必须是一个函数数组!');
        let total = parallel.length;
        if (!total) {
            return cb(null, []);
            // return cb(new Error('数组个数至少要有一个元素!'));
        }
        let finishCount = 0;
        let results = {};
        let errors = {};
        let hasError = false;
        let okCount = 0;
        Thenjs.eachLimit(parallel, (cont, fn, n) => {

            try {
                if (Object.prototype.toString.call(fn) === '[object AsyncFunction]') { //增加支持async+await
                    fn().then(r => callback(null, r)).catch(callback);
                    return;
                }
                fn(callback);
            } catch (e) {
                callback(e);
            }

            function callback(err, result) {
                if (err) {
                    if (err instanceof Error) logger.error(err);
                    errors[String(n)] = err;
                } else {
                    okCount++;
                    results[String(n)] = result;
                }
                finishCount++;
                cont(err, result); //如果有错误,则触发fail,并重新计算执行总数

                if (finishCount >= total) { //全部结束
                    cb((hasError ? errors : null), (okCount ? results : null));
                }
            }
        }, limit).fail((cont, err) => { //fail 只会被首次出错调用一次
            if (hasError === false && err) {
                hasError = true;
                if (limit && (finishCount + (limit - 1) < total)) {
                    total = finishCount + (limit - 1); //计算最后一个
                }
            }
        });
    }

    /**
     *
     * @param array {Array|Map|Set}
     * @param iterator {function(cb,row,index)}
     * @param limit
     * @param cb
     */
    static eachLimit(array, iterator, limit, cb) {
        if (arguments.length !== 4) throw Error('eachLimit参数必须是4个: ' + arguments.length);
        if (!array || !array.forEach) throw Error('第一个参数必须存在forEach方法!');
        let fns = [];
        array.forEach((value, index) => {
            fns.push((cont) => {
                if (Object.prototype.toString.call(iterator) === '[object AsyncFunction]') {
                    iterator(value, index).then(r => cont(null, r)).catch(cont);
                    return;
                }
                iterator(cont, value, index);
            });
        });
        MyUtil.parallelLimit(fns, limit, cb);
    }

    /**
     * 检查必填项
     * @param data {object}
     * @param notNull {Array|object|String}
     * @param [parentStr] {string}
     * @returns {string|null}
     */
    static checkNull(data, notNull, parentStr) {
        if (!notNull) return null;
        if ((typeof notNull) === 'string') notNull = notNull.split(','); //转数组
        if (notNull instanceof Array) {
            let count = notNull.length;
            let temp;
            let result = [];
            for (let i = 0; i < count; i++) {
                temp = _.get(data, notNull[i]);
                if (temp === null || temp === undefined || temp === '') {
                    result.push((parentStr ? parentStr + '.' : '') + notNull[i] + '不能为空!');
                }
            }
            if (result.length > 0) return result.join(',');
            return null;
        } else if (notNull instanceof Object) {
            let keys = _.keys(notNull);
            let count = keys.length;
            let temp;
            let result = [];
            for (let i = 0; i < count; i++) {
                temp = _.get(data, keys[i]);
                if (temp === null || temp === undefined || temp === '') {
                    result.push((parentStr ? parentStr + '.' : '') + keys[i] + '不能为空!');
                    continue;
                }
                temp = MyUtil.checkNull(data[keys[i]], notNull[keys[i]], (parentStr ? parentStr + '.' : '') + keys[i]);
                if (temp) result.push(temp);
            }
            if (result.length > 0) return result.join(',');
            return null;
        } else {
            return 'notNull请传入一个对象/数组!';
        }
    }

    /**
     * 当请求一个条件响应结果需要多页时,自动获取多页数据
     * @param fn {function (object,function(err,result))|AsyncFunction(object)}
     * @param fixData {object}
     * @param oField {{[has_next],[page_no],[totals],[page_count],page_size,key,[key_sort],[sort_value],rows,first:object,[first_data]:boolean,[parallelLimit]:number,[addWheres]:[object]}}
     * @param iteratee {function(rows,maxCreated,cb)}
     * @param cb {*}
     */
    static restPages(fn, fixData, oField, iteratee, cb) {
        let addWheres = oField.addWheres;
        if (_.size(addWheres) > 0) {
            let _oField = _.clone(oField); //保持参数
            delete _oField.addWheres; //
            let serisefns = [];
            _.forEach(addWheres, (addWhere) => {
                serisefns.push((cont) => {
                    let fixData2 = _.clone(fixData);
                    _.merge(fixData2, addWhere); //附加动态条件
                    MyUtil.restPages(fn, fixData2, _oField, iteratee, cont); //递归
                });
            });

            let _parallelLimit = _.get(_oField, 'parallelLimit');
            if (_parallelLimit > 1) {
                // console.debug('并行addwheres:',_parallelLimit);
                MyUtil.parallelLimit(serisefns, _parallelLimit, cb);
            } else {
                //串行依次执行调用
                Thenjs.series(serisefns).fin((c, err, result) => {
                    cb(err, result);
                });
            }

            return;
        }
        //获取响应数据记录数
        function getCurrentCount(data) {
            // if() {
            return _.size(oField.rows ? _.get(data, oField.rows) : data);
            // }
        }
        /**
         * 1.按照时间段调用接口
         * 2.page_size=40 & page_no=1 调用接口
         * 3.记录总行数,计算出总页数
         * 4.记录到mongodb -->获取条码
         * 5.如果本页最大修改时间>增量最大修改时间,则更新
         * 6.page_no+1 循环第2步骤
         */

        let totals;//总行数
        let readCount = 0;
        let page_size = fixData[oField.page_size]; //页大小
        let page_count;
        let pageNo = 0;
        //先时间升序取第一页
        //再时间降序取总页数 - 1页
        //直到时间降序第一页
        let fnType = Object.prototype.toString.call(fn);
        Thenjs((cont) => {
            let postData = {};
            _.merge(postData, fixData, oField.first, JSON.parse('{"' + oField.page_no + '":1}'));
            if (fnType === '[object AsyncFunction]') {
                fn(postData).then((v) => cont(null, v)).catch(cont);
            } else {
                fn(postData, cont);
            }
        }).then((cont, data) => {
            if (oField.totals) {
                totals = _.get(data, oField.totals);
                page_count = Math.ceil(totals / page_size) || 0;
            }
            if (oField.totals && totals === 0) { //无记录时强制触发一下
                fetched(data, (err) => {
                    if (err) return cont(err);
                    return cb(null, 0);
                });
            } else if (oField.first_data) { //使用首次调用的数据
                readCount += getCurrentCount(data);
                pageNo++;
                fetched(data, (err) => {
                    if (err != null) return cont(err);
                    if (oField.totals && page_count <= 1) return cb(null, readCount); //仅一页
                    if (oField.has_next && oField.has_next(data) === false) return cb(null, readCount); //仅一页
                    cont();
                });
            } else {
                cont();
            }
        }).then((cont) => {
            // console.log(oField)
            if (oField.has_next) { //仅使用有无下一页,循环获取
                // let data;
                let iteratee = function (cont2) {
                    let postData = {};
                    pageNo++;
                    _.merge(postData, fixData, JSON.parse('{"' + oField.page_no + '":' + pageNo + '}'));
                    if (fnType === '[object AsyncFunction]') {
                        fn(postData).then((result) => {
                            readCount += getCurrentCount(result);
                            fetched(result, (err) => {
                                if (err) return cb(err);
                                cont2(null, result); //返回结果给test,以检查是否有下一页
                            });
                        }).catch(cont2);
                    } else {
                        fn(postData, (err, result) => {
                            if (err) return cont2(err);
                            readCount += getCurrentCount(result);
                            fetched(result, (err) => {
                                if (err) return cb(err);
                                cont2(null, result); //返回结果给test,以检查是否有下一页
                            });
                        });
                    }
                };
                async.doUntil(iteratee, (data) => {
                    return !oField.has_next(data); //直到has_next=false
                }, (err) => {
                    if (err) return cont(err);
                    cont(null, readCount);
                });
                return;
            }

            let range;
            if (oField.first_data) { //跳过首次调用的数据 (前面一个then已经收集了)
                if (oField.sort_value === 'asc') {
                    range = _.range(2, page_count + 1, 1); //[2~page_count]
                } else {
                    range = _.range(page_count, 1, -1); //[page_count~2]
                }
            } else {
                if (oField.sort_value === 'asc') {
                    range = _.range(1, page_count + 1, 1); //[1~page_count]
                } else {
                    range = _.range(page_count, 0, -1); //[page_count~1]
                }
            }


            let _parallelLimit = _.get(oField, 'parallelLimit');
            if (_parallelLimit > 1) {
                let fns = [];
                _.forEach(range, (n) => {
                    fns.push((cont) => {
                        let postData = {};
                        _.merge(postData, fixData, JSON.parse('{"' + oField.page_no + '":' + String(n) + '}'));
                        if (fnType === '[object AsyncFunction]') {
                            fn(postData).then((data) => fetched(data, cont)).catch(cont);
                        } else {
                            fn(postData, (err, data) => {
                                if (err) return cont(err);
                                fetched(data, cont);
                            });
                        }
                    });
                });
                // console.debug('并行:',_parallelLimit);
                MyUtil.parallelLimit(fns, _parallelLimit, cont);
            } else {
                //依次调用
                Thenjs.eachSeries(range, (cont, n) => {
                    let postData = {};
                    _.merge(postData, fixData, JSON.parse('{"' + oField.page_no + '":' + String(n) + '}'));
                    if (fnType === '[object AsyncFunction]') {
                        fn(postData).then((data) => fetched(data, cont)).catch(cont);
                    } else {
                        fn(postData, (err, data) => {
                            if (err) return cont(err);
                            fetched(data, cont); //err=0为请求结束
                        });
                    }
                }).fin(cont); //err=0为请求结束
            }
        }).fin((cont, err) => { //成功
            cb(err || null, readCount); //返回0认为成功
        });//.fail((cont, err)=> {
        //     cb(err || null,readCount); //返回0认为成功
        // });

        function fetched(data, cont) {
            let maxKey, minKey;
            let rows = oField.rows ? _.get(data, oField.rows) : data;
            let rows_size = _.size(rows);
            if (rows_size > 0 && oField.key) { //取最大值
                let firstValue = rows[0][oField.key];
                let lastValue = rows[rows_size - 1][oField.key];

                //检查响应数据
                if (oField.key_sort) {
                    if (oField.key_sort === 'desc') { //数据降序
                        minKey = lastValue;
                        maxKey = firstValue;
                    } else if (oField.key_sort === 'asc') { //数据升序
                        minKey = firstValue;
                        maxKey = lastValue;
                    } else {
                        return cont({code: -1, msg: 'key_sort参数无效:' + oField.key_sort}); //计算最大值
                    }
                    if (maxKey < minKey) return cont({
                        code: -1,
                        msg: '返回数据排序不符合预期:' + JSON.stringify({key: oField.key, key_sort: oField.key_sort, maxKey, minKey})
                    });
                } else { //兼容
                    if (oField.sort_value === 'desc') {
                        if (firstValue < lastValue) {
                            console.warn('当前按页码降序请求,实际响应数据却为升序,可能存在漏单风险!(如确认无误,可传入key_sort:asc消除警告):'
                                + JSON.stringify({
                                    key: oField.key,
                                    sort_value: oField.sort_value,
                                    firstValue,
                                    lastValue
                                }));
                            maxKey = lastValue;
                        } else {
                            maxKey = firstValue;
                        }
                    } else {
                        if (firstValue > lastValue) {
                            console.warn('当前按页码升序请求,实际响应数据却为降序,可能存在漏单风险!(如确认无误,可传入key_sort:desc消除警告):'
                                + JSON.stringify({
                                    key: oField.key,
                                    sort_value: oField.sort_value,
                                    firstValue,
                                    lastValue
                                }));
                            maxKey = firstValue;
                        } else {
                            maxKey = lastValue;
                        }
                    }
                }
            }
            return iteratee(rows, maxKey, (err, result) => {
                if (err) return cont(err);
                // console.debug({rows_size ,page_size});
                if ((oField.sort_value === 'asc') && (rows_size < page_size)) {
                    return cont(0, result); //页码升序,响应数据小于指定页大小时,不继续请求,返回err=0为成功
                } else {
                    return cont(null, result);
                }
            });
        }
    }

    /**
     * 将时间段切分成数组 (size单位为ms)
     * @param startDate {Date}
     * @param endDate {Date}
     * @param ms {Number}
     */
    static dateRangeSplit(startDate, endDate, ms) {
        if (!ms) return [startDate, endDate];
        if ((startDate instanceof Date) === false) return new Error('startDate必须是Date类型');
        if ((endDate instanceof Date) === false) return new Error('endDate必须是Date类型');
        let dateList = [];
        //计算毫秒差
        let diffSSS = endDate - startDate;
        dateList.push(startDate);
        for (let i = ms; i < diffSSS; i = i + ms) {
            dateList.push(new Date(startDate.getTime() + i));
        }
        dateList.push(endDate);
        return dateList;
    }
    static createDomain(oldCallback, cb) {
        if (_.has(oldCallback.domain)) return cb(null, oldCallback);

        let d = domain.create();
        let newCallback = function (err) { //已捕获的异常或结果
            let args = _.toArray(arguments);
            if (err instanceof Error) {
                args[0] = {code: -1, msg: global.MyUtil.String(err)}; //兼容Socket无法传出Error对象
            }
            oldCallback.apply(this, args);
            d.exit();
        };
        newCallback.domain = true;
        newCallback.ctx = oldCallback.ctx;
        d.on('error', (err) => { //未捕获的异常
            if (err instanceof Error) err = {code: -1, msg: global.MyUtil.String(err)}; //兼容Socket无法传出Error对象
            oldCallback(err);
            d.exit();
            process.emit('uncaughtException', err);
        });
        d.enter();
        try {
            cb(null, newCallback);
        } catch (err) {
            newCallback(err);
        }
    }

    /**
     * 从一个文本中截取指定两个字符串之前的文本
     * @param text {string}
     * @param field {string} allowEmpty
     * @param endstr {string} allowEmpty
     * @return {*}
     */
    static posValue(text, field, endstr) {
        //查找以分号结束的值
        let ls_value;
        let l_row, l_pos, l_endpos, l_startpos, l_endlen;
        if (!field) {
            if (!endstr) return text;
            l_endpos = text.indexOf(endstr);
            if (l_endpos === -1) return text;
            return text.slice(0, l_endpos);
        }
        l_pos = text.indexOf(field);
        if (l_pos > -1) {
            l_startpos = l_pos + field.length;
            if (!endstr) return text.slice(l_startpos); //到末尾
            l_endpos = text.indexOf(endstr, l_startpos);
            if (l_endpos === -1) {
                return text.slice(l_startpos); //到末尾
            } else {
                ls_value = text.slice(l_startpos, l_endpos);
            }
        }
        return (ls_value || '');
    }

    static sleep(fff) {
        logger.debug('sleep:' + fff + 'ms');

        let timeoutHandle;
        let returnPromise;
        returnPromise = new Promise((resolve) => {
            timeoutHandle = setTimeout(() => {
                if (returnPromise) {
                    returnPromise.end = function () {
                        logger.debug('sleep is closed');
                    }
                }
                if (timeoutHandle && (timeoutHandle["_called"] === false)) {
                    // logger.debug('sleep is cancel');
                    clearTimeout(timeoutHandle);
                    // logger.debug('sleep end3')
                    resolve('EARLY_EXIT');
                } else {
                    // logger.debug('sleep end2')
                    resolve('TIME_END');
                }
            }, fff);
        });
        returnPromise.end = timeoutHandle["_onTimeout"]; //
        return returnPromise;
    }
    /**
     *
     * @param cbFn {Function|Promise|AsyncFunction}
     * @return {AsyncFunction}
     */
    static toAsync(cbFn) {

        const prototypeString = Object.prototype.toString.call(cbFn);

        if (prototypeString === '[object AsyncFunction]') return cbFn;

        if (prototypeString === '[object Promise]') {
            return async function (...args) {
                return cbFn.apply(this, args);
            };
        }

        if (prototypeString === '[object Function]') {
            let _cbFnAsync = util.promisify(cbFn);
            return async function (...args) {
                if ((typeof args[args.length - 1]) === 'function') {
                    if ((typeof args[args.length - 2]) === 'function') console.warn('fn(...,fn1,fn2); You may get incorrect results.');
                    return cbFn.apply(this, args);
                } //callback
                return _cbFnAsync.apply(this, args); //return promise
            };
        }

        throw new Error("toAsync(fn)不支持的参数类型:" + prototypeString);
    }
    /**
     *
     * @param obj
     * @param [sep]
     * @param [eq]
     * @return {string}
     */
    static toLine(obj, sep, eq) {
        if (!obj) return String(obj);
        if (_.isArray(obj)) return String(obj);
        if ((typeof obj) === 'object') {
            if (sep == null) sep = ';';
            if (eq == null) eq = '=';
            let result = '';
            for (let key in obj) {
                result += key + eq + MyUtil.String(obj[key]) + sep;
            }
            return result;
        }
        return MyUtil.String(obj);
    }
    /**
     * 转Boolean
     * @param value {string|number|boolean}
     * @param [defaultValue] {boolean}
     * @return {boolean}
     */
    static toBoolean(value,defaultValue) {
        if (defaultValue == null) defaultValue = false;
        if ([undefined, NaN, null].includes(value)) return defaultValue;
        if ([0, false].includes(value)) return false;
        if (typeof value === 'string') {
            if (value.trim() === '') return defaultValue;
            if (['false', 'no', '0', 'off', 'empty', 'null'].includes(value.trim().toLowerCase())) return false;
        }
        return true;
    }

    /**
     * @param arrays {[Array]}
     * @param [option] {{[max2dArrayLength]}}
     * @return string
     */
    static inspect(arrays, option) {

        if (!option) option = {};
        option.breakLength = option.breakLength || Math.max(30000, util.inspect.defaultOptions.breakLength);
        option.maxArrayLength = option.maxArrayLength || Math.max(100, util.inspect.defaultOptions.maxArrayLength);

        //只处理2维数组
        if ((arrays instanceof Array) === true && (arrays[0] instanceof Array) === true) {

            let max2dArrayLength = option.max2dArrayLength || 3;

            let result = "[ ";
            for (let index = 0; index < Math.min(max2dArrayLength, arrays.length); index++) {
                result += (index > 0 ? ', ' : '') + util.inspect(arrays[index], option);
            }

            if (arrays.length > max2dArrayLength) {
                result += ', ... ' + (arrays.length - max2dArrayLength) + ' more items ';
            }
            result += "]";

            return result;
        }
        return util.inspect(arrays, option);
    }
    
    /**
     * 规范传入的表名是否有非法符号,否则返回Error对象,可用于SQL拼接时防SQL注入
     * @param tname String
     * @return {Error|String}
     */
    static normalizeTableName(tname) {
        if (String(tname).match(/^[_a-zA-Z][_a-zA-Z0-9]*$/)) {
            return tname;
        } else {
            return Error('invalid-tablename');
        }
    }
    
    /**
     * 规范传入的列名是否有非法符号,否则返回Error对象,可用于SQL拼接时防SQL注入
     * @param tname String
     * @return {Error|String}
     */
    static normalizeColumnName(tname) {
        if (String(tname).match(/^[_a-zA-Z][_a-zA-Z0-9]*$/)) {
            return tname;
        } else {
            return Error('invalid-columnname');
        }
    }
    
    /**
     * 校验code是否有非法符号,否则返回Error对象,可用于SQL拼接时防SQL注入
     * @param tname
     * @param [regExp]
     * @returns {Error|String}
     */
    static normalizeCode(tname,regExp) {
        if (String(tname).match(regExp || /^[_a-zA-Z0-9]*$/)) {
            return tname;
        } else {
            return Error('invalid-code');
        }
    }
}

MyUtil.parallelLimitPromise = MyUtil.parallelLimit = MyUtil.toAsync(MyUtil.parallelLimit);
MyUtil.eachLimitPromise = MyUtil.eachLimit = MyUtil.toAsync(MyUtil.eachLimit);
MyUtil.eachLimit=MyUtil.toAsync(MyUtil.eachLimit);
/**
 *
 * @type {MyUtil}
 */
module.exports=MyUtil;


// console.log(MyUtil.normalizeTableName('a123'))
// console.log(MyUtil.normalizeTableName('a1!'))
// console.log(MyUtil.normalizeTableName('1a'))
// console.log(MyUtil.normalizeTableName('!a123'))
