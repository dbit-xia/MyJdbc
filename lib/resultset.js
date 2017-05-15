/**
 * Created by Dbit on 2016/8/28.
 */
"use strict";
let ResultSet=require('../jdbc/lib/resultset.js');
let _ = require('lodash');
let Thenjs=require('thenjs');

//console.log();
//JDBC ResultSet添加absolute方法,用于分页process.cwd()+"/node_modules/
//var ResultSet=require("jdbc/lib/resultset.js");
ResultSet.prototype.absolute = function(rows,callback) {
    this._rs.absolute(rows,function(err) {
        return callback(err);
    });
};

/**
 *
 * @param option {{grid:true}}
 * @param callback
 */
ResultSet.prototype.toObjArray = function(option,callback) {
    let args=_.toArray(arguments);
    callback=args.pop();
    option=args[0];
    let dataGrid=option && option.grid;
    let self = this;
    /**
     *
     * @type {[{label,type}]}
     */
    let metadata=[];
    let rsmd;
    let rowCount,fieldsCount;
    let gridData;


    let outTable;
    Thenjs((cont)=>{
        ResultSet.prototype.nodeJdbc.rs2table3(self._rs,cont); //self._rs,
    }).then((cont,result)=> {
        if (!result) return callback(null, null); //无记录
        if (typeof result==='string'){
            gridData=JSON.parse(result);
        }else{
            gridData=result;
        }
        rowCount = _.size(gridData);
        // console.info(rowCount);
        if (dataGrid) return callback(null, gridData); //返回表格数据
        cont();
    }).fail((cont,err)=>{
        callback(err);
    }).then((cont)=>{
        // Get some column metadata.
        self.getMetaData(cont);
    }).then((cont,result)=>{
        rsmd=result;
        rsmd.getColumnCount(cont);
    }).then((cont,count)=> {
        fieldsCount = count;
        let parallelFn = [];
        for (let i = 1; i <= fieldsCount; i++) {
            metadata[i - 1] = {};
            parallelFn.push((cont)=> {
                rsmd._rsmd.getColumnLabel(i, (err, result)=> {
                    if (err) return cont(err);
                    metadata[i - 1].label = result;
                    cont();
                });
            });
            parallelFn.push((cont)=> {
                rsmd._rsmd.getColumnType(i, (err, result)=> {
                    if (err) return cont(err);
                    metadata[i - 1].type = result;
                    cont();
                });
            });
        }

        Thenjs.parallel(parallelFn).then((cont)=> {
            var rows = [];
            for (var r = 0; r < rowCount; r++) {
                rows[r] = {};
                for (var c = 0; c < fieldsCount; c++) {
                    rows[r][metadata[c].label] = gridData[r][c];
                }
            }
            cont(null, rows);

            // // console.time('fetch');
            // next((err,result)=>{
            //     // console.timeEnd('fetch');
            //     callback(err,result);
            // });
            //
            // function next(cb){
            //     fetch((err,row)=> {
            //         if (err) return cb(err);
            //         if (!row.done) {
            //             rows.push(row.value);
            //             return next(cb);
            //         }
            //         cb(null, rows);
            //     });
            // }
        }).fin((c, err, result)=> {
            cont(err,result);
        });
    }).fin((c,err,result)=>callback(err,result));

    function fetch(cb){
        let result = {};
        let rowResult=[];
        Thenjs((cont)=> {
            self._rs.next(cont);
        }).then((cont, nextRow)=> {
            if (!nextRow) return cb(null, {done: true});
            // option;
            let parallelFn = [];
            // loop through each column
            for (let i = 1; i <= fieldsCount; i++) {

                let cmd = metadata[i - 1];
                let type = self._types[cmd.type] || 'String';
                let getter = 'get' + type; //+ 'Sync'

                parallelFn.push((cont)=> {
                    // self._rs['getAsciiStream'](i,cont);
                    self._rs[getter](i, (err, dateVal)=> {
                        if (err) return cont(err);
                        switch (type) {
                            case 'BigDecimal'://nodeJava_java_math_BigDecimal
                                dateVal = Number(dateVal);
                                // result[cmd.label] = Number(dateVal);
                                break;
                            case 'Date':
                            case 'Time':
                            case 'Timestamp':
                                dateVal = dateVal && dateVal.toString();
                                // result[cmd.label] = dateVal && dateVal.toString();
                                break;
                            default:
                                // result[cmd.label] = dateVal;
                                break;
                        }
                        if (dataGrid) {
                            rowResult[i - 1] = dateVal;
                        } else {
                            result[cmd.label] = dateVal;
                        }
                        cont();
                    });
                });
            }
            // cont();

            Thenjs.parallel(parallelFn).fin(cont);
        }).then((cont)=> {
            if (dataGrid){
                cb(null, {value: rowResult, done: false});
            }else {
                cb(null, {value: result, done: false});
            }
        }).fail((cont, err)=> {
            cb(err);
        });
    }

};


module.exports = ResultSet;

