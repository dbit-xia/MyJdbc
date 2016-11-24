/**
 * Created by Dbit on 2016/8/28.
 */
"use strict";
var ResultSet=require('jdbc/lib/resultset.js');
var _ = require('lodash');
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
 * 回调读取resultset
 * @param callback
 */
ResultSet.prototype.toObject = function(callback) {
    //console.debug('My Resultset');
    this.toObjectIter(function(err, rs) {
        if (err) return callback(err);
        var rowIter = rs.rows;
        var rows = [];

        next((err)=>{
            if (err) return callback(err);
            rs.rows = rows;
            return callback(null, rs);
        });

        function next(cb){
            rowIter.next((err,row)=>{
                if (err) return cb(err);
                if (!row.done){
                    rows.push(row.value);
                    return next(cb);
                }
                cb(null);
            });
        }
    });
};


ResultSet.prototype.toObjectIter = function(callback) {
    let self = this;
    let colsmetadata = [];
    let rsmd;
    Thenjs((cont)=>{
        self.getMetaData(cont);
    }).then((cont,rsmd2)=>{
        rsmd=rsmd2;
        rsmd.getColumnCount(cont);
    }).then((cont,colcount)=>{
        let parallelFn = [];
        // Get some column metadata.
        for (let i = 1; i <= colcount; i++) {
            parallelFn.push((cont)=> {
                let parallelFn2 = [];
                parallelFn2.push((cont)=> {
                    rsmd._rsmd.getColumnLabel(i,cont);
                });
                parallelFn2.push((cont)=> {
                    rsmd._rsmd.getColumnType(i,cont);
                });
                Thenjs.parallel(parallelFn2).then((cont, result)=> {
                    colsmetadata[i - 1] = {
                        label: result[0],
                        type: result[1]
                    };
                    cont();
                }).fin(cont);
            });
        }
        Thenjs.parallel(parallelFn).then((cont)=> {
            cont(null, {
                labels: _.map(colsmetadata, 'label'),
                types: _.map(colsmetadata, 'type'),
                rows: {
                    next: function (cb) {
                        let result = {};
                        Thenjs((cont)=> {
                            self._rs.next(cont);
                        }).then((cont, nextRow)=> {
                            if (!nextRow) return cb(null, {done: true});

                            let parallelFn = [];
                            // loop through each column
                            for (let i = 1; i <= colcount; i++) {

                                let cmd = colsmetadata[i - 1];
                                let type = self._types[cmd.type] || 'String';
                                let getter = 'get' + type; //+ 'Sync'

                                parallelFn.push((cont)=> {
                                    self._rs[getter](i, (err, dateVal)=> {
                                        if (err) return cont(err);
                                        switch (type) {
                                            case 'BigDecimal'://nodeJava_java_math_BigDecimal
                                                result[cmd.label] = Number(dateVal);
                                                break;
                                            case 'Date':
                                            case 'Time':
                                            case 'Timestamp':
                                                result[cmd.label] = dateVal && dateVal.toString();
                                                break;
                                            default:
                                                result[cmd.label] = dateVal;
                                                break;
                                        }
                                        cont();
                                    });
                                });
                            }

                            Thenjs.parallel(parallelFn).fin(cont);
                        }).then((cont)=> {
                            cb(null, {value: result, done: false});
                        }).fail((cont, err)=> {
                            cb(err);
                        });
                    }
                }
            });
        }).fin(cont);
    }).fin((cont,err,result)=> {
        callback(err,result);
    });
};

module.exports = ResultSet;

