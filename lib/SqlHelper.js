/**
 * 已弃用
 */
'use strict';
const _=require('lodash');
let dbms=_.get(global.config,'jdbc.dbms','ase');
const adapter=require('./adapter/'+dbms+'/index');
module.exports=adapter.sqlHelper;

