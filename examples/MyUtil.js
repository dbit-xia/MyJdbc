let MyUtil=require('../lib/MyUtil');


MyUtil.eachLimit([1,2,3],async (v)=>{
    return v;
},1).then(v=>console.log(v)).catch(console.error);


MyUtil.eachLimit([1,2,3],(cont,v)=>{
    cont(null,v);
},1,(err,result)=>{
    console.log(err,result);
});

let sqlError={Message:'There is already an object named \'inctable\' in the database.'};
let table = MyUtil.posValue(sqlError.Message, "named '", "'");
if (table) sqlError.Message = '已经存在的表或对象:' + table;
console.log(table,sqlError.Message);