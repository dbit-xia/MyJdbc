let MyUtil=require('../lib/MyUtil');


MyUtil.eachLimit([1,2,3],async (v)=>{
    return v;
},1).then(v=>console.log(v)).catch(console.error);


MyUtil.eachLimit([1,2,3],(cont,v)=>{
    cont(null,v);
},1,(err,result)=>{
    console.log(err,result);
});