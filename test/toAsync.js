
const toAsync=require('../lib/MyUtil').toAsync;

let add=function (x,y,cb){
    cb(null,x+y);
}


let add2=toAsync(toAsync(add));
console.log(Object.prototype.toString.call(add2));

console.log(Date.now());
add2(1,2).then((value)=>console.log(Date.now(),'add2->promise',value)).catch(console.error);
add2(1,2,(err,value)=>{
    console.log(Date.now(),'add2->callback',err,value);
})

console.log(Date.now());
add2(1,2).then((value)=>console.log(Date.now(),'add2->promise',value)).catch(console.error);
add2(1,2,(err,value)=>{
    console.log(Date.now(),'add2->callback',err,value);
})

console.log(Date.now());
add2(1,2).then((value)=>console.log(Date.now(),'add2->promise',value)).catch(console.error);
add2(1,2,(err,value)=>{
    console.log(Date.now(),'add2->callback',err,value);
})




