/**
 * Created by Dbit on 2017/12/5.
 */
const nconf=require('nconf');
const _=require('lodash');
const HOSTNAME=process.env.COMPUTERNAME || process.env.HOSTNAME;
const PID=process.pid;
//hostname,pid,group,(sid,spid,created,borrowed,status,actived)
/**
 *
 * @param myPool {MyPool}
 */
module.exports=function (myPool) {
    const emiter = myPool.emitter;
    const GROUP=myPool["group"];

    emiter.on('create', (sid) => {
        nconf.set(HOSTNAME+':'+PID+':'+GROUP+":"+sid,'');
    });
    emiter.on('created', (conn) => {
        nconf.set(HOSTNAME+':'+PID+':'+GROUP+":"+conn.sid,{
            spid:conn.spid,
            created:conn.created
        });
    });
    emiter.on('destroy', (sid) => {
        nconf.clear(HOSTNAME+':'+PID+':'+GROUP+":"+sid,()=>nconf.clear(HOSTNAME+':'+PID+':'+GROUP+":"+sid));
    });
    emiter.on('acquire', (conn) => {
        nconf.merge(HOSTNAME+':'+PID+':'+GROUP+":"+conn.sid,{
            borrowed:new Date().getTime()
        });
    });
    emiter.on('release', (sid) => {
        nconf.merge(HOSTNAME+':'+PID+':'+GROUP+":"+sid,{
            borrowed:null
        });
    });

    emiter.on('error', console.error);
};