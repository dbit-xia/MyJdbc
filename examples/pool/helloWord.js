let run=require('./dbPool');

run(async (conn)=> {
    conn.setAutoCommit(false);
    console.log(conn.pool.showConnections());
    let rows = await conn.Query("select a=123");
    console.log(rows);
});

run(async (conn)=> {
    conn.setAutoCommit(false);
    console.log(conn.pool.showConnections());
    let rows = await conn.Query("select a=123");
    console.log(rows);
});
