const myjdbc=require('../../lib/index');
// const MyUtil=require('../lib/MyUtil');

//设置驱动程序
myjdbc(['../../../../drivers/pgsql.jar']);

(async function run() {

    //创建连接池
    let newPool = new myjdbc.Pool({
        name: 'myjdbc测试库',
        dbms: 'pgsql',//ase,sybase,mysql,pgsql,mssql,asa
        // encrypt:'des-ecb',
        holdTimeout: 10 * 60 * 1000,//单条语句超时时间
        url: process.env.url,//数据库连接字符串
        drivername: "org.postgresql.Driver",
        minpoolsize: 1,//最小连接池
        maxpoolsize: 127,//最大连接数
        properties: { //数据库账号,//数据库口令
            user: process.env.user,
            password: process.env.password
        }
    });

    let conn;
    try {
        //打开连接池
        await newPool.open();

        //获取连接
        conn = await newPool.getConnection();
        conn.setAppInfo({});

        // let rows = await conn.Query("select a=123");
        // console.log('Query',rows);

        let filename = "/home/dbit/test/test2.db";
        let tables = [
            {
                "querySourceDataSql": `select  rpt_fac_nos_pro_sal.sal_prm_amt::numeric(20,4) as sal_prm_amt , rpt_dim_product.pro_id::varchar(30) as pro_id , rpt_fac_nos_pro_sal.cross_reason::varchar(60) as cross_reason , rpt_dim_org.area_nm1::varchar as area_nm1 , rpt_dim_org.area_nm2::varchar as area_nm2 , rpt_fac_nos_pro_sal.opbrd_id::varchar(60) as opbrd_id , rpt_func_getname(rpt_fac_nos_pro_sal.cst_sal_flag::varchar,'cst_sal_flag','rpt_fac_nos_pro_sal')as cst_sal_flag, rpt_fac_nos_pro_sal.sal_amt::numeric(20,4) as sal_amt , rpt_fac_nos_pro_sal.sal_nos_prm_amt::numeric(20,4) as sal_nos_prm_amt , rpt_dim_time.year_name::varchar(60) as year_name , rpt_fac_nos_pro_sal.sal_qty::numeric(20,4) as sal_qty , rpt_dim_time.halfyear_name::varchar(60) as halfyear_name , rpt_dim_time.month_name::varchar(60) as month_name , rpt_dim_time.year_months::varchar(60) as year_months , rpt_dim_time.year_week::varchar(60) as year_week , rpt_dim_time.holidays::varchar(60) as holidays , rpt_func_getname(rpt_fac_nos_pro_sal.new_old_flag_id::varchar,'newold','rpt_fac_nos_pro_sal')as new_old_flag_id, rpt_dim_customer.age_group_nm::varchar(60) as age_group_nm , rpt_fac_nos_pro_sal.sal_amt_for_atv::numeric(20,4) as sal_amt_for_atv , rpt_dim_time.prd_sdate::varchar(60) as prd_sdate , rpt_dim_time.week_name::varchar(60) as week_name , rpt_dim_time.is_weekday::varchar(60) as is_weekday , rpt_dim_time.hldy_ind::varchar(60) as hldy_ind , rpt_dim_customer.year_sal_nm1::varchar(60) as year_sal_nm1 , rpt_dim_customer.year_sal_nm2::varchar(60) as year_sal_nm2 , rpt_dim_customer.member_active_nm::varchar(60) as member_active_nm , rpt_dim_customer.sub_flag_nm::varchar(60) as sub_flag_nm , rpt_dim_customer.vip_in_age_nm::varchar(60) as vip_in_age_nm , rpt_dim_customer.ttl_sal_amt2::numeric(20,4) as ttl_sal_amt2 , rpt_fac_nos_pro_sal.order_id::varchar(60) as order_id , rpt_dim_customer.in_time::varchar(60) as in_time , rpt_dim_customer.in_org_id::varchar(60) as in_org_id , rpt_dim_customer.in_org_nm::varchar(256) as in_org_nm , rpt_dim_customer.guider_id::varchar(128) as guider_id , rpt_func_getname(rpt_dim_customer.activate::varchar,'activate','rpt_dim_customer')as activate, rpt_dim_customer.active_time::varchar(60) as active_time , rpt_dim_customer.exc_org_id::varchar(256) as exc_org_id , rpt_dim_customer.exc_guider_id::varchar(256) as exc_guider_id , rpt_dim_customer.cst_name::varchar(128) as cst_name , rpt_dim_customer.phone::varchar(128) as phone , rpt_fac_nos_pro_sal.sal_qty_for_upt::numeric(20,4) as sal_qty_for_upt , rpt_fac_nos_pro_sal.order_former::varchar(60) as order_former , rpt_dim_customer.phone_attribution::varchar(128) as phone_attribution , rpt_dim_customer.cst_gender_nm::varchar(128) as cst_gender_nm , rpt_dim_customer.email::varchar(128) as email , rpt_dim_customer.birthday::varchar(60) as birthday , rpt_dim_customer.birthdaymonth::varchar(60) as birthdaymonth , rpt_dim_customer.age::varchar(60) as age , rpt_dim_customer.marriage_nm::varchar(128) as marriage_nm , rpt_dim_customer.job::varchar(128) as job , rpt_dim_customer.degree::varchar(128) as degree , rpt_dim_customer.last_buy_org_id::varchar(128)||' '||                             (select org_nm from rpt_dim_org where org_id=rpt_dim_customer.last_buy_org_id::varchar(128)) as last_buy_org_id, rpt_fac_nos_pro_sal.first_nos::varchar(60) as first_nos , rpt_dim_customer.last_buy_time::varchar(60) as last_buy_time , rpt_dim_customer.first_buy_org_id::varchar(60)||' '||                             (select org_nm from rpt_dim_org where org_id=rpt_dim_customer.first_buy_org_id::varchar(60)) as first_buy_org_id, rpt_dim_customer.first_buy_time::varchar(60) as first_buy_time , rpt_dim_customer.pnt_points::numeric(20,2) as pnt_points , rpt_dim_customer.pnt_future_points::numeric(20,2) as pnt_future_points , rpt_dim_customer.pnt_total_points::numeric(20,2) as pnt_total_points , rpt_func_getname(rpt_fac_nos_pro_sal.act_id::varchar,'account','rpt_fac_nos_pro_sal')as act_id, rpt_dim_customer.ww_id::varchar(128) as ww_id , rpt_dim_customer.cst_type_nm::varchar(60) as cst_type_nm , rpt_dim_customer.cst_level_nm::varchar(128) as cst_level_nm , rpt_func_getname(rpt_fac_nos_pro_sal.sld_type::varchar,'order_type','rpt_fac_nos_pro_sal')as sld_type, rpt_dim_customer.cst_chl_nm::varchar(256) as cst_chl_nm , rpt_dim_product.pro_year_season_nm::varchar as pro_year_season_nm , rpt_dim_product.pro_brd_nm::varchar as pro_brd_nm , rpt_dim_product.pro_gender_nm::varchar as pro_gender_nm , rpt_dim_product.pro_cate_nm2::varchar as pro_cate_nm2 , rpt_dim_product.lsg_date::varchar(10) as lsg_date , rpt_dim_product.valids_nm::text as valids_nm , rpt_dim_product.pro_sizetype::varchar(10) as pro_sizetype , rpt_dim_product.pro_nm::text as pro_nm , rpt_dim_product.cost_price::numeric(15,6) as cost_price , rpt_fac_nos_pro_sal.detail_sld_type::varchar(60) as detail_sld_type , rpt_dim_product.prm::numeric(12,2) as prm , rpt_func_getname(rpt_dim_org.online_flag::varchar,'online_flag','rpt_dim_org')as online_flag, rpt_func_getname(rpt_dim_org.closeif::varchar,'closeif','rpt_dim_org')as closeif, rpt_func_getname(rpt_dim_org.kinds1::varchar,'kinds1','rpt_dim_org')as kinds1, rpt_dim_org.open_date::text as open_date , rpt_dim_org.close_date::bpchar as close_date , rpt_dim_org.org_id2::varchar as org_id2 , rpt_dim_org.mk_id::varchar as mk_id , rpt_dim_org.total_square::numeric as total_square , rpt_fac_nos_pro_sal.pro_color_id::varchar(60) as pro_color_id , rpt_dim_org.org_level::varchar as org_level , rpt_dim_org.channel_nm::varchar as channel_nm , rpt_dim_org.org_brd_nm::varchar as org_brd_nm , rpt_dim_org.org_id::varchar as org_id , rpt_dim_org.org_nm::varchar as org_nm , rpt_dim_org.key_flag::varchar as key_flag , rpt_dim_org.org_flag::text as org_flag , rpt_fac_nos_pro_sal.cst_nos::varchar(60) as cst_nos , rpt_dim_customer.cst_id::varchar(60) as cst_id , rpt_fac_nos_pro_sal.valid_cst_nos::varchar(60) as valid_cst_nos , rpt_fac_nos_pro_sal.pro_size_id::varchar(60) as pro_size_id , rpt_fac_nos_pro_sal.valid_order_id::varchar(60) as valid_order_id , rpt_fac_nos_pro_sal.valid_cst_id::varchar(60) as valid_cst_id , rpt_dim_customer.tag12_nm::varchar(60) as tag12_nm , rpt_dim_customer.tag13_nm::varchar(60) as tag13_nm , rpt_dim_customer.tag14_nm::varchar(60) as tag14_nm , rpt_dim_customer.tag20_nm::varchar(60) as tag20_nm , rpt_dim_customer.tag53_nm::varchar(60) as tag53_nm , rpt_dim_customer.tag69_nm::varchar(60) as tag69_nm , rpt_dim_customer.tag70_nm::varchar(60) as tag70_nm , rpt_dim_customer.tag71_nm::varchar(60) as tag71_nm  
                 from rpt_dim_org
                 \t\t\tjoin rpt_fac_nos_pro_sal on rpt_fac_nos_pro_sal.org_id = rpt_dim_org.org_id
                 \t\t\tjoin rpt_dim_time on rpt_dim_time.prd_sdate = rpt_fac_nos_pro_sal.prd_sdate
                 \t\t\tjoin rpt_dim_product on rpt_fac_nos_pro_sal.pro_id  = rpt_dim_product.pro_id
                 \t\t\tjoin rpt_dim_customer on rpt_fac_nos_pro_sal.cst_id = rpt_dim_customer.cst_id and rpt_fac_nos_pro_sal.act_id = rpt_dim_customer.act_id
                 where (rpt_fac_nos_pro_sal.prd_sdate >= '20190301'  or trim('20190301')='')
                 \tand ( not find_in_set_boolean(rpt_fac_nos_pro_sal.sld_type, ''))
                 \tand (rpt_fac_nos_pro_sal.prd_sdate <= '20200331' or trim('20200331')='')
                 \tand (rpt_dim_org.org_id in (select col from rpt_proc_dataset_treefilter('{"tree_type":"org","userid":"470","filter_value":"''"}'::json) col) or trim('')='')
                 \tand (rpt_fac_nos_pro_sal.new_old_flag_id::varchar='' or trim('')='')
                 \tand (find_in_set('', rpt_dim_customer.cst_level_id)>0 or trim('')='')
                 \tand (rpt_dim_customer.act_cst_id in (SELECT col::bigint from rpt_proc_dataset_treefilter('{"tree_type":"cst","userid":"470","filter_value":"''"}'::json) col) or trim('')='')
                 \tand exists  (select 1)
                 \tand exists (select 1)
                     and ((rpt_fac_nos_pro_sal.cst_sal_flag=1 and find_in_set_boolean('vip_sal','vip_sal')) or (rpt_fac_nos_pro_sal.cst_sal_flag<>1 and find_in_set_boolean('novip_sal','vip_sal')) or 'vip_sal' is null or trim('vip_sal')='')`,
                "targetTableName": "rpt_dim_org",
                "targetFields": 'sal_prm_amt,pro_id,cross_reason,area_nm1,area_nm2,opbrd_id,cst_sal_flag,sal_amt,sal_nos_prm_amt,year_name,sal_qty,halfyear_name,month_name,year_months,year_week,holidays,new_old_flag_id,age_group_nm,sal_amt_for_atv,prd_sdate,week_name,is_weekday,hldy_ind,year_sal_nm1,year_sal_nm2,member_active_nm,sub_flag_nm,vip_in_age_nm,ttl_sal_amt2,order_id,in_time,in_org_id,in_org_nm,guider_id,activate,active_time,exc_org_id,exc_guider_id,cst_name,phone,sal_qty_for_upt,order_former,phone_attribution,cst_gender_nm,email,birthday,birthdaymonth,age,marriage_nm,job,degree,last_buy_org_id,first_nos,last_buy_time,first_buy_org_id,first_buy_time,pnt_points,pnt_future_points,pnt_total_points,act_id,ww_id,cst_type_nm,cst_level_nm,sld_type,cst_chl_nm,pro_year_season_nm,pro_brd_nm,pro_gender_nm,pro_cate_nm2,lsg_date,valids_nm,pro_sizetype,pro_nm,cost_price,detail_sld_type,prm,online_flag,closeif,kinds1,open_date,close_date,org_id2,mk_id,total_square,pro_color_id,org_level,channel_nm,org_brd_nm,org_id,org_nm,key_flag,org_flag,cst_nos,cst_id,valid_cst_nos,pro_size_id,valid_order_id,valid_cst_id,tag12_nm,tag13_nm,tag14_nm,tag20_nm,tag53_nm,tag69_nm,tag70_nm,tag71_nm'.split(','),
                "createTargetTableSql": "CREATE TABLE rpt_dim_org (sal_prm_amt NUMERIC(20,4),pro_id CHARACTER VARYING(30),cross_reason CHARACTER VARYING(60),area_nm1 CHARACTER VARYING(120),area_nm2 CHARACTER VARYING(120),opbrd_id CHARACTER VARYING(60),cst_sal_flag CHARACTER VARYING(2147483647),sal_amt NUMERIC(20,4),sal_nos_prm_amt NUMERIC(20,4),year_name CHARACTER VARYING(60),sal_qty NUMERIC(20,4),halfyear_name CHARACTER VARYING(60),month_name CHARACTER VARYING(60),year_months CHARACTER VARYING(60),year_week CHARACTER VARYING(60),holidays CHARACTER VARYING(60),new_old_flag_id CHARACTER VARYING(2147483647),age_group_nm CHARACTER VARYING(60),sal_amt_for_atv NUMERIC(20,4),prd_sdate CHARACTER VARYING(60),week_name CHARACTER VARYING(60),is_weekday CHARACTER VARYING(60),hldy_ind CHARACTER VARYING(60),year_sal_nm1 CHARACTER VARYING(60),year_sal_nm2 CHARACTER VARYING(60),member_active_nm CHARACTER VARYING(60),sub_flag_nm CHARACTER VARYING(60),vip_in_age_nm CHARACTER VARYING(60),ttl_sal_amt2 NUMERIC(20,4),order_id CHARACTER VARYING(60),in_time CHARACTER VARYING(60),in_org_id CHARACTER VARYING(60),in_org_nm CHARACTER VARYING(256),guider_id CHARACTER VARYING(128),activate CHARACTER VARYING(2147483647),active_time CHARACTER VARYING(60),exc_org_id CHARACTER VARYING(256),exc_guider_id CHARACTER VARYING(256),cst_name CHARACTER VARYING(128),phone CHARACTER VARYING(128),sal_qty_for_upt NUMERIC(20,4),order_former CHARACTER VARYING(60),phone_attribution CHARACTER VARYING(128),cst_gender_nm CHARACTER VARYING(128),email CHARACTER VARYING(128),birthday CHARACTER VARYING(60),birthdaymonth CHARACTER VARYING(60),age CHARACTER VARYING(60),marriage_nm CHARACTER VARYING(128),job CHARACTER VARYING(128),degree CHARACTER VARYING(128),last_buy_org_id TEXT,first_nos CHARACTER VARYING(60),last_buy_time CHARACTER VARYING(60),first_buy_org_id TEXT,first_buy_time CHARACTER VARYING(60),pnt_points NUMERIC(20,2),pnt_future_points NUMERIC(20,2),pnt_total_points NUMERIC(20,2),act_id CHARACTER VARYING(2147483647),ww_id CHARACTER VARYING(128),cst_type_nm CHARACTER VARYING(60),cst_level_nm CHARACTER VARYING(128),sld_type CHARACTER VARYING(2147483647),cst_chl_nm CHARACTER VARYING(256),pro_year_season_nm CHARACTER VARYING(2147483647),pro_brd_nm CHARACTER VARYING(2147483647),pro_gender_nm CHARACTER VARYING(2147483647),pro_cate_nm2 CHARACTER VARYING(2147483647),lsg_date CHARACTER VARYING(10),valids_nm TEXT,pro_sizetype CHARACTER VARYING(10),pro_nm TEXT,cost_price NUMERIC(15,6),detail_sld_type CHARACTER VARYING(60),prm NUMERIC(12,2),online_flag CHARACTER VARYING(2147483647),closeif CHARACTER VARYING(2147483647),kinds1 CHARACTER VARYING(2147483647),open_date TEXT,close_date CHARACTER(2147483647),org_id2 CHARACTER VARYING(2147483647),mk_id CHARACTER VARYING(2147483647),total_square NUMERIC(20,2),pro_color_id CHARACTER VARYING(60),org_level CHARACTER VARYING(2147483647),channel_nm CHARACTER VARYING(2147483647),org_brd_nm CHARACTER VARYING(2147483647),org_id CHARACTER VARYING(30),org_nm CHARACTER VARYING(120),key_flag CHARACTER VARYING(2147483647),org_flag TEXT,cst_nos CHARACTER VARYING(60),cst_id CHARACTER VARYING(60),valid_cst_nos CHARACTER VARYING(60),pro_size_id CHARACTER VARYING(60),valid_order_id CHARACTER VARYING(60),valid_cst_id CHARACTER VARYING(60),tag12_nm CHARACTER VARYING(60),tag13_nm CHARACTER VARYING(60),tag14_nm CHARACTER VARYING(60),tag20_nm CHARACTER VARYING(60),tag53_nm CHARACTER VARYING(60),tag69_nm CHARACTER VARYING(60),tag70_nm CHARACTER VARYING(60),tag71_nm CHARACTER VARYING(60))"
            }
        ];

        let result=await conn.exportSqlite({filename: filename,tables});
        console.log('exportSqlite',result)

    } catch (err) {
        console.error(err);
    } finally {
        //归还连接
        if (conn) await newPool.releaseConn(conn);
        //释放连接池
        if (newPool.pool) await newPool.clear();

    }

})();


