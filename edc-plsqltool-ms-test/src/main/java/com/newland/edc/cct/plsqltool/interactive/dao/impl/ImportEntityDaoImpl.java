package com.newland.edc.cct.plsqltool.interactive.dao.impl;

import com.alibaba.fastjson.JSON;
import com.newland.bd.multidb.autoconfigure.utils.MutilDataSourceUtils;
import com.newland.bd.utils.commons.UUIDUtils;
import com.newland.bd.utils.db.springjdbc.dao.basedao.CommonBaseDao;
import com.newland.bd.utils.db.springjdbc.dao.model.DaoInfo;
import com.newland.bi.util.common.StringUtil;
import com.newland.bi.util.logger.BaseLogger;
import com.newland.edc.cct.dataasset.entity.model.javabean.DataDispBean;
import com.newland.edc.cct.dataasset.entity.model.javabean.ImporterInfo;
import com.newland.edc.cct.dataasset.entity.model.javabean.LoadDataLog;
import com.newland.edc.cct.dataasset.entity.model.javabean.TabInfoBean;
import com.newland.edc.cct.plsqltool.interactive.dao.ImportEntityDao;
import com.newland.edc.cct.plsqltool.interactive.util.JDBCUtils;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository("com.newland.edc.cct.plsqltool.interactive.dao.impl.ImportEntityDaoImpl")
public class ImportEntityDaoImpl implements ImportEntityDao {
    private static BaseLogger log = (BaseLogger) BaseLogger.getLogger(ImportEntityDaoImpl.class);

    // 通用dao
    @Resource(name = "com.newland.bd.utils.db.springjdbc.dao.basedao.CommonBaseDao")
    protected CommonBaseDao dao;

    private DaoInfo daoInfo = MutilDataSourceUtils.getCurrentDaoInfo();

    public ImportEntityDaoImpl() {

    }

    /**
     * 获取业务信息数据库类型
     *
     * @description
     * @return
     */
    public String getBmt_db_type(){
        DaoInfo.DB_TYPE dbType = daoInfo.getDbType();
        String bmt_db_type = "";  //业务信息数据库类型
        bmt_db_type = dbType.toString();
        if(bmt_db_type!=null&&!bmt_db_type.equals("")){
        }else{
            bmt_db_type = "MYSQL";
        }
        log.info("从jdbc.properties配置文件中取得：业务信息数据库类型为--" + bmt_db_type);
        return bmt_db_type;
    }

    public List<DataDispBean> getDataDispBeans(List<String> resource_ids)
    {
        StringBuffer sb=new StringBuffer();
        List<DataDispBean> dataDispBeans=new ArrayList<DataDispBean>();
        if(resource_ids!=null&&resource_ids.size()>0)
        {
            sb.append("select distinct aa.resource_id,                             ");
            sb.append("                aa.resource_name as dispose_desc,                           ");
            sb.append("                ab.resource_type_id,                        ");
            sb.append("                ab.resource_type_name        ");
            sb.append("  from sm2_resource_conf aa                                 ");
            sb.append(" inner join sm2_resource_type ab                            ");
            sb.append("    on aa.resource_type_id = ab.resource_type_id            ");
            sb.append(" where 1 = 1                                                ");
            sb.append("   and aa.resource_id in(   ");
            for (int i = 0; i < resource_ids.size(); i++) {
                String resource_id=resource_ids.get(i);
                sb.append("   '");
                sb.append(resource_id);
                sb.append("'");
                if(i<resource_ids.size()-1)
                {
                    sb.append(",");
                }
            }
            sb.append(")");
            try


            {
                dataDispBeans= (List<DataDispBean>) this.dao.query( sb.toString(), DataDispBean.class);
                log.info("getDataDispBeans:  "+sb.toString());

            }catch (Exception e) {
                log.info(e.getMessage(),e);
            }

        }
        return dataDispBeans;

    }
    public List<TabInfoBean> getTenantBeans(List<String> resource_ids)
    {
        StringBuffer sb=new StringBuffer();
        List<TabInfoBean> tabInfoBeans=new ArrayList<TabInfoBean>();
        if(resource_ids!=null&&resource_ids.size()>0)
        {
            sb.append("  select aa.tenant_name, aa.tenant_id ");
            sb.append("        from sm2_tenant aa                         ");
            sb.append("             where 1 = 1                       ");
            sb.append("               and aa.tenant_id in (       ");

            for (int i = 0; i < resource_ids.size(); i++) {
                String resource_id=resource_ids.get(i);
                sb.append("   '");
                sb.append(resource_id);
                sb.append("'");
                if(i<resource_ids.size()-1)
                {
                    sb.append(",");
                }
            }
            sb.append(")");
            try


            {
                tabInfoBeans= (List<TabInfoBean>) this.dao.query( sb.toString(), TabInfoBean.class);
                log.info("getDataDispBeans:  "+sb.toString());

            }catch (Exception e) {
                log.info(e.getMessage(),e);
            }

        }
        return tabInfoBeans;

    }
    /*
     * 获得等待导入的日志
     * (non-Javadoc)
     * @see com.newland.edc.cct.dataasset.entity.dao.ImportEntityDao#getUnLoadLog()
     */
    public ImporterInfo getUnLoadLog() throws Exception
    {
        ImporterInfo importerInfo=null;
        StringBuffer sb = new StringBuffer();
        sb.append("select ");
        sb.append("LOAD_SEQ	 as seq_id          ,");
        sb.append("TENANT_ID	         ,");
        sb.append("TAB_TYPE	   as source_way       ,");
        sb.append("LOAD_TYPE	as overwrite_type         ,");
        sb.append("TAB_ID	             ,");
        sb.append("PHY_TAB_NAME        ,");
        sb.append("LOAD_PART	  as   partitionData    ,");
        sb.append("CONN_ID	           ,");
        sb.append("LOAD_STATUS	       ,");
        sb.append("FTP_PATH	     as realFtpPath      ,");
        sb.append("RESOURCE_ID	       ");
        sb.append("from ST_DATALOAD_LOG where 	LOAD_STATUS=       ");
        sb.append( WAIT_LOAD);
        sb.append( " order by LOAD_TIME  ");
        try
        {
            List<ImporterInfo> importerInfos= (List<ImporterInfo>) this.dao.query(JDBCUtils.buildPageQuerySQL(this.getBmt_db_type(), sb.toString(), 1, 1)  , ImporterInfo.class);
            log.info("getUnLoadLog:  "+sb.toString());
            if(importerInfos!=null&&importerInfos.size()>0)
            {
                importerInfo=importerInfos.get(0);
            }
        }catch (Exception e) {
            log.info(e.getMessage(),e);
        }

        return importerInfo;
    }
    /**
     * 正在跑数的数量
     * @return
     * @throws Exception
     */
    public int getUnFinishLoadLogNum() throws Exception
    {
        ImporterInfo importerInfo=new ImporterInfo();
        StringBuffer sb = new StringBuffer();
        sb.append("select ");
        sb.append("LOAD_SEQ	 as seq_id          ,");
        sb.append("TENANT_ID	         ,");
        sb.append("TAB_TYPE	   as source_way       ,");
        sb.append("LOAD_TYPE	as overwrite_type         ,");
        sb.append("TAB_ID	             ,");
        sb.append("PHY_TAB_NAME        ,");
        sb.append("LOAD_PART	  as   partitionData    ,");
        sb.append("CONN_ID	           ,");
        sb.append("LOAD_STATUS	       ,");
        sb.append("FTP_PATH	     as realFtpPath      ,");
        sb.append("RESOURCE_ID	       ");
        sb.append("from ST_DATALOAD_LOG where 	LOAD_STATUS=       ");
        sb.append( LOADING);

        try
        {
            int count=   this.dao.getRecordCount(sb.toString(), importerInfo);
            log.info("getUnFinishLoadLogNum:  "+sb.toString());
            return count;
        }catch (Exception e) {
            log.info(e.getMessage(),e);
        }

        return 0;
    }
    public void updateLoadDataLog(String success_cn,String fail_cn,String load_status,String load_seq)throws Exception
    {
        updateLoadDataLog(success_cn, fail_cn, load_status, load_seq, "");
    }
    public void updateLoadDataLog(String success_cn,String fail_cn,String load_status,String load_seq,String err_msg)throws Exception
    {
        LoadDataLog loadDataLog=new LoadDataLog();

        loadDataLog.setSuccess_cn(success_cn);
        loadDataLog.setFail_cn(fail_cn);
        loadDataLog.setLoad_status(load_status);
        loadDataLog.setLoad_seq(load_seq);
        loadDataLog.setLoad_desc(err_msg);
        StringBuffer sb = new StringBuffer();
        sb.append("update ST_DATALOAD_LOG set SUCCESS_CN=:success_cn,FAIL_CN=:fail_cn,load_status=:load_status,LOAD_DESC=:load_desc where load_seq=:load_seq");
        try
        {
            this.dao.update (sb.toString(), loadDataLog);

            log.info("updateLoadDataLog:  "+sb.toString());
            log.info(JSON.toJSON(loadDataLog));

        }catch (Exception e) {
            log.info(e.getMessage(),e);
        }
    }
    public int getLoadingCount(String load_seq)
    {
        return 0;
    }
    /*
     * 插入日志 等待导入
     * (non-Javadoc)
     * @see com.newland.edc.cct.dataasset.entity.dao.ImportEntityDao#insertLoadDataLog(com.newland.edc.cct.dataasset.entity.model.javabean.ImporterInfo, java.lang.String)
     */
    public String insertLoadDataLog(ImporterInfo importerInfo,String user_id) throws Exception {
        if(!StringUtil.isBlank(importerInfo.getTabInfoBean().getTab_id())){
            importerInfo.setTab_id(importerInfo.getTabInfoBean().getTab_id());
        }
        String load_seq = "";
        load_seq="EI_"+ UUIDUtils.getUUID();
        StringBuffer sb = new StringBuffer();
        sb.append("insert into ST_DATALOAD_LOG (");
        sb.append("LOAD_SEQ	           ,");
        sb.append("TENANT_ID	         ,");
        sb.append("TAB_TYPE	           ,");
        sb.append("LOAD_TYPE	         ,");
        sb.append("TAB_ID	             ,");
        sb.append("PHY_TAB_NAME        ,");
        sb.append("LOAD_PART	         ,");
        sb.append("CONN_ID	           ,");
        sb.append("SUCCESS_CN	         ,");
        sb.append("FAIL_CN	           ,");
        sb.append("LOAD_DESC	         ,");
        sb.append("LOAD_USER	         ,");
        sb.append("LOAD_TIME	         ,");
        sb.append("LOAD_STATUS	       ,");
        sb.append("FTP_PATH	           ,");
        sb.append("RESOURCE_ID	       ");
        sb.append(" 	          )");
        sb.append(" values (");
        sb.append("'"+load_seq+"',");
        sb.append("'"+importerInfo.getTenant_id()+"',");
        sb.append("'"+importerInfo.getSource_way()+"',");
        sb.append("'"+importerInfo.getOverwrite_type()+"',");
        sb.append("'"+importerInfo.getTab_id()+"',");
        sb.append("'"+importerInfo.getPhy_tab_name()+"',");
        sb.append(":partitionData,");
        sb.append("'"+importerInfo.getConn_id()+"',");
        sb.append("0,");
        sb.append("0,");
        sb.append("'',");
        sb.append("'"+user_id+"',");
        if("ORACLE".equals(this.getBmt_db_type().toUpperCase()))
        {
            sb.append("sysdate,");
        }else
        {
            sb.append("now(),");
        }
        sb.append(""+ WAIT_LOAD+",");
        sb.append("'"+importerInfo.getRealFtpPath()+"',");
        sb.append("'"+importerInfo.getResource_id()+"')");
        log.info("insertLoadDataLog:"+sb.toString());
        this.dao.update(sb.toString(), importerInfo);
        return load_seq;
    }
//
//    public Map<String, Object> insertHiveData(ImporterInfo importerInfo, String[] fieldItems, String[] items)
//                    throws Exception {
//        log.info("insertHiveData:       " + JSON.toJSON(importerInfo));
//        log.info("insertHiveData fieldItem:       " + JSON.toJSON(fieldItems));
//        log.info("insertHiveData items:       " + JSON.toJSON(items));
//        if (fieldItems == null) {
//            throw new Exception("缺少导入字段");
//        }
//        if (items == null) {
//            throw new Exception("缺少导入数据");
//        }
//        if (fieldItems.length != items.length) {
//            throw new Exception("字段数据不匹配");
//        }
//
//        Map<String, Object> map = new HashMap<String, Object>();
//        map.put("errmsg", "");
//        String overwrite_type = importerInfo.getOverwrite_type();
//        if (StringUtil.isBlank(overwrite_type)) {
//            overwrite_type = "0";
//        }
//        String tab_name = importerInfo.getPhy_tab_name();
//        String partition = importerInfo.getPartition();
//        StringBuffer sb = new StringBuffer();
//        if ("0".equals(overwrite_type)) {
//            sb.append("insert overwrite table ");
//        } else {
//            sb.append("insert into table ");
//        }
//
//        sb.append(tab_name);
//        if (!StringUtil.isBlank(partition)) {
//            sb.append(" partition ( ");
//            sb.append(partition);
//            sb.append(" ) ");
//        }
//        sb.append("  values(");
//		/*if ("0".equals(overwrite_type)) {
//			sb.append("  values(");
//		}else{
//	 	sb.append(" (");
//		for (int i = 0; i < fieldItems.length; i++) {
//			String fieldItem = fieldItems[i];
//			sb.append(fieldItem);
//			if (i < fieldItems.length - 1) {
//				sb.append(",");
//			}
//		}
//		sb.append(" )values(");
//		}*/
//        for (int i = 0; i < items.length; i++) {
//            String item = items[i];
//            sb.append("'");
//            sb.append(item);
//            sb.append("'");
//            if (i < items.length - 1) {
//                sb.append(",");
//            }
//        }
//        sb.append(" ) ");
//        // mysql_columns+") values(" ;
//        String conn_id = importerInfo.getConn_id();
//        Connection connection = null;
//        Statement stHive = null;
//        ResultSet rsHive = null;
//        boolean xx = false;
//        try {
//            connection = ImportClient.getConnByConnId(conn_id);
//            stHive = connection.createStatement();
//            xx = stHive.execute(sb.toString());
//            log.info("  是否成功：" + xx);
//            log.info("rsHive:" + JSON.toJSON(rsHive));
//        } catch (Exception e) {
//            log.info(e.getMessage(), e);
//            log.info("hive sql:" + sb.toString());
//            map.put("errmsg", sb.toString() + "   :   /n" + e.getMessage());
//        } finally {
//            if (rsHive != null) {
//                try {
//                    rsHive.close();
//                } catch (SQLException e) {
//
//                }
//            }
//            if (stHive != null) {
//                try {
//                    stHive.close();
//                } catch (SQLException e) {
//                    // TODO Auto-generated catch block
//
//                }
//            }
//            if (connection != null) {
//
//                try {
//                    connection.close();
//                } catch (SQLException e) {
//                    // TODO Auto-generated catch block
//
//                }
//            }
//
//        }
//
//        return map;
//    }

    /**
     * @param req_bean
     * @param start_page
     * @param page_count
     * @return
     * @throws Exception
     *             select t.load_seq, t.tenant_id, t2.tenant_name, t.tab_type,
     *             t.load_type, t.tab_id, t.phy_tab_name, t.load_part, t.
     *             conn_id, t.success_cn, t.fail_cn, t.load_desc, t. load_user,
     *             t1.user_name as load_user_name, t3.resource_id,
     *             t3.resource_name, t3.resource_type_id, t4.resource_type_name,
     *             to_char( t. load_time,'yyyy-mm-dd hh:mi:ss')as load_time from
     *             ST_DATALOAD_LOG t inner join sm2_user t1 on
     *             t1.user_id=t.load_user inner join sm2_tenant t2 on
     *             t2.tenant_id=t.tenant_id inner join SM2_RESOURCE_CONF t3 on
     *             t3.conn_id=t.conn_id inner join sm2_resource_type t4 on
     *             t4.resource_type_id=t3.resource_type_id where 1=1 and
     *             t.tenant_id=:tenant_id and t3.resource_id=:resource_id and
     *             t.phy_tab_name like '%%' order by t.load_time desc
     */
    public Map<String, Object> getLoadDataLogList(LoadDataLog req_bean, int start_page, int page_count)
                    throws Exception {
        Map<String, Object> map = new HashMap<String, Object>();
        int total_num = 0;
        List<LoadDataLog> dataLogs = new ArrayList<LoadDataLog>();
        StringBuilder sb = new StringBuilder("");
        sb.append("select t.load_seq,                                                                     ");
        sb.append("       t.tenant_id,                                                                    ");
        sb.append("       t2.tenant_name,                                                                 ");
        sb.append("       t.tab_type,                                                                     ");
        sb.append("       t.load_type,                                                                    ");
        sb.append("       t.tab_id,                                                                       ");
        sb.append("       t.phy_tab_name,                                                                 ");
        sb.append("       t.load_part,                                                                    ");
        sb.append("      t. conn_id,                                                                      ");
        sb.append("       t.success_cn,                                                                   ");
        sb.append("       t.fail_cn,                                                                      ");
        sb.append("       t.load_desc,                                                                    ");
        sb.append("      t. load_user,                                                                    ");
        sb.append("      t1.user_name as load_user_name,                                                  ");
        sb.append("      t3.resource_id,                                                                  ");
        sb.append("      t3.resource_name,                                                                ");
        sb.append("      t3.resource_type_id,                                                             ");
        sb.append("      t4.resource_type_name,t.load_status,                                          ");
        sb.append("     to_char( t. load_time,'yyyy-mm-dd HH24:mi:ss')as load_time                          ");
        sb.append("  from ST_DATALOAD_LOG t                                                               ");
        sb.append("  inner join sm2_user t1 on t1.user_id=t.load_user                                     ");
        sb.append("  inner join sm2_tenant t2 on t2.tenant_id=t.tenant_id                                 ");
        sb.append("   inner join sm2_conn_manager t5 on t.conn_id = t5.conn_id                 ");
        sb.append("            inner join SM2_RESOURCE_CONF t3 on t5.resource_id = t3.resource_id ");

        sb.append("  inner join sm2_resource_type t4 on t4.resource_type_id=t3.resource_type_id           ");
        sb.append("  where 1=1                                                                            ");
        if (req_bean != null && !StringUtil.isBlank(req_bean.getTenant_id())) {
            sb.append("  and t.tenant_id=:tenant_id                                                           ");
        }
        if (req_bean != null && !StringUtil.isBlank(req_bean.getResource_id())) {
            sb.append("  and t3.resource_id=:resource_id                                                      ");
        }
        if (req_bean != null && !StringUtil.isBlank(req_bean.getPhy_tab_name())) {
            sb.append("  and  UPPER(t.phy_tab_name) like UPPER('%" + req_bean.getPhy_tab_name() + "%')                                                         ");
        }
        if (req_bean != null && !StringUtil.isBlank(req_bean.getResource_type_id())) {
            sb.append("  and t3.resource_type_id=:resource_type_id                                                      ");
        }

        sb.append("  order by t.load_time desc                                                            ");
        log.info("getLoadDataLogList:" + sb.toString());
        if (start_page != -1) {
            total_num = this.dao.getRecordCount(sb.toString(), req_bean);
            dataLogs = (List<LoadDataLog>) this.dao.query(
                            JDBCUtils.buildPageQuerySQL(this.getBmt_db_type(), sb.toString(), start_page, page_count),
                            req_bean, LoadDataLog.class);
        } else {
            dataLogs = (List<LoadDataLog>) this.dao.query(sb.toString(), req_bean, LoadDataLog.class);
        }
        map.put("total_num", total_num);
        map.put("data", dataLogs);
        return map;
    }
}
