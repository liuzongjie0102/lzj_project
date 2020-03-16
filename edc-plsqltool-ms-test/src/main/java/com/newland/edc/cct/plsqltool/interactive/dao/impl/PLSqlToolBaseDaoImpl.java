package com.newland.edc.cct.plsqltool.interactive.dao.impl;

import com.newland.bd.multidb.autoconfigure.utils.DiffDBUtils;
import com.newland.bd.multidb.autoconfigure.utils.MutilDataSourceUtils;
import com.newland.bd.utils.commons.UUIDUtils;
import com.newland.bd.utils.db.springjdbc.dao.basedao.CommonBaseDao;
import com.newland.bd.utils.db.springjdbc.dao.model.DaoInfo;
import com.newland.bi.util.logger.BaseLogger;
import com.newland.edc.cct.dataasset.common.model.javabean.TreeNode;
import com.newland.edc.cct.dataasset.entity.model.javabean.DataDispBean;
import com.newland.edc.cct.dataasset.entity.model.javabean.TabColBean;
import com.newland.edc.cct.dataasset.entity.model.javabean.TabInfoBean;
import com.newland.edc.cct.dgw.utils.rest.RestFulServiceUtils;
import com.newland.edc.cct.plsqltool.interactive.common.ResAddress;
import com.newland.edc.cct.plsqltool.interactive.dao.PLSqlToolBaseDao;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.*;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.text.SimpleDateFormat;
import java.util.*;

@Repository("com.newland.edc.cct.plsqltool.interactive.dao.impl.PLSqlToolBaseDaoImpl")
public class PLSqlToolBaseDaoImpl implements PLSqlToolBaseDao {

    private static BaseLogger log = (BaseLogger) BaseLogger.getLogger(PLSqlToolBaseDaoImpl.class);

    // 通用dao
    @Resource(name = "com.newland.bd.utils.db.springjdbc.dao.basedao.CommonBaseDao")
    protected CommonBaseDao dao;

    @Resource
    ResAddress resAddress;

    DaoInfo daoInfo = MutilDataSourceUtils.getCurrentDaoInfo();

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


    @Override
    public void insertTemporaryInfo(TemporaryTabInfo temporaryTabInfo) throws Exception{
        StringBuffer sql = new StringBuffer();
        log.info("插入PLS_TEMPORARY_TAB_INFO信息开始");
        if(temporaryTabInfo.getTab_id().equals("")){
            temporaryTabInfo.setTab_id(UUIDUtils.getUUID());
        }else{
            deleteTemporaryTab(temporaryTabInfo);
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        temporaryTabInfo.setCreate_time(sdf.format(new Date()));
        sql.append(" insert into PLS_TEMPORARY_TAB_INFO(TAB_ID,TAB_NAME,RESOURCE_ID,DB_TYPE,CONN_ID,TENANT_ID,USER_ID,CREATE_TIME) ");
        sql.append(" values ");
        sql.append(" ( ");
        sql.append(" :tab_id, ");
        sql.append(" :tab_name, ");
        sql.append(" :resource_id, ");
        sql.append(" :db_type, ");
        sql.append(" :conn_id, ");
        sql.append(" :tenant_id, ");
        sql.append(" :user_id, ");
        sql.append(" :create_time)");
        log.info(sql.toString());
        int count = dao.update(sql.toString(),temporaryTabInfo);
        if(count>0){
            if(!temporaryTabInfo.getColInfos().isEmpty()){
                for (int i=0;i<temporaryTabInfo.getColInfos().size();i++){
                    if(StringUtils.isNotBlank(temporaryTabInfo.getColInfos().get(i).getCol_name())){
                        temporaryTabInfo.getColInfos().get(i).setCol_name(temporaryTabInfo.getColInfos().get(i).getCol_id());
                    }
                    if(StringUtils.isNotBlank(temporaryTabInfo.getColInfos().get(i).getCol_chs_name())){
                        temporaryTabInfo.getColInfos().get(i).setCol_chs_name(temporaryTabInfo.getColInfos().get(i).getCol_id());
                    }
                    StringBuffer sqlcol = new StringBuffer();
                    sqlcol.append(" insert into PLS_TEMPORARY_COL_INFO(TAB_ID,COL_ID,COL_CHS_NAME,COL_NAME,COL_TYPE,COL_LENGTH,COL_PRECISE,IS_KEY,IS_INDEX,IS_PARTITION,CREATE_TIME,ORDER_ID)" );
                    sqlcol.append(" values ");
                    sqlcol.append(" ( ");
                    sqlcol.append(" :tab_id, ");
                    sqlcol.append(" :col_id, ");
                    sqlcol.append(" :col_chs_name, ");
                    sqlcol.append(" :col_name, ");
                    sqlcol.append(" :col_type, ");
                    sqlcol.append(" :col_length, ");
                    sqlcol.append(" :col_precise,");
                    sqlcol.append(" :is_key, ");
                    sqlcol.append(" :is_index, ");
                    sqlcol.append(" :is_partition, ");
                    sqlcol.append(" '"+sdf.format(new Date())+"',");
                    sqlcol.append( i+1 );
                    sqlcol.append(" )");
                    log.info(sqlcol.toString());
                    dao.update(sqlcol.toString(),temporaryTabInfo.getColInfos().get(i));
                }
            }

        }
        log.info("插入PLS_TEMPORARY_TAB_INFO信息结束");
    }

    @Override
    public void insertTemporaryCol(TemporaryColInfo temporaryColInfo)throws Exception{
        StringBuffer sql = new StringBuffer();
        log.info("插入PLS_TEMPORARY_COL_INFO信息开始");
        StringBuffer sqlcol = new StringBuffer();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sqlcol.append(" insert into PLS_TEMPORARY_COL_INFO(TAB_ID,COL_ID,COL_CHS_NAME,COL_NAME,COL_TYPE,COL_LENGTH,COL_PRECISE,IS_KEY,IS_INDEX,IS_PARTITION,CREATE_TIME,ORDER_ID)" );
        sqlcol.append(" values ");
        sqlcol.append(" ( ");
        sqlcol.append(" :tab_id, ");
        sqlcol.append(" :col_id, ");
        sqlcol.append(" :col_chs_name, ");
        sqlcol.append(" :col_name, ");
        sqlcol.append(" :col_type, ");
        sqlcol.append(" :col_length, ");
        sqlcol.append(" :col_precise,");
        sqlcol.append(" :is_key, ");
        sqlcol.append(" :is_index, ");
        sqlcol.append(" :is_partition, ");
        sqlcol.append(" '"+sdf.format(new Date())+"',");
        sqlcol.append(" :order_id");
        sqlcol.append(" )");
        log.info(sqlcol.toString());
        dao.update(sqlcol.toString(),temporaryColInfo);
        log.info("插入PLS_TEMPORARY_COL_INFO信息结束");
    }


    @Override
    public void deleteTemporaryTab(TemporaryTabInfo temporaryTabInfo) throws Exception{
        StringBuffer sql = new StringBuffer();
        log.info("删除PLS_TEMPORARY_TAB_INFO信息开始");
        if(temporaryTabInfo.getTab_id().equals("")){
            log.info("执行删除PLS_TEMPORARY_TAB_INFO,tab_id为空");
            return;
        }
        sql.append("delete from PLS_TEMPORARY_TAB_INFO where tab_id=:tab_id ");
        if(temporaryTabInfo.getTab_name()!=null&&!temporaryTabInfo.getTab_name().equals("")){
            sql.append(" and tab_name=:tab_name ");
        }
        if(temporaryTabInfo.getResource_id()!=null&&!temporaryTabInfo.getResource_id().equals("")){
            sql.append(" and resource_id=:resource_id ");
        }
        if(temporaryTabInfo.getConn_id()!=null&&!temporaryTabInfo.getConn_id().equals("")){
            sql.append(" and conn_id=:conn_id ");
        }
        if(temporaryTabInfo.getUser_id()!=null&&!temporaryTabInfo.getUser_id().equals("")){
            sql.append(" and user_id=:user_id");
        }
        log.info(sql.toString());
        int count = this.dao.update(sql.toString(), temporaryTabInfo);
        if(count>0){
            deleteTemporaryCol(temporaryTabInfo.getTab_id());
        }
        log.info("删除PLS_TEMPORARY_TAB_INFO信息结束");
    }

    @Override
    public void deleteTemporaryCol(String tab_id) throws Exception{
        log.info("删除PLS_TEMPORARY_COL_INFO信息开始");
        TemporaryColInfo temporaryColInfo = new TemporaryColInfo();
        temporaryColInfo.setTab_id(tab_id);
        String sql = "delete from PLS_TEMPORARY_COL_INFO where tab_id=:tab_id";
        log.info(sql);
        dao.update(sql,temporaryColInfo);
        log.info("删除PLS_TEMPORARY_COL_INFO信息结束");
    }

    @Override
    public List<TemporaryTabInfo> selectTemporaryInfo(String tenant_id,String user_id){
        log.info("查询PLS_TEMPORARY_TAB_INFO信息开始");
        StringBuffer sql = new StringBuffer();
        sql.append(" select TAB_ID,TAB_NAME,RESOURCE_ID,CONN_ID,TENANT_ID,USER_ID,CREATE_TIME from PLS_TEMPORARY_TAB_INFO ");
        sql.append(" where 1=1 ");
        if(tenant_id!=null&&!tenant_id.equals("")){
            sql.append(" and tenant_id='"+tenant_id+"' " );
        }
        if(user_id!=null&&!user_id.equals("")){
            sql.append(" and user_id='"+user_id+"' " );
        }
        sql.append(" order by CREATE_TIME desc");
        log.info(sql.toString());
        List<TemporaryTabInfo> tablist = (List<TemporaryTabInfo>)this.dao.query(sql.toString(), TemporaryTabInfo.class);
        if(!tablist.isEmpty()){
            Map<String, TemporaryTabInfo> map = new HashMap<>();
            for (int i=0;i<tablist.size();i++){
                map.put(tablist.get(i).getTab_id(),tablist.get(i));
            }
            StringBuffer sqlcol = new StringBuffer();
            sqlcol.append(" select TAB_ID,COL_ID,COL_CHS_NAME,COL_NAME,COL_TYPE,COL_LENGTH,COL_PRECISE,IS_KEY,IS_INDEX,IS_PARTITION from PLS_TEMPORARY_COL_INFO ");
            sqlcol.append(" where 1=1 ");
            sqlcol.append(" and tab_id in ( select TAB_ID from ("+sql.toString()+") tt )");
            sqlcol.append(" order by ORDER_ID asc ");
            log.info(sqlcol.toString());
            List<TemporaryColInfo> collist = (List<TemporaryColInfo>)this.dao.query(sqlcol.toString(), TemporaryColInfo.class);
            if(!collist.isEmpty()){
                for (int i=0;i<collist.size();i++){
                    if(map.get(collist.get(i).getTab_id())!=null){
                        map.get(collist.get(i).getTab_id()).getColInfos().add(collist.get(i));
                    }
                }
            }
        }else{
            tablist = new ArrayList<>();
        }
        log.info("查询PLS_TEMPORARY_TAB_INFO信息结束");
        return tablist;
    }

    @Override
    public List<TemporaryTabInfo> selectTemporaryInfo(TemporaryTabInfo temporaryTabInfo){
        log.info("查询PLS_TEMPORARY_TAB_INFO信息开始");
        StringBuffer sql = new StringBuffer();
        sql.append(" select TAB_ID,TAB_NAME,RESOURCE_ID,DB_TYPE,CONN_ID,TENANT_ID,USER_ID,CREATE_TIME from PLS_TEMPORARY_TAB_INFO ");
        sql.append(" where 1=1 ");
        if(temporaryTabInfo.getTenant_id()!=null&&!temporaryTabInfo.getTenant_id().equals("")){
            sql.append(" and tenant_id='"+temporaryTabInfo.getTenant_id()+"' " );
        }
        if(temporaryTabInfo.getUser_id()!=null&&!temporaryTabInfo.getUser_id().equals("")){
            sql.append(" and user_id='"+temporaryTabInfo.getUser_id()+"' " );
        }
        if(temporaryTabInfo.getResource_id()!=null&&!temporaryTabInfo.getResource_id().equals("")){
            sql.append(" and resource_id='"+temporaryTabInfo.getResource_id()+"' ");
        }
        if(temporaryTabInfo.getConn_id()!=null&&!temporaryTabInfo.getConn_id().equals("")){
            sql.append(" and conn_id='"+temporaryTabInfo.getConn_id()+"' ");
        }
        if(temporaryTabInfo.getTab_name()!=null&&!temporaryTabInfo.getTab_name().equals("")){
            sql.append(" and ( upper(tab_name)=upper('"+temporaryTabInfo.getTab_name()+"') or lower(tab_name)=lower('"+temporaryTabInfo.getTab_name()+"')) ");
        }
        if(temporaryTabInfo.getDb_type()!=null&&!temporaryTabInfo.getDb_type().equals("")){
            sql.append(" and db_type='"+temporaryTabInfo.getDb_type()+"' ");
        }
        sql.append(" order by CREATE_TIME desc ");
        log.info(sql.toString());
        List<TemporaryTabInfo> tablist = this.dao.query(sql.toString(), TemporaryTabInfo.class);
        if(!tablist.isEmpty()){
            Map<String, TemporaryTabInfo> map = new HashMap<>();
            for (int i=0;i<tablist.size();i++){
                map.put(tablist.get(i).getTab_id(),tablist.get(i));
            }
            StringBuffer sqlcol = new StringBuffer();
            sqlcol.append(" select TAB_ID,COL_ID,COL_CHS_NAME,COL_NAME,COL_TYPE,COL_LENGTH,COL_PRECISE,IS_KEY,IS_INDEX,IS_PARTITION from PLS_TEMPORARY_COL_INFO ");
            sqlcol.append(" where 1=1 ");
            sqlcol.append(" and tab_id in ( select TAB_ID from ("+sql.toString()+") tt )");
            sqlcol.append(" order by ORDER_ID asc ");
            log.info(sqlcol.toString());
            List<TemporaryColInfo> collist = (List<TemporaryColInfo>)this.dao.query(sqlcol.toString(), TemporaryColInfo.class);
            if(!collist.isEmpty()){
                for (int i=0;i<collist.size();i++){
                    if(map.get(collist.get(i).getTab_id())!=null){
                        map.get(collist.get(i).getTab_id()).getColInfos().add(collist.get(i));
                    }
                }
            }
        }else{
            tablist = new ArrayList<>();
        }
        log.info("查询PLS_TEMPORARY_TAB_INFO信息结束");
        return tablist;
    }

    @Override
    public List<TemporaryTabInfo> selectTemporaryInfo(TemporaryReq temporaryReq){
        log.info("查询PLS_TEMPORARY_COL_INFO信息开始");
        StringBuffer sql = new StringBuffer();
        sql.append(" select TAB_ID,TAB_NAME,RESOURCE_ID,DB_TYPE,CONN_ID,TENANT_ID,USER_ID,CREATE_TIME from PLS_TEMPORARY_TAB_INFO ");
        sql.append(" where 1=1 ");
        if(StringUtils.isNotBlank(temporaryReq.getTab_id())){
            sql.append(" and tab_id='"+temporaryReq.getTab_id()+"' ");
        }
        if(StringUtils.isNotBlank(temporaryReq.getTenant_id())){
            sql.append(" and tenant_id='"+temporaryReq.getTenant_id()+"' " );
        }
        if(StringUtils.isNotBlank(temporaryReq.getUser_id())){
            sql.append(" and user_id='"+temporaryReq.getUser_id()+"' ");
        }
        if(StringUtils.isNotBlank(temporaryReq.getResource_id())){
            sql.append(" and resource_id='"+temporaryReq.getResource_id()+"' ");
        }
        if(StringUtils.isNotBlank(temporaryReq.getConn_id())){
            sql.append(" and conn_id='"+temporaryReq.getConn_id()+"' ");
        }
        if(StringUtils.isNotBlank(temporaryReq.getTab_name())){
            sql.append(" and (lower(tab_name) like '%"+temporaryReq.getTab_name().toLowerCase()+"%' or upper(tab_name) like '%"+temporaryReq.getTab_name().toUpperCase()+"%') ");
        }
        if(StringUtils.isNotBlank(temporaryReq.getDb_type())){
            sql.append(" and db_type='"+temporaryReq.getDb_type()+"' ");
        }
        if(StringUtils.isNotBlank(temporaryReq.getBegin_time())){
            sql.append(" and create_time >= '"+temporaryReq.getBegin_time()+"' ");
        }
        if(StringUtils.isNotBlank(temporaryReq.getEnd_time())){
            sql.append(" and create_time <= '"+temporaryReq.getEnd_time()+"' ");
        }
        sql.append(" order by CREATE_TIME desc ");
        if(temporaryReq.getPage()>0&&temporaryReq.getPage_count()>0){
            log.info("执行分页查询");
            String sql_ = "";
            if(getBmt_db_type().equals("ORACLE")){
                sql_ = " select TAB_ID,TAB_NAME,RESOURCE_ID,DB_TYPE,CONN_ID,TENANT_ID,USER_ID,CREATE_TIME from ( select rownum rownum_,t1.* from ("+sql.toString()+") t1) t2 where t2.rownum_>"+(temporaryReq.getPage()-1)*temporaryReq.getPage_count()+" and t2.rownum_<="+temporaryReq.getPage()*temporaryReq.getPage_count();
            }else if(getBmt_db_type().equals("MYSQL")){
                sql_ = " select * from ("+sql.toString()+") t1 limit "+(temporaryReq.getPage()-1)*temporaryReq.getPage_count()+" , "+temporaryReq.getPage_count();
            }
            sql = new StringBuffer();
            sql.append(sql_);
        }
        log.info(sql.toString());
        List<TemporaryTabInfo> tablist = (List<TemporaryTabInfo>)this.dao.query(sql.toString(), TemporaryTabInfo.class);
        if(!tablist.isEmpty()){
            Map<String, TemporaryTabInfo> map = new HashMap<>();
            for (int i=0;i<tablist.size();i++){
                map.put(tablist.get(i).getTab_id(),tablist.get(i));
            }
            StringBuffer sqlcol = new StringBuffer();
            sqlcol.append(" select TAB_ID,COL_ID,COL_CHS_NAME,COL_NAME,COL_TYPE,COL_LENGTH,COL_PRECISE,IS_KEY,IS_INDEX,IS_PARTITION from PLS_TEMPORARY_COL_INFO ");
            sqlcol.append(" where 1=1 ");
            sqlcol.append(" and tab_id in ( select a.TAB_ID from ("+sql.toString()+") a )");
            sqlcol.append(" order by ORDER_ID asc ");
            log.info(sqlcol.toString());
            List<TemporaryColInfo> collist = (List<TemporaryColInfo>)this.dao.query(sqlcol.toString(), TemporaryColInfo.class);
            if(!collist.isEmpty()){
                for (int i=0;i<collist.size();i++){
                    if(map.get(collist.get(i).getTab_id())!=null){
                        map.get(collist.get(i).getTab_id()).getColInfos().add(collist.get(i));
                    }
                }
            }
        }else{
            tablist = new ArrayList<>();
        }
        log.info("查询PLS_TEMPORARY_COL_INFO信息结束");
        return tablist;
    }

    @Override
    public void updateTemporaryTableName(String tenant_id,String user_id,String resource_id,String conn_id,String fromTable,String toTable) throws Exception{
        log.info("修改PLS_TEMPORARY_TAB_INFO信息开始");
        StringBuffer sql = new StringBuffer();
        sql.append(" update PLS_TEMPORARY_TAB_INFO set tab_name='"+toTable+"'" );
        sql.append(" where 1=1 ");
        if(!tenant_id.equals("")){
            sql.append(" and tenant_id = '"+tenant_id+"'" );
        }
        if(!user_id.equals("")){
            sql.append(" and user_id = '"+user_id+"'" );
        }
        if(!resource_id.equals("")){
            sql.append(" and resource_id = '"+resource_id+"'" );
        }
        if(!conn_id.equals("")){
            sql.append(" and conn_id = '"+conn_id+"'" );
        }
        log.info("sql:"+sql.toString());
        this.dao.update(sql.toString(),null);
        log.info("修改PLS_TEMPORARY_TAB_INFO信息结束");
    }


    @Override
    public Integer getTemporaryCount(TemporaryReq temporaryReq){
        log.info("查询PLS_TEMPORARY_TAB_INFO数量开始");
        StringBuffer sql = new StringBuffer();
        sql.append(" select TAB_ID,TAB_NAME,RESOURCE_ID,DB_TYPE,CONN_ID,TENANT_ID,USER_ID,CREATE_TIME from PLS_TEMPORARY_TAB_INFO ");
        sql.append(" where 1=1 ");
        if(temporaryReq.getTenant_id()!=null&&!temporaryReq.getTenant_id().equals("")){
            sql.append(" and tenant_id='"+temporaryReq.getTenant_id()+"'" );
        }
        if(StringUtils.isNotBlank(temporaryReq.getUser_id())){
            sql.append(" and user_id='"+temporaryReq.getUser_id()+"' ");
        }
        if(temporaryReq.getResource_id()!=null&&!temporaryReq.getResource_id().equals("")){
            sql.append(" and resource_id='"+temporaryReq.getResource_id()+"'");
        }
        if(temporaryReq.getConn_id()!=null&&!temporaryReq.getConn_id().equals("")){
            sql.append(" and conn_id='"+temporaryReq.getConn_id()+"'");
        }
        if(temporaryReq.getTab_name()!=null&&!temporaryReq.getTab_name().equals("")){
            sql.append(" and (tab_name like '%"+temporaryReq.getTab_name().toLowerCase()+"%' or tab_name like '%"+temporaryReq.getTab_name().toUpperCase()+"%')");
        }
        if(temporaryReq.getDb_type()!=null&&!temporaryReq.getDb_type().equals("")){
            sql.append(" and db_type='"+temporaryReq.getDb_type()+"'");
        }
        if(StringUtils.isNotBlank(temporaryReq.getBegin_time())){
            sql.append(" and create_time >='"+temporaryReq.getBegin_time()+"' ");
        }
        if(StringUtils.isNotBlank(temporaryReq.getEnd_time())){
            sql.append(" and create_time <='"+temporaryReq.getEnd_time()+"' ");
        }
        log.info(sql.toString());
        Integer num  = (Integer)this.dao.getRecordCount(sql.toString(), null);
        log.info("查询PLS_TEMPORARY_TAB_INFO数量结束");
        if(num==null){
            num = 0;
        }
        return num;
    }

    @Override
    public List<PLSqlToolTable> getTabEntity(String tenant_id,String db_type,String resource_id) throws Exception{
        String userHis = "";
        if(RestFulServiceUtils.getconfig("PLS_USER_HIS").equals("1")){
            userHis = "_his";
        }
        log.info("查询订阅实体信息开始，tennat_id："+tenant_id+" db_type："+db_type);
        StringBuffer sql = new StringBuffer();
        //查询定义实体
        sql.append("SELECT ");
        sql.append("t1.tab_id,t1.tab_chs_name,t1.phy_tab_name,t1.oper_id,t1.tenant_id, ");
        sql.append("t60.resource_id,t60.resource_name,t60.resource_type_id,t6.db_name,t6.user_name,t6.conn_id ");
        sql.append("FROM ST_TAB"+userHis+" t1 ");
        sql.append("INNER JOIN ST_TAB_DISPOSITION"+userHis+" t6 ON t6.tab_id = t1.tab_id ");
        sql.append("INNER JOIN SM2_RESOURCE_CONF t60 ON t6.dispose_type = t60.RESOURCE_ID ");
        sql.append("INNER JOIN V_ST_OBJ_AUTH t11 ON t11.obj_id = t1.tab_id AND t11.tenant_id = '"+tenant_id+"' ");
        sql.append("WHERE 1 = 1 AND (t11.execute_opt = 1) ");
        if(db_type.equals("hive")){
            sql.append("AND t6.db_name is not null ");
        }else {
            sql.append("AND t6.user_name is not null ");
        }
        sql.append("AND t60.resource_type_id = '"+db_type+"' ");
        sql.append("AND t1.tenant_id ='"+tenant_id+"' " );
        sql.append("AND t6.dispose_type ='"+resource_id+"' ");
        sql.append("UNION ");
        //查询订阅实体
        sql.append("SELECT ");
        sql.append("t1.tab_id,t1.tab_chs_name,t1.phy_tab_name,t1.oper_id,t1.tenant_id, ");
        sql.append("t60.resource_id,t60.resource_name,t60.resource_type_id,t6.db_name,t6.user_name,t6.conn_id ");
        sql.append("FROM ST_TAB"+userHis+" t1 ");
        sql.append("INNER JOIN ST_TAB_DISPOSITION"+userHis+" t6 ON t6.tab_id = t1.tab_id ");
        sql.append("INNER JOIN SM2_RESOURCE_CONF t60 ON t6.dispose_type = t60.RESOURCE_ID ");
        sql.append("INNER JOIN V_ST_OBJ_AUTH t11 ON t11.obj_id = t1.tab_id AND t11.tenant_id = '"+tenant_id+"' ");
        sql.append("WHERE 1 = 1 AND (t11.execute_opt = 1) ");
        if(db_type.equals("hive")){
            sql.append("AND t6.db_name is not null ");
        }else {
            sql.append("AND t6.user_name is not null ");
        }
        sql.append("AND t60.resource_type_id = '"+db_type+"' ");
        sql.append("AND t1.tenant_id !='"+tenant_id+"' " );
        log.info("查询实体："+sql.toString());
        List<TabEntityInfo> list = this.dao.query(sql.toString(), TabEntityInfo.class);
        if(list==null){
            return new ArrayList<>();
        }
        Map<String, PLSqlToolTable> map = new HashMap<>();
        List<PLSqlToolTable> tabInfoBeanList = new ArrayList<>();
        for (int i=0;i<list.size();i++){
            PLSqlToolTable tabInfoBean = new PLSqlToolTable();
            tabInfoBean.setTab_id(list.get(i).getTab_id());
            if(list.get(i).getPhy_tab_name()!=null){
                tabInfoBean.setPhy_tab_name(list.get(i).getPhy_tab_name());
            }
            if(list.get(i).getTab_chs_name()!=null){
                tabInfoBean.setTab_chs_name(list.get(i).getTab_chs_name());
            }
            if(list.get(i).getOper_id()!=null){
                tabInfoBean.setOper_id(list.get(i).getOper_id());
            }
            if(list.get(i).getTenant_id()!=null){
                tabInfoBean.setTenant_id(list.get(i).getTenant_id());
            }
            PLSqlToolDataDisp dataDispBean = new PLSqlToolDataDisp();
            dataDispBean.setTab_id(list.get(i).getTab_id());
            dataDispBean.setTenant_id(list.get(i).getTenant_id());
            if(list.get(i).getConn_id()!=null){
                dataDispBean.setConn_id(list.get(i).getConn_id());
            }
            if(list.get(i).getResource_id()!=null){
                dataDispBean.setResource_id(list.get(i).getResource_id());
            }
            if(list.get(i).getResource_type_id()!=null){
                dataDispBean.setResource_type_id(list.get(i).getResource_type_id());
            }
            if(db_type.equals("hive")){
                if(list.get(i).getDb_name()!=null){
                    dataDispBean.setDb_name(list.get(i).getDb_name());
                }
            }else if(db_type.equals("oracle")){
                if(list.get(i).getUser_name()!=null){
                    dataDispBean.setDb_user(list.get(i).getUser_name());
                }
                if(list.get(i).getDb_name()!=null){
                    dataDispBean.setDb_name(list.get(i).getDb_name());
                }
            }else if(db_type.equals("db2")){
                if(list.get(i).getUser_name()!=null){
                    dataDispBean.setDb_user(list.get(i).getUser_name());
                }
                if(list.get(i).getDb_name()!=null){
                    dataDispBean.setDb_name(list.get(i).getDb_name());
                }
            }else if(db_type.equals("greenplum")){
                if(list.get(i).getUser_name()!=null){
                    dataDispBean.setDb_user(list.get(i).getUser_name());
                }
                if(list.get(i).getDb_name()!=null){
                    dataDispBean.setDb_name(list.get(i).getDb_name());
                }
            }else if(db_type.equals("mysql")||db_type.equals("gbase")){
                if(list.get(i).getUser_name()!=null){
                    dataDispBean.setDb_user(list.get(i).getUser_name());
                }
                if(list.get(i).getDb_name()!=null){
                    dataDispBean.setDb_name(list.get(i).getDb_name());
                }
            }
            tabInfoBean.addDisposeInfo(dataDispBean);
            map.put(list.get(i).getTab_id(),tabInfoBean);
            tabInfoBeanList.add(tabInfoBean);
        }
        StringBuffer sql1 = new StringBuffer();
        sql1.append("select tab_id,col_id,col_chs_name,col_name,col_type,col_length,col_precise,(case when is_partition is null or is_partition='' or is_partition!='1' then 0 else 1 end) is_partition,\n" +
                        "(case when is_primary_key is null or is_primary_key='' or is_primary_key!='1' then 0 else 1 end) is_primary_key,\n" +
                        "(case when is_index is null or is_index='' or is_index!='1' then 0 else 1 end) is_index  from st_tab_struct"+userHis+" t  where t.tab_id in ( ");
        sql1.append("select a.tab_id from ( ");
        sql1.append(sql.toString());
        sql1.append(") a ");
        sql1.append(") ");
        sql1.append("order by col_order asc");
        log.info("查询实体字段："+sql1.toString());
        List<PLSqlToolColumn> tabColBeans = this.dao.query(sql1.toString(), PLSqlToolColumn.class);
        if(!tabColBeans.isEmpty()){
            String TAB_VIEW_SWITCH = RestFulServiceUtils.getconfig("TAB_VIEW_SWITCH");
            String TAB_VIEW_RESOURCE_TYPE = RestFulServiceUtils.getconfig("TAB_VIEW_RESOURCE_TYPE");
            if(TAB_VIEW_SWITCH!=null&&TAB_VIEW_SWITCH.equals("1")&&TAB_VIEW_RESOURCE_TYPE!=null&&TAB_VIEW_RESOURCE_TYPE.toUpperCase().indexOf(db_type.toUpperCase())!=-1){
                String sql11 = "select data_range from sm2_tenant where 1=1 and tenant_id = '"+tenant_id+"'";
                log.info("执行订阅表权限控制，sql："+sql11);
                List<String> data_ranges = this.dao.query(sql11,String.class);
                if(data_ranges!=null&&data_ranges.get(0)!=null&&!data_ranges.get(0).equals("")){
                    String data_range = data_ranges.get(0);
                    log.info("data_range:"+data_range);
                    String TAB_VIEW_COL_NAME = RestFulServiceUtils.getconfig("TAB_VIEW_COL_NAME");
                    Map<String,String> tabmap = new HashMap<>();
                    for (int i=0;i<tabColBeans.size();i++){
                        if(map.get(tabColBeans.get(i).getTab_id())!=null){
                            if(map.get(tabColBeans.get(i).getTab_id()).getTab_col_list()==null){
                                map.get(tabColBeans.get(i).getTab_id()).setTab_col_list(new ArrayList<>());
                            }
                            map.get(tabColBeans.get(i).getTab_id()).getTab_col_list().add(tabColBeans.get(i));
                            //订阅权限控制
                            if(!map.get(tabColBeans.get(i).getTab_id()).getTenant_id().equalsIgnoreCase(tenant_id)){
                                if(TAB_VIEW_COL_NAME.toUpperCase().indexOf(tabColBeans.get(i).getCol_id().toUpperCase())!=-1){
                                    if("1".equals(tabColBeans.get(i).getIs_partition())){
                                        log.info("权限控制表："+map.get(tabColBeans.get(i).getTab_id()).getPhy_tab_name());
                                        tabmap.put(tabColBeans.get(i).getTab_id(),tabColBeans.get(i).getTab_id());
                                    }else{
                                        log.info(map.get(tabColBeans.get(i).getTab_id()).getPhy_tab_name()+"."+tabColBeans.get(i).getCol_id()+"不是分区字段，无需添加地市权限");
                                    }
                                }
                            }
                        }
                    }
                    for(String key : tabmap.keySet()){
                        map.get(key).setPhy_tab_name(map.get(key).getPhy_tab_name()+"_"+data_range);
                        log.info("修改后表名："+map.get(key).getPhy_tab_name());
                    }
                }else{
                    log.info("查询结果为空，不做权限控制");
                    for (int i=0;i<tabColBeans.size();i++){
                        if(map.get(tabColBeans.get(i).getTab_id())!=null){
                            if(map.get(tabColBeans.get(i).getTab_id()).getTab_col_list()==null){
                                map.get(tabColBeans.get(i).getTab_id()).setTab_col_list(new ArrayList<>());
                            }
                            map.get(tabColBeans.get(i).getTab_id()).getTab_col_list().add(tabColBeans.get(i));
                        }
                    }
                }
            }else{
                for (int i=0;i<tabColBeans.size();i++){
                    if(map.get(tabColBeans.get(i).getTab_id())!=null){
                        if(map.get(tabColBeans.get(i).getTab_id()).getTab_col_list()==null){
                            map.get(tabColBeans.get(i).getTab_id()).setTab_col_list(new ArrayList<>());
                        }
                        map.get(tabColBeans.get(i).getTab_id()).getTab_col_list().add(tabColBeans.get(i));
                    }
                }
            }
        }
        return tabInfoBeanList;
    }

    @Override
    public List<ExecuteGroupLog> queryExecuteLogs(ExecuteLog executeLog) throws Exception{
        List<ExecuteGroupLog> executeGroupLogs = null;
        log.info("查询执行日志组开始");
        StringBuffer sql = new StringBuffer();
        sql.append(" select group_id,substr(min(create_time),1,19) log_time from PLS_EXECUTE_LOG ");
        sql.append(" where 1=1 ");
        sql.append(" and group_id is not null ");
        if(StringUtils.isNotBlank(executeLog.getUser_id())){
            sql.append(" and user_id = '"+executeLog.getUser_id()+"' " );
        }
        if(StringUtils.isNotBlank(executeLog.getDb_type())){
            sql.append(" and db_type = '"+executeLog.getDb_type()+"' " );
        }
        if(StringUtils.isNotBlank(executeLog.getConn_id())){
            sql.append(" and conn_id = '"+executeLog.getConn_id()+"' ");
        }
        if(StringUtils.isNotBlank(executeLog.getTenant_id())){
            sql.append(" and tenant_id = '"+executeLog.getTenant_id()+"' ");
        }
        if(StringUtils.isNotBlank(executeLog.getRun_mode())){
            sql.append(" and run_mode = '"+executeLog.getRun_mode()+"' ");
        }
        if(StringUtils.isNotBlank(executeLog.getSys_id())){
            sql.append(" and sys_id = '"+executeLog.getSys_id()+"' ");
        }
        if(StringUtils.isNotBlank(executeLog.getStart_time())){
            sql.append(" and create_time >= '"+executeLog.getStart_time()+"' ");
        }
        if(StringUtils.isNotBlank(executeLog.getEnd_time())){
            sql.append(" and create_time < '"+executeLog.getEnd_time()+"' ");
        }
        if(StringUtils.isNotBlank(executeLog.getId())){
            sql.append(" and id ='"+executeLog.getId()+"' ");
        }
        if(StringUtils.isNotBlank(executeLog.getStatus())){
            //全部成功|等待
            if("2".equals(executeLog.getStatus())||"0".equals(executeLog.getStatus())){
                sql.append(" and group_id in (select group_id from ( select group_id,count(1) data_total, sum(case when status="+executeLog.getStatus()+" then 1 else 0 end) data_condition from PLS_EXECUTE_LOG group by group_id  ) t where t.data_total = t.data_condition ) ");
            }
            //失败
            if("-1".equals(executeLog.getStatus())){
                sql.append(" and group_id in (select group_id from ( select group_id,count(1) data_total, sum(case when status=-1 then 1 else 0 end) data_condition from PLS_EXECUTE_LOG group by group_id ) t where t.data_condition>0 ) ");
            }
            //执行中
            if("1".equals(executeLog.getStatus())){
                sql.append(" and group_id in (select group_id from ( select group_id,count(1) data_total, sum(case when status=1 then 1 else 0 end) data_condition1, sum(case when status=-1 then 1 else 0 end) data_condition2 from PLS_EXECUTE_LOG group by group_id ) t where t.data_condition1>0 and t.data_condition2=0 ) ");
            }
        }
        sql.append(" group by group_id ");
        sql.append(" order by log_time desc ");
        log.info("sql:"+sql.toString());
        if(StringUtils.isNotBlank(executeLog.getStart_page())&&StringUtils.isNotBlank(executeLog.getPage_num())){
            executeGroupLogs = this.dao.pageQuery(sql.toString(),null, ExecuteGroupLog.class,Integer.parseInt(executeLog.getStart_page()),Integer.parseInt(executeLog.getPage_num()));
        }else{
            executeGroupLogs = this.dao.query(sql.toString(), ExecuteGroupLog.class);
        }
        if(!executeGroupLogs.isEmpty()){
            Map<String, ExecuteGroupLog> map = new HashMap<>();
            for (int i=0;i<executeGroupLogs.size();i++){
                executeGroupLogs.get(i).setStatus("2");
                executeGroupLogs.get(i).setRum_mode("syn");//先设置成同步数据，后面在改
                map.put(executeGroupLogs.get(i).getGroup_id(),executeGroupLogs.get(i));
            }
            StringBuffer sql1 = new StringBuffer();
            sql1.append(" select t1.id,t1.user_id,t1.tenant_id,t1.resource_id,t1.conn_id,t1.db_type,t1.execute_type, ");
            sql1.append(" t1.keycol,t1.status,t1.utime,t1.create_time,t1.ip,t1.sqlstring,t1.real_sqlstring,t1.group_id,t1.run_mode,t2.result result_info,t3.error_info ");
            sql1.append(" from PLS_EXECUTE_LOG t1  ");
            sql1.append(" left join PLS_EXECUTE_RESULT t2 on t1.id = t2.task_id ");
            sql1.append(" left join PLS_EXECUTE_ERROR_INFO t3 on t1.id = t3.log_id ");
            sql1.append(" where 1=1 ");
            sql1.append(" and group_id in ( ");
            if(StringUtils.isNotBlank(executeLog.getStart_page())&&StringUtils.isNotBlank(executeLog.getPage_num())){
                for (int i=0;i<executeGroupLogs.size();i++){
                    if(i==0){
                        sql1.append("'"+executeGroupLogs.get(i).getGroup_id()+"'");
                    }else{
                        sql1.append(",'"+executeGroupLogs.get(i).getGroup_id()+"'");
                    }
                }
            }else{
                sql1.append(" select a.group_id from ( ");
                sql1.append(sql.toString());
                sql1.append(" ) a ");
            }
            sql1.append(" ) ");
            sql1.append(" order by t1.create_time asc ");
            log.info("sql:"+sql1.toString());
            List<ExecuteLog> executeLogs = this.dao.query(sql1.toString(), ExecuteLog.class);
            if(!executeLogs.isEmpty()){
                for (int i=0;i<executeLogs.size();i++){
                    map.get(executeLogs.get(i).getGroup_id()).getExecuteLogs().add(executeLogs.get(i));
                    if(map.get(executeLogs.get(i).getGroup_id()).getExecute_sqls().equals("")){
                        map.get(executeLogs.get(i).getGroup_id()).setExecute_sqls(executeLogs.get(i).getSqlstring()+";");
                    }else{
                        String sqls  = map.get(executeLogs.get(i).getGroup_id()).getExecute_sqls();
                        map.get(executeLogs.get(i).getGroup_id()).setExecute_sqls(sqls+"\n"+executeLogs.get(i).getSqlstring()+";");
                    }
                    if(!map.get(executeLogs.get(i).getGroup_id()).getRum_mode().equals(executeLogs.get(i).getRun_mode())){
                        map.get(executeLogs.get(i).getGroup_id()).setRum_mode(executeLogs.get(i).getRun_mode());
                    }
                    if(map.get(executeLogs.get(i).getGroup_id()).getStatus().equals("-1")){
                        continue;
                    }else if(executeLogs.get(i).getStatus().equals("-1")){
                        map.get(executeLogs.get(i).getGroup_id()).setStatus(executeLogs.get(i).getStatus());
                        continue;
                    }else if(!executeLogs.get(i).getStatus().equals("-1")){
                        if(executeLogs.get(i).getStatus().equals("2")&&map.get(executeLogs.get(i).getGroup_id()).getStatus().equals("2")){
                            continue;
                        }else if(!executeLogs.get(i).getStatus().equals("2")&&map.get(executeLogs.get(i).getGroup_id()).getStatus().equals("2")){
                            map.get(executeLogs.get(i).getGroup_id()).setStatus(executeLogs.get(i).getStatus());
                            continue;
                        }else if(!executeLogs.get(i).getStatus().equals("2")&&!map.get(executeLogs.get(i).getGroup_id()).getStatus().equals("2")){
                            if(Integer.parseInt(executeLogs.get(i).getStatus())>Integer.parseInt(map.get(executeLogs.get(i).getGroup_id()).getStatus())){
                                map.get(executeLogs.get(i).getGroup_id()).setStatus(executeLogs.get(i).getStatus());
                            }
                        }
                    }
                }
            }
        }else{
            executeGroupLogs = new ArrayList<>();
        }
        log.info("查询执行日志组结束");
        return executeGroupLogs;
    }


    @Override
    public List<ExecuteLog> selectExecuteLogs(ExecuteLog executeLog) throws Exception{
        StringBuffer sql = new StringBuffer();
        log.info("查询执行日志开始");
        sql.append(" SELECT id,user_id,tenant_id,resource_id,conn_id,db_type,execute_type,keycol,sqlstring,real_sqlstring,status,utime,create_time,ip,run_mode FROM PLS_EXECUTE_LOG ");
        sql.append(" WHERE 1=1 ");
        if(executeLog.getId()!=null&&!executeLog.getId().equals("")){
            sql.append(" and id = '"+executeLog.getId()+"' ");
        }
        if(executeLog.getUser_id()!=null&&!executeLog.getUser_id().equals("")){
            sql.append(" and user_id = '"+executeLog.getUser_id()+"' ");
        }
        if(executeLog.getTenant_id()!=null&&!executeLog.getTenant_id().equals("")){
            sql.append(" and tenant_id = '"+executeLog.getTenant_id()+"' ");
        }
        if(executeLog.getResource_id()!=null&&!executeLog.getResource_id().equals("")){
            sql.append(" and resource_id ='"+executeLog.getTenant_id()+"' ");
        }
        if(executeLog.getConn_id()!=null&&!executeLog.getConn_id().equals("")){
            sql.append(" and conn_id ='"+executeLog.getConn_id()+"' ");
        }
        if(executeLog.getDb_type()!=null&&!executeLog.getDb_type().equals("")){
            sql.append(" and db_type ='"+executeLog.getDb_type()+"' ");
        }
        if(executeLog.getKeycol()!=null&&!executeLog.getKeycol().equals("")){
            sql.append(" and keycol ='"+executeLog.getKeycol()+"' ");
        }
        if(executeLog.getExecute_type()!=null&&!executeLog.getExecute_type().equals("")){
            sql.append(" and execute_type ='"+executeLog.getExecute_type()+"' ");
        }
        if(executeLog.getStatus()!=null&&!executeLog.getStatus().equals("")){
            sql.append(" and status ='"+executeLog.getStatus()+"' ");
        }
        if(executeLog.getIp()!=null&&!executeLog.getIp().equals("")){
            sql.append(" and ip = '"+executeLog.getIp()+"'");
        }
        if(executeLog.getRun_mode()!=null&&!executeLog.getRun_mode().equals("")){
            sql.append(" and run_mode = '"+executeLog.getRun_mode()+"' ");
        }
        log.info("sql:"+sql.toString());
        List<ExecuteLog> list = this.dao.query(sql.toString(), ExecuteLog.class);
        if(list==null){
            list = new ArrayList<>();
        }
        log.info("查询执行日志结束");
        return list;
    }

    @Override
    public void insertExecuteLog(ExecuteLog executeLog) throws Exception{
        log.info("插入执行日志开始");
        if(executeLog.getId()!=null&&!executeLog.getId().equals("")){
        }else{
            executeLog.setId(UUIDUtils.getUUID());
        }
        if(executeLog.getCreate_time()!=null&&!executeLog.getCreate_time().equals("")){
        }else{
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            executeLog.setCreate_time(sdf.format(new Date()));
        }
        StringBuffer sql = new StringBuffer();
        sql.append(" insert into PLS_EXECUTE_LOG(group_id,id,user_id,tenant_id,resource_id,conn_id,db_type,execute_type,keycol,sqlstring,real_sqlstring,status,utime,create_time,ip,run_mode,sys_id)");
        sql.append(" values");
        sql.append(" ( ");
        sql.append(" :group_id,:id,:user_id,:tenant_id,:resource_id,:conn_id,:db_type,:execute_type,:keycol,:sqlstring,:real_sqlstring,:status,:utime,:create_time,:ip,:run_mode,:sys_id");
        sql.append(" ) ");
        log.info("sql:"+sql.toString());
        dao.update(sql.toString(),executeLog);
        log.info("插入执行日志结束");
    }

    @Override
    public void updateExecuteLog(ExecuteLog executeLog,String status) throws Exception{
        log.info("修改执行状态开始");
//        if(executeLog.getId()!=null&&!executeLog.getId().equals("")){
//        }else{
//            throw new Exception("无日志ID，无法执行更新操作");
//        }
        StringBuffer sql = new StringBuffer();
        sql.append(" update PLS_EXECUTE_LOG set status = '"+status+"'");
        if(executeLog.getUtime()!=null&&!executeLog.getUtime().equals("")){
            sql.append(" ,utime = '"+executeLog.getUtime()+"' ");
        }
        sql.append(" where 1=1");
        if(executeLog.getId()!=null&&!executeLog.getId().equals("")){
            sql.append(" and id = '"+executeLog.getId()+"' ");
        }
        if(executeLog.getUser_id()!=null&&!executeLog.getUser_id().equals("")){
            sql.append(" and user_id = '"+executeLog.getUser_id()+"' ");
        }
        if(executeLog.getTenant_id()!=null&&!executeLog.getTenant_id().equals("")){
            sql.append(" and tenant_id = '"+executeLog.getTenant_id()+"' ");
        }
        if(executeLog.getResource_id()!=null&&!executeLog.getResource_id().equals("")){
            sql.append(" and resource_id ='"+executeLog.getResource_id()+"' ");
        }
        if(executeLog.getConn_id()!=null&&!executeLog.getConn_id().equals("")){
            sql.append(" and conn_id ='"+executeLog.getConn_id()+"' ");
        }
        if(executeLog.getDb_type()!=null&&!executeLog.getDb_type().equals("")){
            sql.append(" and db_type ='"+executeLog.getDb_type()+"' ");
        }
        if(executeLog.getKeycol()!=null&&!executeLog.getKeycol().equals("")){
            sql.append(" and keycol ='"+executeLog.getKeycol()+"' ");
        }
        if(executeLog.getExecute_type()!=null&&!executeLog.getExecute_type().equals("")){
            sql.append(" and execute_type ='"+executeLog.getExecute_type()+"' ");
        }
        if(executeLog.getIp()!=null&&!executeLog.getIp().equals("")){
            sql.append(" and ip ='"+executeLog.getIp()+"' ");
        }
        if(executeLog.getRun_mode()!=null&&!executeLog.getRun_mode().equals("")){
            sql.append(" and run_mode ='"+executeLog.getRun_mode()+"'");
        }
        if(executeLog.getStatus()!=null&&!executeLog.getStatus().equals("")){
            sql.append(" and status ='"+executeLog.getStatus()+"'");
        }
        log.info("sql:"+sql.toString());
        dao.update(sql.toString(),null);
        log.info("修改执行状态结束");
    }

    @Override
    public void cleanExecuteLog(String time) throws Exception{
        log.info("执行日志清除开始");
        StringBuffer sql = new StringBuffer();
        sql.append(" delete from PLS_EXECUTE_LOG ");
        sql.append(" where 1=1 ");
        sql.append(" and substr(create_time,1,10)<= '"+ time+"'");
        log.info("sql:"+sql.toString());
        this.dao.delete(sql.toString(),null);
        log.info("执行日志清除结束");
    }


    @Override
    public List<ExecuteErrorInfo> selectErrorInfos(ExecuteErrorInfo executeErrorInfo){
        log.info("查询执行错误信息开始");
        StringBuffer sql = new StringBuffer();
        sql.append(" select LOG_ID,ERROR_INFO,CREATE_TIME from PLS_EXECUTE_ERROR_INFO ");
        sql.append(" where 1=1 ");
        if(executeErrorInfo.getLog_id()!=null&&!executeErrorInfo.getLog_id().equals("")){
            sql.append(" and log_id = '"+executeErrorInfo.getLog_id()+"'");
        }
        log.info("sql:"+sql.toString());
        List<ExecuteErrorInfo> executeErrorInfos = this.dao.query(sql.toString(), ExecuteErrorInfo.class);
        if(executeErrorInfos==null){
            executeErrorInfos = new ArrayList<>();
        }
        log.info("查询执行错误信息结束");
        return executeErrorInfos;
    }


    @Override
    public void insertErrorInfo(ExecuteErrorInfo executeErrorInfo) throws Exception{
        log.info("插入执行错误信息开始");
        StringBuffer sql = new StringBuffer();
        if(executeErrorInfo.getCreate_time()!=null&&!executeErrorInfo.getCreate_time().equals("")){
        }else{
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            executeErrorInfo.setCreate_time(sdf.format(new Date()));
        }
        sql.append(" insert into PLS_EXECUTE_ERROR_INFO(LOG_ID,ERROR_INFO,CREATE_TIME,ERROR_STACKTRACE) ");
        sql.append(" values");
        sql.append(" (:log_id,:error_info,:create_time,:error_stacktrace)");
        log.info("sql:"+sql.toString());
        dao.update(sql.toString(),executeErrorInfo);
        log.info("插入执行错误信息结束");
    }


    @Override
    public List<ExecuteResult> selectExecuteResult(String task_id){
        log.info("查询执行结果开始");
        StringBuffer sql = new StringBuffer();
        sql.append(" SELECT task_id,result,create_time from PLS_EXECUTE_RESULT ");
        sql.append(" where 1=1 ");
        sql.append(" and task_id = '"+task_id+"'");
        log.info("sql:"+sql.toString());
        List<ExecuteResult> executeResults = dao.query(sql.toString(), ExecuteResult.class);
        if(executeResults==null){
            executeResults = new ArrayList<>();
        }
        log.info("查询执行结果结束");
        return executeResults;
    }

    @Override
    public void insertExecuteResult(ExecuteResult executeResult) throws Exception{
        log.info("插入执行结果开始");
        StringBuffer sql = new StringBuffer();
        if(executeResult.getCreate_time()!=null&&!executeResult.getCreate_time().equals("")){
        }else{
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            executeResult.setCreate_time(sdf.format(new Date()));
        }
        sql.append(" insert into PLS_EXECUTE_RESULT (task_id,result,create_time) ");
        sql.append(" values(:task_id,:result,:create_time)");
        log.info("sql:"+sql.toString());
        dao.update(sql.toString(),executeResult);
        log.info("插入执行结果结束");
    }

    @Override
    public void cleanExecuteResult(String time) throws Exception{
        log.info("执行删除执行结果开始");
        StringBuffer sql = new StringBuffer();
        sql.append(" delete from PLS_EXECUTE_RESULT ");
        sql.append(" where 1=1 ");
        sql.append(" and substr(create_time,1,10)<= '"+ time+"'");
        log.info("sql:"+sql.toString());
        this.dao.delete(sql.toString(),null);
        log.info("执行删除执行结果结束");
    }

    @Override
    public List<String> selectDataRange(String tenant_id){
        log.info("查询用户权限控制开始");
        StringBuffer sql = new StringBuffer();
        sql.append("select data_range from sm2_tenant where 1=1 and tenant_id = '"+tenant_id+"'");
        log.info("sql:"+sql.toString());
        List<String> data_ranges = this.dao.query(sql.toString(),String.class);
        if(data_ranges==null){
            data_ranges = new ArrayList<>();
        }
        log.info("查询用户权限控制结束");
        return data_ranges;
    }

    @Override
    public List<TabColBean> selectTabEntityCols(String tab_id){
        String userHis = "";
        if(RestFulServiceUtils.getconfig("PLS_USER_HIS").equals("1")){
            userHis = "_his";
        }
        log.info("查询实体字段开始");
        StringBuffer sql = new StringBuffer();
        sql.append("select tab_id,col_id,col_chs_name,col_name,col_type,col_length,col_precise,col_remark,(case when is_partition is null or is_partition='' or is_partition='0' then 0 else 1 end) is_partition,\n" +
                        "(case when is_primary_key is null or is_primary_key=''or is_primary_key='0'  then 0 else 1 end) is_primary_key,\n" +
                        "(case when is_index is null or is_index='' or is_index='0' then 0 else 1 end) is_index  from st_tab_struct"+userHis+" t  where t.tab_id in ( ");
        sql.append("'"+tab_id+"'");
        sql.append(") ");
        sql.append("order by col_order asc");
        log.info("sql:"+sql.toString());
        List<TabColBean> list = this.dao.query(sql.toString(),TabColBean.class);
        if(list==null){
            list = new ArrayList<>();
        }
        log.info("查询实体字段结束");
        return list;
    }

    @Override
    public List<TableOpt> selectAuthority(String user_id){
        log.info("查询用户数据库权限开始");
        StringBuffer sql = new StringBuffer();
        sql.append(" select execute_type,(case when sum(decode(authority_value,1,1,0))>0 then 1 else 0 end) authority_value from PLS_EXECUTE_AUTHORITY ");
        sql.append(" where authority_type in ( ");
        sql.append(" select t1.svc_addr from ( ");
        sql.append(" select * from sm2_priv ");
        sql.append(" where hint like '%交互%' and description like '%权限%' and svc_addr like '%authority%' and priv_type= 2 ");
        sql.append(" ) t1 inner join ( ");
        sql.append(" select * from sm2_role_priv ");
        sql.append(" where role_id in ( ");
        sql.append(" select role_id from sm2_user_role ");
        sql.append(" where user_id ='"+user_id+"' ");
        sql.append(" ) ");
        sql.append(" ) t2 on t1.priv_id = t2.priv_id ");
        sql.append(" ) ");
        sql.append(" group by execute_type ");
        log.info("sql:"+sql.toString());
        List<TableOpt> tableOpts = this.dao.query(sql.toString(), TableOpt.class);
        if(!tableOpts.isEmpty()){
        }else{
            log.info("user_id:"+user_id+"该用户未分配角色权限");
            tableOpts = new ArrayList<>();
        }
        log.info("查询用户数据库权限结束");
        return tableOpts;
    }

    @Override
    public void insertTabInfo(TabInfo tabInfo) throws Exception {
        log.info("插入标签页信息开始");
        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();
        StringBuilder sql = new StringBuilder();

        columns.append("update_time");
        values.append(DiffDBUtils.datetimeToString(DiffDBUtils.sysdate(),"-",":"," "));
        columns.append(" ,tab_id");
        values.append(" ,:tab_id");
        columns.append(" ,tab_name");
        values.append(" ,:tab_name");
        columns.append(" ,user_id");
        values.append(" ,:user_id");
        columns.append(" ,tenant_id");
        values.append(" ,:tenant_id");
        columns.append(" ,resource_id");
        values.append(" ,:resource_id");
        columns.append(" ,conn_id");
        values.append(" ,:conn_id");
        columns.append(" ,sqlstring");
        values.append(" ,:sqlstring");

        sql.append("insert into PLS_TAB_INFO (").append(columns.toString()).append(") ");
        sql.append(" values(").append(values.toString()).append(") ");
        log.info("sql:"+sql.toString());
        dao.update(sql.toString(), tabInfo);
        log.info("插入标签页信息结束");
    }

    @Override
    public void updateTabInfo(TabInfo tabInfo) throws Exception {

        log.info("更新标签页信息开始");
        StringBuilder sb = new StringBuilder();

        sb.append("update PLS_TAB_INFO set ");
        sb.append("update_time = " + DiffDBUtils.datetimeToString(DiffDBUtils.sysdate(),"-",":"," ") +" ");
        if (StringUtils.isNotBlank(tabInfo.getSqlstring())){
            sb.append(" ,sqlstring = :sqlstring");
        }
        if (tabInfo.getTab_name() != null && StringUtils.isNotBlank(tabInfo.getTab_name())){
            sb.append(" ,tab_name = :tab_name");
        }
        if (tabInfo.getUser_id() != null && StringUtils.isNotBlank(tabInfo.getUser_id())){
            sb.append(" ,user_id = :user_id");
        }
        if (tabInfo.getResource_id() != null && StringUtils.isNotBlank(tabInfo.getResource_id())){
            sb.append(" ,resource_id = :resource_id");
        }
        if (tabInfo.getConn_id() != null && StringUtils.isNotBlank(tabInfo.getConn_id())){
            sb.append(" ,conn_id = :conn_id");
        }
        sb.append(" where tab_id = :tab_id");
        log.info("sql:" + sb.toString());
        dao.update(sb.toString(), tabInfo);
        log.info("更新标签页信息结束");
    }

    @Override
    public TabInfo selectTabInfo(TabInfo tabInfo) throws Exception{
        log.info("查询标签页信息开始");
        StringBuilder sb = new StringBuilder();
        sb.append(" select TAB_ID,TAB_NAME,USER_ID,TENANT_ID,RESOURCE_ID,CONN_ID,IS_OPEN,UPDATE_TIME,SQLSTRING from PLS_TAB_INFO ");
        sb.append(" where 1=1");

        if (tabInfo.getTab_id() != null && !tabInfo.getTab_id().equals("")){
            sb.append(" and tab_id = :tab_id");
        }

        log.info("sql:"+sb.toString());
        List<TabInfo> tabInfoList = this.dao.query(sb.toString(), tabInfo, TabInfo.class);
        if(tabInfoList == null){
            throw new Exception("查询无标签页信息");
        }
        log.info("查询标签页信息结束");
        return tabInfoList.get(0);
    }

    @Override
    public List<TabInfo> selectTabInfoListByPage(TabInfoReq tabInfoReq) {
        log.info("查询标签页列表信息开始");
        StringBuilder sb = new StringBuilder();
        sb.append(" select TAB_ID,TAB_NAME,USER_ID,TENANT_ID,RESOURCE_ID,CONN_ID,IS_OPEN,UPDATE_TIME from PLS_TAB_INFO ");
        sb.append(" where 1=1");
        if (tabInfoReq.getUser_id() != null && !tabInfoReq.getUser_id().equals("")){
            sb.append(" and user_id = :user_id");
        }
        if (tabInfoReq.getTenant_id() != null && !tabInfoReq.getTenant_id().equals("")){
            sb.append(" and tenant_id = :tenant_id");
        }
        if (tabInfoReq.getResource_id() != null && !tabInfoReq.getResource_id().equals("")){
            sb.append(" and resource_id = :resource_id");
        }
        if (tabInfoReq.getConn_id() != null && !tabInfoReq.getConn_id().equals("")){
            sb.append(" and conn_id = :conn_id");
        }
        if (tabInfoReq.getTab_name() != null && !tabInfoReq.getTab_name().equals("")){
            sb.append(" and upper(tab_name) like upper('%" + tabInfoReq.getTab_name() + "%')");
        }
        sb.append(" order by update_time desc");

        String sql = "";
        if(tabInfoReq.getPage()>0&&tabInfoReq.getPage_count()>0){
            log.info("执行分页查询");
            log.info(getBmt_db_type());
            if(getBmt_db_type().equals("ORACLE")){
                sql = " select TAB_ID,TAB_NAME,USER_ID,TENANT_ID,RESOURCE_ID,CONN_ID,IS_OPEN,UPDATE_TIME from ( select rownum rownum_,t1.* from ("+sb.toString()+") t1) t2 where t2.rownum_>"+(tabInfoReq.getPage()-1)*tabInfoReq.getPage_count()+" and t2.rownum_<="+tabInfoReq.getPage()*tabInfoReq.getPage_count();
            }else if(getBmt_db_type().equals("MYSQL")){
                sql = sb.toString()+" limit "+(tabInfoReq.getPage()-1)*tabInfoReq.getPage_count()+", "+tabInfoReq.getPage_count();
            }
        }else {
            sql = sb.toString();
        }

        log.info("sql:"+sql);
        List<TabInfo> tabInfoList = this.dao.query(sql, tabInfoReq, TabInfo.class);
        if(tabInfoList ==null){
            tabInfoList = new ArrayList<>();
        }
        log.info("查询标签页列表信息结束");
        return tabInfoList;
    }

    @Override
    public int getTabInfoCount(TabInfoReq tabInfoReq) {
        log.info("查询标签页列表总数开始");
        StringBuilder sb = new StringBuilder();
        sb.append(" select 1 from PLS_TAB_INFO ");
        sb.append(" where 1=1");
        if (tabInfoReq.getUser_id() != null && !tabInfoReq.getUser_id().equals("")){
            sb.append(" and user_id = :user_id");
        }
        if (tabInfoReq.getTenant_id() != null && !tabInfoReq.getTenant_id().equals("")){
            sb.append(" and tenant_id = :tenant_id");
        }
        if (tabInfoReq.getResource_id() != null && !tabInfoReq.getResource_id().equals("")){
            sb.append(" and resource_id = :resource_id");
        }
        if (tabInfoReq.getConn_id() != null && !tabInfoReq.getConn_id().equals("")){
            sb.append(" and conn_id = :conn_id");
        }
        if (tabInfoReq.getTab_name() != null && !tabInfoReq.getTab_name().equals("")){
            sb.append(" and upper(tab_name) like upper('%" + tabInfoReq.getTab_name() + "%')");
        }

        Integer num = this.dao.getRecordCount(sb.toString(),tabInfoReq);
        log.info("查询标签页列表总数结束");
        if(num == null){
            num = 0;
        }
        return num;
    }

    @Override
    public void deleteTabInfo(TabInfo tabInfo) throws Exception {
        log.info("删除标签页信息开始");
        String sql = "delete from PLS_TAB_INFO where tab_id=:tab_id";
        log.info("sql:"+sql);
        dao.update(sql, tabInfo);
        log.info("删除标签页信息结束");
    }

    @Override
    public List<ExecutePoolHeartbeat> selectPoolHeartbeat(ExecutePoolHeartbeat executePoolHeartbeat){
        log.info("查询执行线程池心跳开始");
        StringBuffer sql = new StringBuffer();
        sql.append(" select EXECUTE_ID,DB_TYPE,P_WORKING,CREATE_TIME from PLS_EXECUTEPOOL_HEARTBEAT where 1=1 ");
        if(!executePoolHeartbeat.getExecute_id().equals("")){
            sql.append(" and EXECUTE_ID='"+executePoolHeartbeat.getExecute_id()+"' ");
        }
        if(!executePoolHeartbeat.getDb_type().equals("")){
            sql.append(" and db_type='"+executePoolHeartbeat.getDb_type()+"' ");
        }
        log.info("sql:"+sql.toString());
        List<ExecutePoolHeartbeat> executePoolHeartbeats = this.dao.query(sql.toString(), ExecutePoolHeartbeat.class);
        if(executePoolHeartbeats!=null){

        }else{
            executePoolHeartbeats = new ArrayList<>();
        }
        log.info("查询执行线程池心跳结束");
        return executePoolHeartbeats;
    }

    @Override
    public void insertPoolHeartbeat(ExecutePoolHeartbeat executePoolHeartbeat) throws Exception{
        log.info("插入执行线程池心跳开始");
        if(executePoolHeartbeat.getCreate_time().equals("")){
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            executePoolHeartbeat.setCreate_time(sdf.format(new Date()));
        }
        StringBuffer sql = new StringBuffer();
        sql.append(" insert into PLS_EXECUTEPOOL_HEARTBEAT(EXECUTE_ID,DB_TYPE,P_WORKING,CREATE_TIME) ");
        sql.append(" values");
        sql.append(" (:execute_id,:db_type,:p_working,:create_time)");
        log.info("sql:"+sql.toString());
        this.dao.update(sql.toString(),executePoolHeartbeat);
        log.info("插入执行线程池心跳结束");
    }

    @Override
    public int updatePoolHeartbeat(ExecutePoolHeartbeat executePoolHeartbeat,String heartbeat)throws Exception{
        log.info("更新执行线程池心跳开始");
        StringBuffer sql = new StringBuffer();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sql.append(" update PLS_EXECUTEPOOL_HEARTBEAT set P_WORKING='"+heartbeat+"', CREATE_TIME='"+sdf.format(new Date())+"'");
        sql.append(" where 1=1 ");
        if(!executePoolHeartbeat.getExecute_id().equals("")){
            sql.append(" and execute_id='"+executePoolHeartbeat.getExecute_id()+"' ");
        }
        if(!executePoolHeartbeat.getDb_type().equals("")){
            sql.append(" and db_type='"+executePoolHeartbeat.getDb_type()+"' ");
        }
        log.info("sql:"+sql.toString());
        int num = this.dao.update(sql.toString(),null);
        log.info("更新执行线程池心跳结束");
        return num;
    }

    @Override
    public ExecutePoolHeartbeat queryExecutePool(String db_type){
        log.info("查询集群闲置线程池开始");
        ExecutePoolHeartbeat executePoolHeartbeat = new ExecutePoolHeartbeat();
        StringBuffer sql = new StringBuffer();
        sql.append(" select t1.execute_id,p_working-(case when p_waiting is null or p_waiting ='' then 0 else p_waiting end ) p_working  ");
        sql.append(" from PLS_EXECUTEPOOL_HEARTBEAT t1 ");
        sql.append(" left join ( ");
        sql.append(" select ip,count(distinct group_id) p_waiting from PLS_EXECUTE_LOG ");
        sql.append(" where db_type ='"+db_type+"' and status in ('0','1') and ip is not null and group_id is not null ");
        sql.append(" group by ip ");
        sql.append(" ) t2 on t1.execute_id = t2.ip ");
        sql.append(" where db_type = '"+db_type+"' ");
        Calendar begin = Calendar.getInstance();
        Calendar end = Calendar.getInstance();
        begin.add(Calendar.MINUTE,-5);
        end.add(Calendar.MINUTE,5);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sql.append(" and create_time>='"+sdf.format(begin.getTime())+"' and create_time<= '"+sdf.format(end.getTime())+"' ");
        sql.append(" order by p_working desc ");
        log.info("sql:"+sql.toString());
        List<ExecutePoolHeartbeat> executePoolHeartbeats = this.dao.query(sql.toString(), ExecutePoolHeartbeat.class);
        if(!executePoolHeartbeats.isEmpty()){
            executePoolHeartbeat = executePoolHeartbeats.get(0);
        }else{
            //查不到数据就往本台机器的线程池丢
            executePoolHeartbeat.setExecute_id(RestFulServiceUtils.getconfig("EDC_PLSQLTOOL_IP"));
        }
        log.info("查询集群闲置线程池结束");
        return executePoolHeartbeat;
    }

    @Override
    public TabInfoBean getTabEntityInfo(String tabId) {
        String userHis = "";
        if(RestFulServiceUtils.getconfig("PLS_USER_HIS").equals("1")){
            userHis = "_his";
        }
        log.info("查询实体信息开始，tab_id："+tabId);
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT distinct ");
        sb.append("t1.data_cycle, ");
        sb.append("t1.req_no, ");
        sb.append("(select tenant_name from sm2_tenant t where t.tenant_id = t1.tenant_id ) tenant_name, ");
        sb.append("t3.tab_class_id, ");
        sb.append("(select tab_class_name from st_tab_class"+userHis+" t where t.tab_class_id = t3.tab_class_id) tab_class_name, ");
        sb.append("t3.tab_path_id, ");
        sb.append("t1.tab_type, ");
        sb.append("t4.security_level ");
        sb.append("FROM ST_TAB"+userHis+" t1 ");
        sb.append("LEFT JOIN ST_TAB_DISPOSITION"+userHis+" t6 ON t6.tab_id = t1.tab_id ");
        sb.append("LEFT JOIN ST_TAB_CLASS_PATH_REL"+userHis+" t3 ON t3.tab_id = t1.tab_id ");
        sb.append("LEFT JOIN ST_TAB_REGIST_CONF"+userHis+" t4 ON t4.tab_id = t1.tab_id ");
        sb.append("WHERE t1.tab_id = :tab_id");
        log.info("sql："+sb.toString());
        Map<String, String> parMap = new HashMap<>();
        parMap.put("tab_id",tabId);
        List<TabInfoBean> tabInfoBeanList = this.dao.query(sb.toString(), parMap, TabInfoBean.class);

        if (tabInfoBeanList == null || tabInfoBeanList.isEmpty()){
            log.info("查询实体信息为空");
            return null;
        }
        TabInfoBean tabInfoBean = tabInfoBeanList.get(0);

        //实体的分层分类详细路径
        sb.setLength(0);
        sb.append("select t.tab_path_id as id ,t.tab_path_name as text,t.parent_tab_path_id as parent_id ");
        sb.append(" from st_tab_class_path"+userHis+" t");
        sb.append(" where t.tab_path_id = :tab_path_id");
        log.info("实体的分层分类详细路径SQL："+sb.toString());
        parMap.put("tab_path_id",tabInfoBean.getTab_path_id());
        Map<Integer, String> tabPathNameMap = new HashMap<>();
        int i = 0;
        while (true){
            List<TreeNode> treeNodeList = this.dao.query(sb.toString(), parMap, TreeNode.class);
            if (treeNodeList != null && treeNodeList.size() > 0 && !treeNodeList.get(0).getText().equals("")){
                tabPathNameMap.put(i++, treeNodeList.get(0).getText());
            }else{
                break;
            }

            if (treeNodeList.get(0).getParent_id() == null || treeNodeList.get(0).getParent_id().equals("0") || treeNodeList.get(0).getParent_id().equals("")){
                break;
            }
            parMap.put("tab_path_id",treeNodeList.get(0).getParent_id());
            log.info("id：" + treeNodeList.get(0).getId() + ",text：" + treeNodeList.get(0).getText() + ",Parent_id：" + treeNodeList.get(0).getParent_id());
        }
        tabInfoBean.setTab_class_path_tree(tabInfoBean.getTab_class_name());
        for (int c = tabPathNameMap.size() - 1; c >= 0; c--){
            String tabClassPathTree = tabInfoBean.getTab_class_path_tree();
            tabInfoBean.setTab_class_path_tree(tabClassPathTree + "->" + tabPathNameMap.get(c));
        }

        sb.setLength(0);
        sb.append("select ");
        sb.append("(select conn_name from sm2_conn_manager t where t.conn_id = t6.conn_id) conn_name, ");
        sb.append("(select resource_name from SM2_RESOURCE_CONF where RESOURCE_ID = t6.dispose_type) dispose_desc, ");
        sb.append("t6.user_name db_user, ");
        sb.append("t6.db_name, ");
        sb.append("t6.data_update_cycle, ");
        sb.append("t6.entity_type, ");
        sb.append("(select class_name from SM2_RESOURCE_BUSCLASS where class_id = t6.storage_type) storage_type_name ");
        sb.append("from ST_TAB_DISPOSITION"+userHis+" t6 ");
        sb.append("where tab_id = :tab_id");
        log.info("sql："+sb.toString());
        List<DataDispBean> dispBeanList = this.dao.query(sb.toString(), parMap, DataDispBean.class);
        if (dispBeanList != null && dispBeanList.size() > 0){
            tabInfoBean.addDisposeInfo(dispBeanList.get(0));
        }
        log.info("查询实体信息结束，tab_id："+tabId);
        return tabInfoBean;
    }

    @Override
    public List<ConnectTnsConf> queryConnConf(List<com.newland.edc.cct.dataasset.dispose.model.javabean.DataDispBean> dataDispBeans){
        StringBuffer sql = new StringBuffer();
        sql.append(" select t1.conn_name,t1.conn_id,t2.resource_id ");
        sql.append(" ,(case when t3.param_value is not null and t3.param_value !='' then t3.param_value else t2.para_value end) as tns ");
        sql.append(" from sm2_conn_manager t1 ");
        sql.append(" left join sm2_resource_para t2 on t1.resource_id = t2.resource_id and t2.para_col='tns' ");
        sql.append(" left join sm2_conn_param t3 on t1.conn_id=t3.conn_id and t3.param_name='tns' ");
        sql.append(" where 1=1 ");
        if(!dataDispBeans.isEmpty()){
            String resource_ids = "";
            for (int i=0;i<dataDispBeans.size();i++){
                if(resource_ids.equals("")){
                    resource_ids = "'"+dataDispBeans.get(i).getDispose_type()+"'";
                }else{
                    resource_ids = resource_ids+",'"+dataDispBeans.get(i).getDispose_type()+"'";
                }
            }
            sql.append(" and t1.resource_id in ("+resource_ids+")");
        }
        log.info("sql:"+sql.toString());
        List<ConnectTnsConf> connectTnsConfs = this.dao.query(sql.toString(), ConnectTnsConf.class);
        if(connectTnsConfs.isEmpty()){
            connectTnsConfs = new ArrayList<>();
        }
        return connectTnsConfs;
    }

    @Override
    public String queryExecuteIp(String group_id){
        log.info("查询任务组执行ip开始");
        StringBuffer sql = new StringBuffer();
        sql.append(" select distinct ip from pls_execute_log where group_id = '"+group_id+"'");
        log.info("sql:"+sql.toString());
        List<String> list = this.dao.query(sql.toString(),String.class);
        log.info("查询任务组执行ip结束");
        if(list.isEmpty()){
            return "";
        }else{
            return list.get(0);
        }
    }

    @Override
    public void insertExportLog(ExportLog exportLogBean) throws Exception{
        log.info("插入导出日志表开始");
        StringBuffer sql = new StringBuffer();
        sql.append("insert into pls_export_log(export_id,user_id,tenant_id,resource_id,conn_id,db_type,log_id,execute_type,create_time,update_time,sqlstring,real_sqlstring,status,export_result,ftp_filepath,ftp_filename,download_status) ");
        sql.append("values ");
        sql.append("(:export_id,:user_id,:tenant_id,:resource_id,:conn_id,:db_type,:log_id,:execute_type,:create_time,:update_time,:sqlstring,:real_sqlstring,:status,:export_result,:ftp_filepath,:ftp_filename,:download_status)");
        log.info("执行语句:"+sql.toString());
        this.dao.update(sql.toString(),exportLogBean);
        log.info("插入导出日志表结束");
    }

    @Override
    public List<ExportLog> queryExportLog(ExportLog exportLog){
        log.info("查询导出日志表开始");
        StringBuffer sql = new StringBuffer();
        sql.append("select t1.export_id,t1.user_id,t2.user_name,t1.tenant_id,t1.resource_id,t1.conn_id,t1.db_type,t1.log_id,t1.execute_type,t1.create_time,t1.update_time,t1.sqlstring,t1.real_sqlstring,t1.status,t1.export_result,t1.ftp_filepath,t1.ftp_filename,t1.total_count,t1.download_status from pls_export_log t1 ");
        sql.append("left join sm2_user t2 on t1.user_id=t2.user_id ");
        sql.append("where 1=1 ");
        if(StringUtils.isNotBlank(exportLog.getLog_id())){
            sql.append("and t1.export_id = :export_id ");
        }
        if(StringUtils.isNotBlank(exportLog.getUser_id())){
            sql.append("and t1.user_id = :user_id ");
        }
        if(StringUtils.isNotBlank(exportLog.getDb_type())){
            sql.append("and t1.db_type = :db_type ");
        }
        if(StringUtils.isNotBlank(exportLog.getExport_id())){
            sql.append("and t1.export_id = :export_id ");
        }
        if(StringUtils.isNotBlank(exportLog.getConn_id())){
            sql.append("and t1.conn_id = :conn_id ");
        }
        if(StringUtils.isNotBlank(exportLog.getStatus())){
            sql.append("and t1.status = :status ");
        }else{
            sql.append("and t1.status != '-1' ");
        }
        if(StringUtils.isNotBlank(exportLog.getTenant_id())){
            sql.append("and t1.tenant_id = :tenant_id ");
        }
        if(StringUtils.isNotBlank(exportLog.getResource_id())){
            sql.append("and t1.resource_id = :resource_id ");
        }
        sql.append("order by t1.create_time desc ");
        log.info("执行语句:"+sql.toString());
        List<ExportLog> exportLogBeans = this.dao.query(sql.toString(),exportLog, ExportLog.class);
        if(exportLogBeans.isEmpty()){
            exportLogBeans = new ArrayList<>();
        }
        log.info("查询导出日志表结束");
        return exportLogBeans;
    }

    @Override
    public ExportLogReq queryExportLog(ExportLogReq exportLogBean){
        log.info("查询导出日志表开始-分页");
        StringBuffer sql = new StringBuffer();
        sql.append("select t1.export_id,t1.user_id,t2.user_name,t1.tenant_id,t1.resource_id,t1.conn_id,t1.db_type,t1.log_id,t1.execute_type,t1.create_time,t1.update_time,t1.sqlstring,t1.real_sqlstring,t1.status,t1.export_result,t1.ftp_filepath,t1.ftp_filename,t1.total_count,t1.download_status from pls_export_log t1 ");
        sql.append("left join sm2_user t2 on t1.user_id=t2.user_id ");
        sql.append("where 1=1 ");
        if(StringUtils.isNotBlank(exportLogBean.getUser_id())){
            sql.append("and t1.user_id = :user_id ");
        }
        if(StringUtils.isNotBlank(exportLogBean.getDb_type())){
            sql.append("and t1.db_type = :db_type ");
        }
        if(StringUtils.isNotBlank(exportLogBean.getExport_id())){
            sql.append("and t1.export_id = :export_id ");
        }
        if(StringUtils.isNotBlank(exportLogBean.getConn_id())){
            sql.append("and t1.conn_id = :conn_id ");
        }
        if(StringUtils.isNotBlank(exportLogBean.getStatus())){
            sql.append("and t1.status = :status ");
        }else{
            sql.append("and t1.status != '-1' ");
        }
        if(StringUtils.isNotBlank(exportLogBean.getTenant_id())){
            sql.append("and t1.tenant_id = :tenant_id ");
        }
        if(StringUtils.isNotBlank(exportLogBean.getResource_id())){
            sql.append("and t1.resource_id = :resource_id ");
        }
        if(StringUtils.isNotBlank(exportLogBean.getBeginTime())){
            sql.append("and t1.create_time >= :beginTime ");
        }
        if(StringUtils.isNotBlank(exportLogBean.getEndTime())){
            sql.append("and t1.create_time <= :endTime ");
        }
        sql.append("order by t1.create_time desc ");
        log.info("执行语句:"+sql.toString());
        List<String> count = this.dao.query("select count(1) from ( "+sql.toString()+" ) t ",exportLogBean,String.class);
        if(count.isEmpty()){
            exportLogBean.setAmount("0");
            exportLogBean.setExportLogs(new ArrayList<>());
        }else{
            exportLogBean.setAmount(count.get(0));
            List<ExportLog> exportLogBeans = null;
            if(StringUtils.isNotBlank(exportLogBean.getStartPage())&&StringUtils.isNotBlank(exportLogBean.getPageLimit())){
                exportLogBeans = this.dao.pageQuery(sql.toString(),exportLogBean, ExportLog.class,Integer.parseInt(exportLogBean.getStartPage()),Integer.parseInt(exportLogBean.getPageLimit()));
            }else{
                exportLogBeans = this.dao.query(sql.toString(),exportLogBean, ExportLog.class);
            }
            if(exportLogBeans.isEmpty()){
                exportLogBeans = new ArrayList<>();
            }
            exportLogBean.setExportLogs(exportLogBeans);
        }
        log.info("查询导出日志表结束-分页");
        return exportLogBean;
    }

    @Override
    public List<ExportLog> queryExportLogQuartz(){
        log.info("定时器查询导出日志表开始");
        StringBuffer sql = new StringBuffer();
        sql.append("select export_id,user_id,tenant_id,resource_id,conn_id,db_type,log_id,execute_type,create_time,update_time,sqlstring,real_sqlstring,status,export_result,ftp_filepath,ftp_filename,download_status from pls_export_log ");
        sql.append("where 1=1 ");
        sql.append("and status = 1 ");
        sql.append("and create_time >="+DiffDBUtils.datetimeToString(DiffDBUtils.dateAdd(-2,"DAY"),"-",":"," "));
        log.info("执行语句:"+sql.toString());
        List<ExportLog> exportLogBeans = this.dao.query(sql.toString(), ExportLog.class);
        if(exportLogBeans.isEmpty()){
            exportLogBeans = new ArrayList<>();
        }
        log.info("定时器查询导出日志表结束");
        return exportLogBeans;
    }

    @Override
    public void updateExportLog(String export_id,String status,String export_result,String download_status) throws Exception{
        log.info("更新导出日志表开始");
        StringBuffer sql = new StringBuffer();
        sql.append("update pls_export_log set update_time = "+DiffDBUtils.datetimeToString(DiffDBUtils.sysdate(),"-",":"," ") +" ");
        if(StringUtils.isNotBlank(status)){
            sql.append(",status = '"+status+"' ");
        }
        if(StringUtils.isNotBlank(export_result)){
            sql.append(",export_result = '"+export_result+"' ");
        }
        if(StringUtils.isNotBlank(download_status)){
            sql.append(",download_status = '"+download_status+"' ");
        }
        sql.append("where 1=1 ");
        sql.append("and export_id = '"+export_id+"' ");
        log.info("执行语句:"+sql.toString());
        this.dao.update(sql.toString(),null);
        log.info("更新导出日志表结束");
    }

    @Override
    public void updateExportLog(ExportLog exportLog,String export_id) throws Exception{
        log.info("更新导出日志表开始");
        StringBuffer sql = new StringBuffer();
        sql.append("update pls_export_log set update_time = "+DiffDBUtils.datetimeToString(DiffDBUtils.sysdate(),"-",":"," ") +" ");
        if(StringUtils.isNotBlank(exportLog.getStatus())){
            sql.append(",status = :status ");
        }
        if(StringUtils.isNotBlank(exportLog.getFtp_filename())){
            sql.append(",ftp_filename = :ftp_filename ");
        }
        if(StringUtils.isNotBlank(exportLog.getFtp_filepath())){
            sql.append(",ftp_filepath = :ftp_filepath ");
        }
        if(StringUtils.isNotBlank(exportLog.getTotal_count())){
            sql.append(",total_count = :total_count ");
        }
        if(StringUtils.isNotBlank(exportLog.getLog_id())){
            sql.append(",log_id = :log_id ");
        }
        if(StringUtils.isNotBlank(exportLog.getExport_result())){
            sql.append(",export_result = :export_result ");
        }
        sql.append("where 1=1 ");
        sql.append("and export_id = '"+export_id+"' ");
        this.dao.update(sql.toString(),exportLog);
        log.info("更新导出日志表结束");
    }

    @Override
    public void updateExportLogQuartz() throws Exception{
        log.info("定时器更新导出日志表开始");
        StringBuffer sql = new StringBuffer();
        sql.append("update pls_export_log set update_time = "+DiffDBUtils.datetimeToString(DiffDBUtils.sysdate(),"-",":"," ") +" ");
        sql.append(",status = '0' ");
        sql.append(",download_status = '0' ");
        sql.append(",export_result = '导出执行超时' ");
        sql.append("where 1=1 ");
        sql.append("and (status = '1' or status = '3') ");
        sql.append("and create_time < "+DiffDBUtils.datetimeToString(DiffDBUtils.dateAdd(-2,"DAY"),"-",":"," "));
        log.info("执行语句:"+sql.toString());
        this.dao.update(sql.toString(),null);
        log.info("定时器更新导出日志表结束");
    }

    @Override
    public void deleteExportLog(String export_id) throws Exception{
        log.info("删除导出日志表开始");
        StringBuffer sql = new StringBuffer();
        sql.append("delete from pls_export_log where export_id ='"+export_id+"' ");
        log.info("执行语句:"+sql.toString());
        this.dao.update(sql.toString(),null);
        log.info("删除导出日志表结束");
    }

    @Override
    public List<ExportLog> queryTimeoutExportLog(int time){
        log.info("查询超时导出日志表开始");
        StringBuffer sql = new StringBuffer();
        sql.append("select export_id,user_id,tenant_id,resource_id,conn_id,db_type,log_id,execute_type,create_time,update_time,sqlstring,real_sqlstring,status,export_result,ftp_filepath,ftp_filename,download_status from pls_export_log ");
        sql.append("where 1=1 ");
        sql.append("and download_status != '0' ");
        sql.append("and create_time<"+DiffDBUtils.datetimeToString(DiffDBUtils.dateAdd(-time,"DAY"),"-",":"," "));
        log.info("执行语句:"+sql.toString());
        List<ExportLog> exportLogBeans = this.dao.query(sql.toString(), ExportLog.class);
        if(exportLogBeans.isEmpty()){
            exportLogBeans = new ArrayList<>();
        }
        log.info("查询超时导出日志表结束");
        return exportLogBeans;
    }

    @Override
    public void insertExportDownloadLog(ExportDownloadLog exportDownloadLog) throws Exception{
        log.info("查询导出日志开始");
        exportDownloadLog.setLog_id(UUIDUtils.getUUID());
        StringBuffer sql = new StringBuffer();
        sql.append("insert into pls_export_download_log(log_id,export_id,user_id,tenant_id,resource_id,conn_id,db_type,create_time,status,err_msg) ");
        sql.append("values ");
        sql.append("(:log_id,:export_id,:user_id,:tenant_id,:resource_id,:conn_id,:db_type,"+DiffDBUtils.datetimeToString(DiffDBUtils.sysdate(),"-",":"," ")+",:status,:err_msg)");
        log.info("执行语句;"+sql.toString());
        this.dao.update(sql.toString(),exportDownloadLog);
        log.info("查询导出日志结束");
    }

    @Override
    public List<FtpConfig> queryFtpConfig(FtpConfig ftpConfig){
        log.info("查询ftp配置信息开始");
        StringBuffer sql = new StringBuffer();
        sql.append("select id,ftp_name,ftp_host,ftp_user,ftp_pwd,ftp_port,ftp_type,default_path,file_encoding,create_time from pls_ftp_config ");
        sql.append("where 1=1 ");
        if(StringUtils.isNotBlank(ftpConfig.getId())){
            sql.append("and id='"+ftpConfig.getId()+"' ");
        }
        sql.append("order by create_time ");
        log.info("执行语句;"+sql.toString());
        List<FtpConfig> ftpConfigs = this.dao.query(sql.toString(), FtpConfig.class);
        if(ftpConfigs.isEmpty()){
            ftpConfigs = new ArrayList<>();
        }
        log.info("查询ftp配置信息结束");
        return ftpConfigs;
    }

    @Override
    public void insertFtpSynchronousLog(FtpSynchronousLog ftpSynchronousLog) throws Exception{
        log.info("插入ftp同步日志开始");
        StringBuffer sql = new StringBuffer();
        sql.append("insert into pls_ftp_synchronous_log(log_id,export_id,ftp_config_id,upload_filename,status,create_time,update_time,error_info) ");
        sql.append("values ");
        sql.append("(:log_id,:export_id,:ftp_config_id,:upload_filename,:status,:create_time,:update_time,:error_info)");
        log.info("执行语句;"+sql.toString());
        this.dao.update(sql.toString(),ftpSynchronousLog);
        log.info("插入ftp同步日志结束");
    }

    @Override
    public List<FtpSynchronousLog> queryFtpSynchronousLog(FtpSynchronousLog ftpSynchronousLog){
        log.info("查询ftp同步日志开始");
        StringBuffer sql = new StringBuffer();
        sql.append("select t1.log_id,t1.export_id,t1.ftp_config_id,t1.upload_filename,t1.status,t1.create_time,t1.create_time,t1.update_time,t1.error_info from pls_ftp_synchronous_log t1 ");
        sql.append("left join pls_ftp_config t2 on t1.ftp_config_id = t2.id ");
        sql.append("where 1=1 ");
        if(StringUtils.isNotBlank(ftpSynchronousLog.getExport_id())){
            sql.append("and t1.export_id=:export_id ");
        }
        if(StringUtils.isNotBlank(ftpSynchronousLog.getLog_id())){
            sql.append("and t1.log_id=:log_id ");
        }
        sql.append("order by t1.create_time desc");
        log.info("执行语句;"+sql.toString());
        List<FtpSynchronousLog> ftpSynchronousLogs = this.dao.query(sql.toString(),ftpSynchronousLog, FtpSynchronousLog.class);
        if(ftpSynchronousLogs.isEmpty()){
            ftpSynchronousLogs = new ArrayList<>();
        }
        log.info("查询ftp同步日志结束");
        return ftpSynchronousLogs;
    }

    @Override
    public void updateFtpSynchronousLog(String log_id,String status,String error_info) throws Exception{
        log.info("插入ftp同步日志开始");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        StringBuffer sql = new StringBuffer();
        sql.append("update pls_ftp_synchronous_log set status = '"+status+"' ");
        sql.append(",update_time='"+sdf.format(new Date())+"' ");
        if(StringUtils.isNotBlank(error_info)){
            sql.append(",error_info = '"+error_info+"' ");
        }
        sql.append("where log_id='"+log_id+"' ");
        log.info("执行语句;"+sql.toString());
        this.dao.update(sql.toString(),null);
        log.info("插入ftp同步日志结束");
    }

    @Override
    public String queryConnIdByConnNameEn(String conn_name_en){
        log.info("根据连接英文名开始");
        StringBuffer sql = new StringBuffer();
        sql.append(" select conn_id from sm2_conn_manager t where t.conn_flag='"+conn_name_en+"' ");
        List<String> connIds = this.dao.query(sql.toString(),String.class);
        log.info("根据连接英文名结束");
        if(connIds.isEmpty()){
            return null;
        }else{
            return connIds.get(0);
        }
    }

}
