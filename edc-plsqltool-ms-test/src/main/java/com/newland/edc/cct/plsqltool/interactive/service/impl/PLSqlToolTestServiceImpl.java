package com.newland.edc.cct.plsqltool.interactive.service.impl;

import com.alibaba.fastjson.JSON;
import com.newland.bd.ms.core.model.RespInfo;
import com.newland.bd.utils.commons.UUIDUtils;
import com.newland.bd.workflow.sql.api.SqlInspect;
import com.newland.bd.workflow.sql.api.SqlSplit;
import com.newland.bd.workflow.sql.bean.DbType;
import com.newland.bd.workflow.sql.bean.SqlHandleResult;
import com.newland.bd.workflow.sql.bean.TableNameAndOpt;
import com.newland.bd.workflow.sql.bean.consanguinity.TableAndPartitionInfo;
import com.newland.bi.util.logger.BaseLogger;
import com.newland.edc.cct.dgw.utils.rest.RestFulServiceUtils;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.*;
import com.newland.edc.cct.plsqltool.interactive.service.PLSqlToolQueryService;
import com.newland.edc.cct.plsqltool.interactive.service.PLSqlToolTestService;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository("com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolTestServiceImpl")
public class PLSqlToolTestServiceImpl implements PLSqlToolTestService {

    private static BaseLogger log = (BaseLogger) BaseLogger.getLogger(PLSqlToolTestServiceImpl.class);

    @Resource(name ="com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolQueryServiceImpl")
    private PLSqlToolQueryService plSqlToolQueryService;

    public TestGrammarBean testGrammar(TestGrammarBean testGrammarBean,DbType dbType) {
        try {
            Pair<String, List<TableNameAndOpt>> pair = SqlInspect.inspect(testGrammarBean.getSql(), dbType ,true);//允许select *
            SqlHandleResult sqlHandleResult = JSON.parseObject(pair.getLeft(),SqlHandleResult.class);
            if(!sqlHandleResult.getSuccess()){
                TestErrorBean testErrorBean = new TestErrorBean();
                testErrorBean.setErrorMsg(sqlHandleResult.getErrLexerMsg().getSimpleErrMsg());
                testErrorBean.setErrorDetail(sqlHandleResult.getErrLexerMsg().getCompleteErrMsg());
                testErrorBean.setErrorLine((Integer.parseInt(testGrammarBean.getStartLine())+sqlHandleResult.getErrLexerMsg().getErrLine())+"");
                testErrorBean.setErrorStartColumn(sqlHandleResult.getErrLexerMsg().getErrStartColumn()+"");
                testErrorBean.setErrorEndColumn(sqlHandleResult.getErrLexerMsg().getErrStopColumn()+"");
                testGrammarBean.setTestResult("false");
                testGrammarBean.setTestErrorBean(testErrorBean);
                return testGrammarBean;
            }else{
                List<String> sqlList = SqlSplit.getStatementList(testGrammarBean.getSql());
                if(!sqlList.isEmpty()){
                    testGrammarBean.setCount(sqlList.size());
                    testGrammarBean.setTestSqlBeans(new ArrayList<>());
                    for (int i=0;i<sqlList.size();i++){
                        TestSqlBean testSqlBean = new TestSqlBean();
                        testSqlBean.setSql(sqlList.get(i));
                        testSqlBean.setReal_sql(sqlList.get(i));
                        testGrammarBean.getTestSqlBeans().add(testSqlBean);
                    }
                    testGrammarBean.setTestResult("true");
                }else{
                    TestErrorBean testErrorBean = new TestErrorBean();
                    testErrorBean.setErrorMsg("该组语句无法执行拆分");
                    testErrorBean.setErrorDetail("该组语句无法执行拆分");
                    testErrorBean.setErrorLine(testGrammarBean.getStartLine());
                    testGrammarBean.setTestResult("false");
                    testGrammarBean.setTestErrorBean(testErrorBean);
                }
                return testGrammarBean;
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
    }

    @Override
    public void getPartitionCol(String sql , TemporaryTabInfo temporaryTabInfo){
        //先执行校验 后执行分区语句摘取
        Pair<String, List<TableNameAndOpt>> pair = SqlInspect.inspect(sql, DbType.HIVE);
        List<TableAndPartitionInfo> hiveTablePartitionInfoList = SqlInspect.getHiveTablePartitionInfoList();
        if(hiveTablePartitionInfoList==null){
            return;
        }
        for (int i=0;i<hiveTablePartitionInfoList.size();i++){
            TableAndPartitionInfo tableAndPartitionInfo = hiveTablePartitionInfoList.get(i);
            int length = temporaryTabInfo.getColInfos().size();
            for (int m=0;m<tableAndPartitionInfo.partitionInfos.size();m++){
                boolean incol = true;
                for (int n=0;n<length;n++){
                    if(tableAndPartitionInfo.partitionInfos.get(m).columnName.equalsIgnoreCase(temporaryTabInfo.getColInfos().get(n).getCol_id())){
                        temporaryTabInfo.getColInfos().get(n).setIs_partition("1");
                        incol = false;
                        break;
                    }
                }
                if(incol){
                    TemporaryColInfo temporaryColInfo = new TemporaryColInfo();
                    temporaryColInfo.setIs_partition("1");
                    temporaryColInfo.setCol_id(tableAndPartitionInfo.partitionInfos.get(m).columnName);

                }
            }
        }
    }

    @Override
    public String transExecuteType(String execute){
        if(execute.indexOf("CREATE")!=-1){
            execute = "CREATE";
        }
        if(execute.indexOf("ALTER")!=-1){
            execute = "ALTER";
        }
        if(execute.indexOf("DROP")!=-1){
            execute = "DROP";
        }
        if(execute.indexOf("TRUNCATE")!=-1){
            execute = "TRUNCATE";
        }
        if(execute.indexOf("UPDATE")!=-1){
            execute = "UPDATE";
        }
        if(execute.indexOf("INSERT")!=-1){
            execute = "INSERT";
        }
        if(execute.indexOf("DELETE")!=-1){
            execute = "DELETE";
        }
        if(execute.indexOf("SELECT")!=-1){
            execute = "SELECT";
        }
        return execute;
    }

    @Override
    public RespInfo grammarAdapter(TestGrammarBean testGrammarBean){
        RespInfo respInfo = new RespInfo();
        try {
            if(testGrammarBean.getDbType().equalsIgnoreCase("HIVE")){
                testGrammarBean = testGrammar(testGrammarBean,DbType.HIVE);
                respInfo = this.tenantAuthorityVer(testGrammarBean,DbType.HIVE);
            }else if(testGrammarBean.getDbType().equalsIgnoreCase("ORACLE")){
                testGrammarBean = testGrammar(testGrammarBean,DbType.ORACLE);
                respInfo = this.tenantAuthorityVer(testGrammarBean,DbType.ORACLE);
            }else if(testGrammarBean.getDbType().equalsIgnoreCase("MYSQL")){
                testGrammarBean = testGrammar(testGrammarBean,DbType.MYSQL);
                respInfo = this.tenantAuthorityVer(testGrammarBean,DbType.MYSQL);
            }else if(testGrammarBean.getDbType().equalsIgnoreCase("DB2")){
                testGrammarBean = testGrammar(testGrammarBean,DbType.DB2);
                respInfo = this.tenantAuthorityVer(testGrammarBean,DbType.DB2);
            }else if(testGrammarBean.getDbType().equalsIgnoreCase("GREENPLUM")){
                testGrammarBean = testGrammar(testGrammarBean,DbType.GP);
                respInfo = this.tenantAuthorityVer(testGrammarBean,DbType.GP);
            }else if(testGrammarBean.getDbType().equalsIgnoreCase("GBASE")){
                testGrammarBean = testGrammar(testGrammarBean,DbType.GBASE);
                respInfo = this.tenantAuthorityVer(testGrammarBean,DbType.GBASE);
            }else{
                respInfo.setRespResult("0");
                respInfo.setRespErrorCode("0");
                respInfo.setRespErrorDesc("DB:"+testGrammarBean.getDbType()+"无效的数据库类型匹配");
                return respInfo;
            }
            respInfo.setRespData(testGrammarBean);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc(e.getMessage());
        }
        return respInfo;
    }


    public RespInfo tenantAuthorityVer(TestGrammarBean testGrammarBean,DbType dbType) throws Exception{
        RespInfo respInfo = new RespInfo();
        String group_id = UUIDUtils.getUUID();
        //租户权限控制
        if(testGrammarBean.getTestResult().equals("true")){
            PLSqlToolDataAssetEntity dataAssetEntity = plSqlToolQueryService.getTabEntity(testGrammarBean);
            if(dataAssetEntity==null){
                respInfo.setRespResult("0");
                respInfo.setRespErrorCode("0");
                respInfo.setRespErrorDesc("权限校验失败");
                return respInfo;
            }
            String data_range = this.plSqlToolQueryService.selectDataRange(testGrammarBean.getTenant_id());
            boolean is_data_range = false;
            String TAB_VIEW_SWITCH = RestFulServiceUtils.getconfig("TAB_VIEW_SWITCH");
            String TAB_VIEW_RESOURCE_TYPE = RestFulServiceUtils.getconfig("TAB_VIEW_RESOURCE_TYPE");
            String TAB_VIEW_COL_NAME = RestFulServiceUtils.getconfig("TAB_VIEW_COL_NAME");
            //满足当前租户有做权限控制 表视图配置失效 以及数据库类型一致情况
            if((data_range!=null&&!data_range.equals(""))&&(TAB_VIEW_SWITCH==null||TAB_VIEW_SWITCH.equals("0"))){
                String dbtype = dbType.toString();
                if(dbtype.equalsIgnoreCase("GP")){
                    dbtype =  "greenplum";
                }
                if(TAB_VIEW_RESOURCE_TYPE!=null&&TAB_VIEW_RESOURCE_TYPE.toUpperCase().indexOf(dbtype.toUpperCase())!=-1){
                    is_data_range = true;
                }
            }
            for (int i=0;i<testGrammarBean.getTestSqlBeans().size();i++){
                List<PLSqlToolTable> tablist = new ArrayList<>();
                testGrammarBean.getTestSqlBeans().get(i).setGroup_id(group_id);
                testGrammarBean.getTestSqlBeans().get(i).setTask_id(UUIDUtils.getUUID());
                String sql = testGrammarBean.getTestSqlBeans().get(i).getSql();
                Pair<String, List<TableNameAndOpt>> pair = SqlInspect.inspect(sql, dbType ,true);//允许select *
                List<TableNameAndOpt> tableNameAndOptList = pair.getRight();
                testGrammarBean.getTestSqlBeans().get(i).setTestSqlDetailBeans(new ArrayList<>());
                for (TableNameAndOpt tableNameAndOpt: tableNameAndOptList){
                    if(tableNameAndOpt.getTableOpt().name().equalsIgnoreCase("ALTER_RENAME_FROMTABLE")||tableNameAndOpt.getTableOpt().name().equalsIgnoreCase("ALTER_RENAME_TOTABLE")){
                        respInfo.setRespResult("0");
                        respInfo.setRespErrorCode("0");
                        respInfo.setRespErrorDesc(tableNameAndOpt.getTableName()+"，暂不支持RENAME操作");
                        return respInfo;
                    }
                    boolean flag = true;
                    if(flag){
                        for (int m=0;m<dataAssetEntity.getDefinitions().size();m++){
                            if(tableNameAndOpt.getTableName().equalsIgnoreCase(dataAssetEntity.getDefinitions().get(m).getPhy_tab_name())){
                                //定义
                                flag = false;
                                TestSqlDetailBean testSqlDetailBean = new TestSqlDetailBean();
                                if(tableNameAndOpt.getTableOpt().isDDL()){
                                    testSqlDetailBean.setExecuteType("DDL");
                                    testSqlDetailBean.setKeyCol(tableNameAndOpt.getTableOpt().name());
                                    testSqlDetailBean.setTableName(tableNameAndOpt.getTableName());
                                    if(RestFulServiceUtils.getconfig("DE_TABLE_TRUNCATE")!=null&&RestFulServiceUtils.getconfig("DE_TABLE_TRUNCATE").equals("1")){
                                        if(testSqlDetailBean.getKeyCol().equalsIgnoreCase("TRUNCATE")){
                                            testSqlDetailBean.setExecuteType("DML");
                                            testGrammarBean.getTestSqlBeans().get(i).getTestSqlDetailBeans().add(testSqlDetailBean);
                                            continue;
                                        }
                                    }
                                    respInfo.setRespResult("0");
                                    respInfo.setRespErrorCode("0");
                                    respInfo.setRespErrorDesc(tableNameAndOpt.getTableName()+"，您不能对定义的表进行表结构更改数据等DDL操作。");
                                    return respInfo;
                                }else if(tableNameAndOpt.getTableOpt().isDML()){
                                    if(RestFulServiceUtils.getconfig("DE_TABLE_DML")!=null&&RestFulServiceUtils.getconfig("DE_TABLE_DML").equals("1")){
                                        testSqlDetailBean.setExecuteType("DML");
                                        testSqlDetailBean.setKeyCol(tableNameAndOpt.getTableOpt().name());
                                        testSqlDetailBean.setTableName(tableNameAndOpt.getTableName());
                                        if(RestFulServiceUtils.getconfig("DE_TABLE_TRUNCATE")==null||!RestFulServiceUtils.getconfig("DE_TABLE_TRUNCATE").equals("1")){
                                            if(testSqlDetailBean.getKeyCol().equalsIgnoreCase("TRUNCATE")){
                                                respInfo.setRespResult("0");
                                                respInfo.setRespErrorCode("0");
                                                respInfo.setRespErrorDesc(tableNameAndOpt.getTableName()+"，定义实体TRUNCATE操作未开启，无法执行");
                                                return respInfo;
                                            }
                                        }
                                    }else{
                                        respInfo.setRespResult("0");
                                        respInfo.setRespErrorCode("0");
                                        respInfo.setRespErrorDesc(tableNameAndOpt.getTableName()+"，您不能对定义的表进行表结构更改、更新数据等DDL、DML操作。");
                                        return respInfo;
                                    }
                                }else if(tableNameAndOpt.getTableOpt().isDQL()){
                                    testSqlDetailBean.setExecuteType("DQL");
                                    testSqlDetailBean.setKeyCol(tableNameAndOpt.getTableOpt().name());
                                    testSqlDetailBean.setTableName(tableNameAndOpt.getTableName());
                                }else {
                                    respInfo.setRespResult("0");
                                    respInfo.setRespErrorCode("0");
                                    respInfo.setRespErrorDesc("无法执行DDL DML DQL 以外的操作");
                                    return respInfo;
                                }
                                testGrammarBean.getTestSqlBeans().get(i).getTestSqlDetailBeans().add(testSqlDetailBean);
                            }
                        }
                    }
                    if(flag){
                        for (int n=0;n<dataAssetEntity.getSubscriptions().size();n++){
                            if(tableNameAndOpt.getTableName().equalsIgnoreCase(dataAssetEntity.getSubscriptions().get(n).getPhy_tab_name())){
                                //订阅
                                dataAssetEntity.getSubscriptions().get(n).setPhy_tab_name(tableNameAndOpt.getTableName());
                                tablist.add(dataAssetEntity.getSubscriptions().get(n));
                                flag = false;
                                TestSqlDetailBean testSqlDetailBean = new TestSqlDetailBean();
                                if(tableNameAndOpt.getTableOpt().isDDL()){
                                    testSqlDetailBean.setExecuteType("DDL");
                                    testSqlDetailBean.setKeyCol(tableNameAndOpt.getTableOpt().name());
                                    respInfo.setRespResult("0");
                                    respInfo.setRespErrorCode("0");
                                    respInfo.setRespErrorDesc(tableNameAndOpt.getTableName()+"，您不能对订阅的表进行表结构更改、更新数据等DDL、DML操作。");
                                    return respInfo;
                                }else if(tableNameAndOpt.getTableOpt().isDML()){
                                    testSqlDetailBean.setExecuteType("DML");
                                    testSqlDetailBean.setTableName(tableNameAndOpt.getTableName());
                                    testSqlDetailBean.setKeyCol(tableNameAndOpt.getTableOpt().name());
                                    respInfo.setRespResult("0");
                                    respInfo.setRespErrorCode("0");
                                    respInfo.setRespErrorDesc(tableNameAndOpt.getTableName()+"，您不能对订阅的表进行表结构更改、更新数据等DDL、DML操作。");
                                    return respInfo;
                                }else if(tableNameAndOpt.getTableOpt().isDQL()){
                                    testSqlDetailBean.setExecuteType("DQL");
                                    testSqlDetailBean.setTableName(tableNameAndOpt.getTableName());
                                    testSqlDetailBean.setKeyCol(tableNameAndOpt.getTableOpt().name());
                                }else {
                                    respInfo.setRespResult("0");
                                    respInfo.setRespErrorCode("0");
                                    respInfo.setRespErrorDesc("无法执行除DQL以外的操作");
                                    return respInfo;
                                }
                                testGrammarBean.getTestSqlBeans().get(i).getTestSqlDetailBeans().add(testSqlDetailBean);
                            }
                        }
                    }
                    if(flag){
                        for (int k=0;k<dataAssetEntity.getTemporarytabs().size();k++){
                            if(tableNameAndOpt.getTableName().equalsIgnoreCase(dataAssetEntity.getTemporarytabs().get(k).getPhy_tab_name())){
                                //临时表信息
                                flag = false;
                                TestSqlDetailBean testSqlDetailBean = new TestSqlDetailBean();
                                if(tableNameAndOpt.getTableOpt().isDDL()){
                                    if(tableNameAndOpt.getTableOpt().name().equalsIgnoreCase("CREATE_TABLE")){
                                        respInfo.setRespResult("0");
                                        respInfo.setRespErrorCode("0");
                                        respInfo.setRespErrorDesc("无法对临时表执行重复建表操作");
                                        return respInfo;
                                    }
                                    testSqlDetailBean.setExecuteType("DDL");
                                    testSqlDetailBean.setKeyCol(tableNameAndOpt.getTableOpt().name());
                                    testSqlDetailBean.setTableName(tableNameAndOpt.getTableName());
                                }else if(tableNameAndOpt.getTableOpt().isDML()){
                                    testSqlDetailBean.setExecuteType("DML");
                                    testSqlDetailBean.setTableName(tableNameAndOpt.getTableName());
                                    testSqlDetailBean.setKeyCol(tableNameAndOpt.getTableOpt().name());
                                }else if(tableNameAndOpt.getTableOpt().isDQL()){
                                    testSqlDetailBean.setExecuteType("DQL");
                                    testSqlDetailBean.setTableName(tableNameAndOpt.getTableName());
                                    testSqlDetailBean.setKeyCol(tableNameAndOpt.getTableOpt().name());
                                }else {
                                    respInfo.setRespResult("0");
                                    respInfo.setRespErrorCode("0");
                                    respInfo.setRespErrorDesc("无法执行DDL DML DQL 以外的操作");
                                    return respInfo;
                                }
                                testGrammarBean.getTestSqlBeans().get(i).getTestSqlDetailBeans().add(testSqlDetailBean);
                            }
                        }
                    }
                    if(flag){
                        //定义 订阅 临时表集合都检索不到
                        TestSqlDetailBean testSqlDetailBean = new TestSqlDetailBean();
                        if(tableNameAndOpt.getTableOpt().isDDL()){
                            testSqlDetailBean.setExecuteType("DDL");
                            testSqlDetailBean.setTableName(tableNameAndOpt.getTableName());
                            testSqlDetailBean.setKeyCol(tableNameAndOpt.getTableOpt().name());
                            if("DB2".equalsIgnoreCase(dbType.toString())&&tableNameAndOpt.getTableName().equalsIgnoreCase("SYSPROC.ADMIN_CMD")){

                            }else if(testSqlDetailBean.getKeyCol().equalsIgnoreCase("DROP_INDEX")){
                                //处理oracle drop_index
                            }/*else if(testSqlDetailBean.getKeyCol().equalsIgnoreCase("ALTER_RENAME_TOTABLE")){
                                //处理alter_rename_totable 新表名不做转换
                            }*/else if(!testSqlDetailBean.getKeyCol().equalsIgnoreCase("CREATE_TABLE")&&!testSqlDetailBean.getKeyCol().equalsIgnoreCase("CREATE_VIEW")){
                                respInfo.setRespResult("0");
                                respInfo.setRespErrorCode("0");
                                respInfo.setRespErrorDesc(tableNameAndOpt.getTableName()+"，您未订阅该表或该表不存在");
                                return respInfo;
                            }else if(testSqlDetailBean.getKeyCol().equalsIgnoreCase("CREATE_TABLE")||testSqlDetailBean.getKeyCol().equalsIgnoreCase("CREATE_VIEW")){
                                if(tableNameAndOpt.getTableName().indexOf(".")!=-1){
                                    respInfo.setRespResult("0");
                                    respInfo.setRespErrorCode("0");
                                    respInfo.setRespErrorDesc(tableNameAndOpt.getTableName()+"，创建临时表时，禁止带用户模式，默认创建在当前连接下");
                                    return respInfo;
                                }
                            }
                        }else if(tableNameAndOpt.getTableOpt().isDML()){
                            testSqlDetailBean.setExecuteType("DML");
                            testSqlDetailBean.setTableName(tableNameAndOpt.getTableName());
                            testSqlDetailBean.setKeyCol(tableNameAndOpt.getTableOpt().name());
                            respInfo.setRespResult("0");
                            respInfo.setRespErrorCode("0");
                            respInfo.setRespErrorDesc(tableNameAndOpt.getTableName()+"，您未订阅该表或该表不存在");
                            return respInfo;
                        }else if(tableNameAndOpt.getTableOpt().isDQL()){
                            testSqlDetailBean.setExecuteType("DQL");
                            testSqlDetailBean.setTableName(tableNameAndOpt.getTableName());
                            testSqlDetailBean.setKeyCol(tableNameAndOpt.getTableOpt().name());
                            respInfo.setRespResult("0");
                            respInfo.setRespErrorCode("0");
                            respInfo.setRespErrorDesc(tableNameAndOpt.getTableName()+"，您未订阅该表或该表不存在");
                            return respInfo;
                        }else {
                            respInfo.setRespResult("0");
                            respInfo.setRespErrorCode("0");
                            respInfo.setRespErrorDesc("无法执行DDL DML DQL 以外的操作");
                            return respInfo;
                        }
                        testGrammarBean.getTestSqlBeans().get(i).getTestSqlDetailBeans().add(testSqlDetailBean);
                    }
                }
                //订阅实体权限控制
                if(is_data_range){
                    if(!tablist.isEmpty()){
                        Map<String,String> map = new HashMap<>();
                        for (int h=0;h<tablist.size();h++){
                            List<PLSqlToolColumn> tabColBeans = tablist.get(h).getTab_col_list();
                            if(!tabColBeans.isEmpty()){
                                for (int g=0;g<tabColBeans.size();g++){
                                    if("1".equals(tabColBeans.get(g).getIs_partition())){
                                        if(TAB_VIEW_COL_NAME!=null&&TAB_VIEW_COL_NAME.toUpperCase().indexOf(tabColBeans.get(g).getCol_name().toUpperCase())!=-1){
                                            map.put(tablist.get(h).getPhy_tab_name(),tablist.get(h).getPhy_tab_name()+"_"+data_range);
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        //替换sql语句中的表名
                        for(String key : map.keySet()){
                            log.info("租户："+testGrammarBean.getTenant_id()+"权限表："+key+" 变更后表名："+map.get(key));
                            sql = sql.replace(key,map.get(key));
                        }
                    }
                }
                testGrammarBean.getTestSqlBeans().get(i).setReal_sql(this.plSqlToolQueryService.transVariableAll(sql));
            }
        }

        return respInfo;
    }
}
