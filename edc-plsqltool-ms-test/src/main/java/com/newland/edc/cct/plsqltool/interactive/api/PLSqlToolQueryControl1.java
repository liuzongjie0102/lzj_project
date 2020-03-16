package com.newland.edc.cct.plsqltool.interactive.api;

import com.newland.bd.ms.core.model.RespInfo;
import com.newland.bd.utils.commons.UUIDUtils;
import com.newland.bd.workflow.sql.api.SqlInspect;
import com.newland.bd.workflow.sql.bean.DbType;
import com.newland.bd.workflow.sql.bean.TableNameAndOpt;
import com.newland.bi.baseinfo.user.UserInfo;
import com.newland.bi.util.logger.BaseLogger;
import com.newland.bi.webservice.common.MessageHandler;
import com.newland.edc.cct.dataasset.entity.model.DataAssetServiceReqData;
import com.newland.edc.cct.dataasset.entity.model.DataAssetServiceRequestObject;
import com.newland.edc.cct.dataasset.entity.model.DataAssetServiceRespData;
import com.newland.edc.cct.dataasset.entity.model.DataAssetServiceResponseObject;
import com.newland.edc.cct.dataasset.entity.model.javabean.TabInfoBean;
import com.newland.edc.cct.dataasset.entity.model.javabean.TabReqBean;
import com.newland.edc.cct.dataasset.entity.service.IDataAssetEntityService;
import com.newland.edc.cct.dgw.utils.rest.RestFulServiceUtils;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.*;
import com.newland.edc.cct.plsqltool.interactive.service.PLSqlToolQueryService;
import com.newland.edc.cct.plsqltool.interactive.service.PLSqlToolTestService;
import com.newland.edc.cct.plsqltool.interactive.tool.ConcurrentExcutor;
import com.newland.edc.cct.plsqltool.interactive.util.ClassUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("${cmcc.web.servlet-path}/querydata")
public class PLSqlToolQueryControl1 {

    private static BaseLogger log = (BaseLogger) BaseLogger.getLogger(PLSqlToolQueryControl1.class);

    @Resource(name ="com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolQueryServiceImpl")
    private PLSqlToolQueryService plSqlToolQueryService;

    @Resource(name ="com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolTestServiceImpl")
    private PLSqlToolTestService plSqlToolTestService;

    @Autowired
    private IDataAssetEntityService iDataAssetEntityService;


    private static ConcurrentExcutor concurrentExcutor = new ConcurrentExcutor();

//    /**
//     * 异步提交任务
//     * @param testSqlBean
//     * @return
//     * @throws Exception
//     */
//    @RequestMapping(value = "/asyexecute", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
//    public RespInfo asyexecute(@RequestBody TestSqlBean testSqlBean) throws Exception{
//        RespInfo respInfo = new RespInfo();
//        if(testSqlBean.getTask_id().equals("")){
//            respInfo.setRespResult("0");
//            respInfo.setRespErrorCode("0");
//            respInfo.setRespErrorDesc("sql："+testSqlBean.getSql()+"，因无法获取任务id，任务无法提交执行");
//            return respInfo;
//        }
//        if(testSqlBean.getDbType().equals("")){
//            respInfo.setRespResult("0");
//            respInfo.setRespErrorCode("0");
//            respInfo.setRespErrorDesc("数据库类型为空");
//            return respInfo;
//        }
//        if(testSqlBean.getSql().equals("")){
//            respInfo.setRespResult("0");
//            respInfo.setRespErrorCode("0");
//            respInfo.setRespErrorDesc("执行语句为空");
//            return respInfo;
//        }
//        if(testSqlBean.getResource_id().equals("")){
//            respInfo.setRespResult("0");
//            respInfo.setRespErrorCode("0");
//            respInfo.setRespErrorDesc("资源id为空");
//            return respInfo;
//        }
//        if(testSqlBean.getConn_id().equals("")){
//            respInfo.setRespResult("0");
//            respInfo.setRespErrorCode("0");
//            respInfo.setRespErrorDesc("连接id为空");
//            return respInfo;
//        }
//        if(testSqlBean.getTenant_id().equals("")){
//            respInfo.setRespResult("0");
//            respInfo.setRespErrorCode("0");
//            respInfo.setRespErrorDesc("租户id为空");
//            return respInfo;
//        }
//        if(testSqlBean.getUser_id().equals("")){
//            respInfo.setRespResult("0");
//            respInfo.setRespErrorCode("0");
//            respInfo.setRespErrorDesc("用户id为空");
//            return respInfo;
//        }
//        try{
//            //封装数据请求
//            ExecuteRequestBean executeRequestBean = new ExecuteRequestBean();
//            //遍历testSqlDetailBeans获取执行类型
//            //最低执行类型
//            String execute_type = "DQL";
//            String key_col = "SELECT";
//            String table_name = "";
//            List<String> tabList = new ArrayList<>();//建表集合
//            if(testSqlBean.getTestSqlDetailBeans()!=null&&testSqlBean.getTestSqlDetailBeans().size()>0){
//                for (int i=0;i<testSqlBean.getTestSqlDetailBeans().size();i++){
//                    if(!testSqlBean.getTestSqlDetailBeans().get(i).getExecuteType().equalsIgnoreCase("DQL")&&execute_type.equalsIgnoreCase("DQL")){
//                        //DQL执行层级最低  DDL DML一般只有其中一种
//                        execute_type = testSqlBean.getTestSqlDetailBeans().get(i).getExecuteType();
//                        key_col = testSqlBean.getTestSqlDetailBeans().get(i).getKeyCol();
//                        table_name = testSqlBean.getTestSqlDetailBeans().get(i).getTableName();
//                    }
//                    if(testSqlBean.getTestSqlDetailBeans().get(i).getIsCreateTable().equalsIgnoreCase("true")){
//                        tabList.add(testSqlBean.getTestSqlDetailBeans().get(i).getTableName());
//                    }
//                }
//            }
//            executeRequestBean.setTableName(table_name);
//            executeRequestBean.setResourceId(testSqlBean.getResource_id());
//            executeRequestBean.setConnId(testSqlBean.getConn_id());
//            executeRequestBean.setDbType(testSqlBean.getDbType());
//            executeRequestBean.setExecuteType(execute_type);
//            executeRequestBean.setKey_col(key_col);
//            executeRequestBean.setSql(testSqlBean.getSql());
//            if(key_col.toUpperCase().equals("SELECT")){
//                executeRequestBean.setIsPage("true");
//                executeRequestBean.setPageNum("1000");
//                executeRequestBean.setStartPage("1");
//            }
//            executeRequestBean.setUserId(testSqlBean.getUser_id());
//            executeRequestBean.setTenantId(testSqlBean.getTenant_id());
//
//            CallableTask callableTask = new CallableTask(testSqlBean.getTask_id(),executeRequestBean);
//            ExecuteLog executeLog = new ExecuteLog();
//            executeLog.setId(testSqlBean.getTask_id());
//            executeLog.setUser_id(testSqlBean.getUser_id());
//            executeLog.setTenant_id(testSqlBean.getTenant_id());
//            executeLog.setDb_type(testSqlBean.getDbType());
//            executeLog.setExecute_type(execute_type);
//            executeLog.setKeycol(key_col);
//            executeLog.setResource_id(testSqlBean.getResource_id());
//            executeLog.setConn_id(testSqlBean.getConn_id());
//            executeLog.setSqlstring(testSqlBean.getSql());
//            executeLog.setIp(this.plSqlToolQueryService.getIp());
//            executeLog.setStatus("0");
//            this.plSqlToolQueryService.insertExecuteLog(executeLog);
//            concurrentExcutor.execute(callableTask);
//            DgwSqlToolResult dgwSqlToolResult = (DgwSqlToolResult)concurrentExcutor.getResult(testSqlBean.getTask_id(),20);
//            if(dgwSqlToolResult!=null){
//                if(dgwSqlToolResult.getIs_success()!=null&&dgwSqlToolResult.getIs_success()){
//                    respInfo.setRespResult("1");
//                    respInfo.setRespData(dgwSqlToolResult);
//                }else{
//                    respInfo.setRespResult("0");
//                    respInfo.setRespErrorDesc(dgwSqlToolResult.getErr_msg());
//                }
//            }
//        } catch (Exception e){
//            if(e instanceof TimeoutException){
//                respInfo.setRespResult("0");
//                respInfo.setRespErrorCode("0");
//                respInfo.setRespErrorDesc("任务正在执行，请等候");
//                return respInfo;
//            }else{
//                log.error(e.getMessage(),e);
//            }
//            respInfo.setRespResult("0");
//            respInfo.setRespErrorCode("0");
//            respInfo.setRespErrorDesc(e.getMessage());
//        }
//        return respInfo;
//    }

    @RequestMapping(value = "/queryResult", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo queryResult(@RequestBody ExecuteLog executeLog){
        RespInfo respInfo = new RespInfo();
        if(executeLog.getIp().equals("")){
            respInfo.setRespData("0");
            respInfo.setRespErrorDesc("查询的任务id为空");
            return respInfo;
        }
        try {
            //获取任务日志
            List<ExecuteLog> logs = this.plSqlToolQueryService.selectExecuteLog(executeLog);
            if(!logs.isEmpty()){
                ExecuteLog executeLog1 = logs.get(0);
                if(executeLog1.getStatus()==null){
                    respInfo.setRespData("0");
                    respInfo.setRespErrorDesc(executeLog.getId()+"无效的任务");
                    return respInfo;
                }
                if(executeLog1.getStatus().equals("1")){
                    //获取任务结果
                    List<ExecuteResult> executeResults = this.plSqlToolQueryService.selectExecuteResult(executeLog1.getId());
                    if(!executeResults.isEmpty()){
                        respInfo.setRespResult("1");
                        respInfo.setRespData(executeResults.get(0).getResult());
                    }else{
                        respInfo.setRespResult("0");
                        respInfo.setRespErrorCode("0");
                        respInfo.setRespErrorDesc("查无结果");
                    }
                }else if(executeLog1.getStatus().equals("0")){
                    respInfo.setRespResult("0");
                    respInfo.setRespErrorCode("0");
                    respInfo.setRespErrorDesc("任务正在执行，请等候");
                }else if(executeLog1.getStatus().equals("-1")){
                    ExecuteErrorInfo executeErrorInfo = new ExecuteErrorInfo();
                    executeErrorInfo.setLog_id(executeLog1.getId());
                    List<ExecuteErrorInfo> executeErrorInfos = this.plSqlToolQueryService.selectErrorInfo(executeErrorInfo);
                    if(!executeErrorInfos.isEmpty()){
                        respInfo.setRespResult("0");
                        respInfo.setRespErrorCode("0");
                        respInfo.setRespErrorDesc("任务执行过程中出现异常，Exception："+executeErrorInfos.get(0).getError_info());
                    }else{
                        respInfo.setRespResult("0");
                        respInfo.setRespErrorCode("0");
                        respInfo.setRespErrorDesc("任务执行过程中出现异常");
                    }
                }
            }else{
                respInfo.setRespData("0");
                respInfo.setRespErrorDesc("查无任务或任务已被删除");
                return respInfo;
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
            respInfo.setRespData("0");
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
                if(TAB_VIEW_RESOURCE_TYPE!=null&&TAB_VIEW_RESOURCE_TYPE.toUpperCase().indexOf(dbType.toString().toUpperCase())!=-1){
                    is_data_range = true;
                }
            }
            TableOptAuthority tableOptAuthority = this.plSqlToolQueryService.selectAuthority(testGrammarBean.getUser_id());
            for (int i=0;i<testGrammarBean.getTestSqlBeans().size();i++){
                List<PLSqlToolTable> tablist = new ArrayList<>();
                testGrammarBean.getTestSqlBeans().get(i).setGroup_id(group_id);
                testGrammarBean.getTestSqlBeans().get(i).setTask_id(UUIDUtils.getUUID());
                String sql = testGrammarBean.getTestSqlBeans().get(i).getSql();
                Pair<String, List<TableNameAndOpt>> pair = SqlInspect.inspect(sql, dbType ,true);//允许select *
                List<TableNameAndOpt> tableNameAndOptList = pair.getRight();
                testGrammarBean.getTestSqlBeans().get(i).setTestSqlDetailBeans(new ArrayList<>());
                for (TableNameAndOpt tableNameAndOpt: tableNameAndOptList){
                    String authority_value = "0";
                    try {
                        authority_value = ClassUtil.invoke(tableOptAuthority,"get"+tableNameAndOpt.getTableOpt().name()).toString();
                    }catch (Exception e){
                        log.info(e.getMessage(),e);
                        authority_value = "0";
                    }
                    if(!authority_value.equals("1")){
                        respInfo.setRespResult("0");
                        respInfo.setRespErrorCode("0");
                        respInfo.setRespErrorDesc(tableNameAndOpt.getTableName()+"，无权限执行"+tableNameAndOpt.getTableOpt().name()+"操作");
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
                                    respInfo.setRespErrorDesc(tableNameAndOpt.getTableName()+"，您不能对定义的表进行表结构更改、更新数据等DDL、DML操作，您只能进行查询操作。");
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
                                        respInfo.setRespErrorDesc(tableNameAndOpt.getTableName()+"，您不能对定义的表进行表结构更改、更新数据等DDL、DML操作，您只能进行查询操作。");
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
                                tablist.add(dataAssetEntity.getSubscriptions().get(n));
                                flag = false;
                                TestSqlDetailBean testSqlDetailBean = new TestSqlDetailBean();
                                if(tableNameAndOpt.getTableOpt().isDDL()){
                                    testSqlDetailBean.setExecuteType("DDL");
                                    testSqlDetailBean.setKeyCol(tableNameAndOpt.getTableOpt().name());
                                    respInfo.setRespResult("0");
                                    respInfo.setRespErrorCode("0");
                                    respInfo.setRespErrorDesc(tableNameAndOpt.getTableName()+"，您不能对订阅的表进行表结构更改、更新数据等DDL、DML操作，您只能进行查询操作。");
                                    return respInfo;
                                }else if(tableNameAndOpt.getTableOpt().isDML()){
                                    testSqlDetailBean.setExecuteType("DML");
                                    testSqlDetailBean.setTableName(tableNameAndOpt.getTableName());
                                    testSqlDetailBean.setKeyCol(tableNameAndOpt.getTableOpt().name());
                                    respInfo.setRespResult("0");
                                    respInfo.setRespErrorCode("0");
                                    respInfo.setRespErrorDesc(tableNameAndOpt.getTableName()+"，您不能对订阅的表进行表结构更改、更新数据等DDL、DML操作，您只能进行查询操作。");
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
                            testSqlDetailBean.setKeyCol(tableNameAndOpt.getTableOpt().name());
                            if(!testSqlDetailBean.getKeyCol().equalsIgnoreCase("CREATE")&&!testSqlDetailBean.getKeyCol().equalsIgnoreCase("ALTER_RENAME_TOTABLE")){
                                respInfo.setRespResult("0");
                                respInfo.setRespErrorCode("0");
                                respInfo.setRespErrorDesc(tableNameAndOpt.getTableName()+"，您未订阅该表或该表不存在");
                                return respInfo;
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

    @RequestMapping(value = "/getTabEntityInfo/{user_id}/{tab_id}", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo getEntityData(@PathVariable String tab_id,@PathVariable String user_id) {
        RespInfo respInfo = new RespInfo();
        TabReqBean reqBean = new TabReqBean();
        reqBean.setTab_id(tab_id);
        reqBean.setJudge_state("3");
        String start = "1";
        String pageCount = "10";

        DataAssetServiceRequestObject requestObject = new DataAssetServiceRequestObject();
        DataAssetServiceReqData reqData = new DataAssetServiceReqData();
        reqData.setTab_info_req_bean(reqBean);
        MessageHandler.newInstance().setReqData(requestObject, reqData);
        MessageHandler.newInstance().setStart(requestObject,start);
        MessageHandler.newInstance().setPageCount(requestObject,pageCount);
        UserInfo userInfo = new UserInfo();
//        if(userInfo!=null){
//            MessageHandler.newInstance().setTokenCode(requestObject, userInfo.getToken_code());
//            MessageHandler.newInstance().setClientIdPassword(requestObject, userInfo.getUser_id(), userInfo.getPassword());
//        }
        userInfo.setUser_id(user_id);
        userInfo.setPassword("111111");
        MessageHandler.newInstance().setClientIdPassword(requestObject, userInfo.getUser_id(), userInfo.getPassword());
        DataAssetServiceResponseObject responseObject = iDataAssetEntityService.getTabInfoList(requestObject);
        DataAssetServiceRespData respData = (DataAssetServiceRespData) MessageHandler.newInstance().getRespData(responseObject);
        List<TabInfoBean> resList = respData.getTab_info_list();
        respInfo.setRespData(resList);
        return respInfo;
    }
}
