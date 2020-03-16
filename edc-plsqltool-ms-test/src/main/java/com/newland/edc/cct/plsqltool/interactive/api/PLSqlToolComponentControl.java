package com.newland.edc.cct.plsqltool.interactive.api;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.newland.bd.ms.core.model.RespInfo;
import com.newland.bd.utils.commons.UUIDUtils;
import com.newland.bd.workflow.sql.api.SqlInspect;
import com.newland.bd.workflow.sql.bean.DbType;
import com.newland.bd.workflow.sql.bean.TableNameAndOpt;
import com.newland.bi.util.logger.BaseLogger;
import com.newland.edc.cct.dgw.utils.rest.RestClientUtil;
import com.newland.edc.cct.dgw.utils.rest.RestFulServiceUtils;
import com.newland.edc.cct.dgw.utils.token.TokenUtil;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.*;
import com.newland.edc.cct.plsqltool.interactive.service.PLSqlToolQueryService;
import com.newland.edc.cct.plsqltool.interactive.service.PLSqlToolTestService;
import com.newland.edc.pub.system.expression.model.ExpVarBean;
import com.newland.edc.pub.system.expression.model.TranslateResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RestController
@RequestMapping("${cmcc.web.servlet-path}/querydata")
public class PLSqlToolComponentControl {
    private static BaseLogger log = (BaseLogger) BaseLogger.getLogger(PLSqlToolComponentControl.class);

    @Resource(name ="com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolTestServiceImpl")
    private PLSqlToolTestService plSqlToolTestService;

    @Resource(name ="com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolQueryServiceImpl")
    private PLSqlToolQueryService plSqlToolQueryService;

    /**
     * 语法、执行权限校验接口
     * @param testGrammarBean
     * @return
     */
    @RequestMapping(value = "/testGrammarComponent", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo testGrammarComponent(@RequestBody TestGrammarBean testGrammarBean,
                                          @RequestHeader(value = "X-UserId", required = true) String userId,
                                          @RequestHeader(value = "X-NG-SessionId", required = false) String sessionId) {
        RespInfo respInfo = new RespInfo();
        if(StringUtils.isBlank(userId)){
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc("请求用户id为空");
            return respInfo;
        }
        try {
            if(StringUtils.isNotBlank(testGrammarBean.getConn_name_en())&&StringUtils.isBlank(testGrammarBean.getConn_id())){
                String conn_id = this.plSqlToolQueryService.queryConnIdByConnNameEn(testGrammarBean.getConn_name_en());
                if(StringUtils.isBlank(conn_id)){
                    respInfo.setRespResult("0");
                    respInfo.setRespErrorDesc("无效的连接信息");
                    return respInfo;
                }
                testGrammarBean.setConn_id(conn_id);
            }else if(StringUtils.isBlank(testGrammarBean.getConn_name_en())&&StringUtils.isBlank(testGrammarBean.getConn_id())){
                respInfo.setRespResult("0");
                respInfo.setRespErrorDesc("无法获取具体连接信息");
                return respInfo;
            }
            if(testGrammarBean.getDbType().equalsIgnoreCase("HIVE")){
                testGrammarBean = plSqlToolTestService.testGrammar(testGrammarBean,DbType.HIVE);
                respInfo = this.tenantAuthorityVer(testGrammarBean,DbType.HIVE,userId,sessionId);
            }else if(testGrammarBean.getDbType().equalsIgnoreCase("ORACLE")){
                testGrammarBean = plSqlToolTestService.testGrammar(testGrammarBean,DbType.ORACLE);
                respInfo = this.tenantAuthorityVer(testGrammarBean,DbType.ORACLE,userId,sessionId);
            }else if(testGrammarBean.getDbType().equalsIgnoreCase("MYSQL")){
                testGrammarBean = plSqlToolTestService.testGrammar(testGrammarBean,DbType.MYSQL);
                respInfo = this.tenantAuthorityVer(testGrammarBean,DbType.MYSQL,userId,sessionId);
            }else if(testGrammarBean.getDbType().equalsIgnoreCase("DB2")){
                testGrammarBean = plSqlToolTestService.testGrammar(testGrammarBean,DbType.DB2);
                respInfo = this.tenantAuthorityVer(testGrammarBean,DbType.DB2,userId,sessionId);
            }else if(testGrammarBean.getDbType().equalsIgnoreCase("GREENPLUM")){
                testGrammarBean = plSqlToolTestService.testGrammar(testGrammarBean,DbType.GP);
                respInfo = this.tenantAuthorityVer(testGrammarBean,DbType.GP,userId,sessionId);
            }else if(testGrammarBean.getDbType().equalsIgnoreCase("GBASE")){
                testGrammarBean = plSqlToolTestService.testGrammar(testGrammarBean,DbType.GBASE);
                respInfo = this.tenantAuthorityVer(testGrammarBean,DbType.GBASE,userId,sessionId);
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

    public RespInfo tenantAuthorityVer(TestGrammarBean testGrammarBean,DbType dbType,String userId,String sessionId) throws Exception{
        RespInfo respInfo = new RespInfo();
        String group_id = UUIDUtils.getUUID();
        //执行权限校验（语法校验通过，并且权限开关打开）
        if(testGrammarBean.getTestResult().equals("true")&&testGrammarBean.isTestAuthorityVer()){
            Map<String, String> headerMap = new HashMap<>();
            headerMap.put("X-UserId",userId);
            headerMap.put("X-NG-SessionId",sessionId);
            if(StringUtils.isBlank(testGrammarBean.getComponentService())){
                respInfo.setRespResult("0");
                respInfo.setRespErrorCode("0");
                respInfo.setRespErrorDesc("未提供列表提取服务名");
                return respInfo;
            }
            String url = RestFulServiceUtils.getconfig(testGrammarBean.getComponentService());
            if(StringUtils.isBlank(url)){
                respInfo.setRespResult("0");
                respInfo.setRespErrorCode("0");
                respInfo.setRespErrorDesc("服务名："+testGrammarBean.getComponentService()+"，未注册");
                return respInfo;
            }
            String json = "{\"isQueryDetail\":\"1\",\"resourceId\":\""+testGrammarBean.getResource_id()+"\",\"tenantId\":\""+testGrammarBean.getTenant_id()+"\",\"userId\":\""+testGrammarBean.getUser_id()+"\"}";
            log.info("请求地址："+url+" 请求报文："+json);
            RespInfo respInfo1 = RestClientUtil.sendRestClient(null,url,testGrammarBean.getUser_id(),json,headerMap,60*1000);
            if(respInfo1.getRespResult().equals("0")){
                //获取校验列表信息异常
                return respInfo1;
            }
            JSONArray objects = JSONArray.parseArray(respInfo1.getRespData().toString());
            List<PLSqlToolTable> definitions = new ArrayList<>();//定义集合
            List<PLSqlToolTable> subscriptions = new ArrayList<>();//订阅集合
            List<PLSqlToolTable> temporarytabs = new ArrayList<>();//临时表
            for (int m = 0;m<objects.size();m++){
                JSONObject object = JSONObject.parseObject(objects.getString(m));
                if("definitions".equals(object.getString("name"))){
                    definitions = JSONArray.parseArray(object.getString("tables"), PLSqlToolTable.class);
                }
                if("subscriptions".equals(object.getString("name"))){
                    subscriptions = JSONArray.parseArray(object.getString("tables"), PLSqlToolTable.class);
                }
                if("temporarytabs".equals(object.getString("name"))){
                    temporarytabs = JSONArray.parseArray(object.getString("tables"), PLSqlToolTable.class);
                }
            }
            if(definitions.isEmpty()&&subscriptions.isEmpty()){
                respInfo.setRespResult("0");
                respInfo.setRespErrorCode("0");
                respInfo.setRespErrorDesc("当前无操作权限");
                return respInfo;
            }
            //添加前缀 begin
            for (PLSqlToolTable plSqlToolTable : subscriptions){
                if(testGrammarBean.getDbType().equals("hive")&&StringUtils.isNotBlank(plSqlToolTable.getDispose_list().get(0).getDb_name())){
                    plSqlToolTable.setPhy_tab_name(plSqlToolTable.getDispose_list().get(0).getDb_name()+"."+plSqlToolTable.getPhy_tab_name());
                }else if(testGrammarBean.getDbType().equals("oracle")&&StringUtils.isNotBlank(plSqlToolTable.getDispose_list().get(0).getDb_user())){
                    plSqlToolTable.setPhy_tab_name(plSqlToolTable.getDispose_list().get(0).getDb_user()+"."+plSqlToolTable.getPhy_tab_name());
                }else if(testGrammarBean.getDbType().equals("db2")&&StringUtils.isNotBlank(plSqlToolTable.getDispose_list().get(0).getDb_user())){
                    plSqlToolTable.setPhy_tab_name(plSqlToolTable.getDispose_list().get(0).getDb_user()+"."+plSqlToolTable.getPhy_tab_name());
                }else if(testGrammarBean.getDbType().equals("greenplum")&&StringUtils.isNotBlank(plSqlToolTable.getDispose_list().get(0).getDb_user())){
                    plSqlToolTable.setPhy_tab_name(plSqlToolTable.getDispose_list().get(0).getDb_user()+"."+plSqlToolTable.getPhy_tab_name());
                }else if(testGrammarBean.getDbType().equals("mysql")&&StringUtils.isNotBlank(plSqlToolTable.getDispose_list().get(0).getDb_name())){
                    plSqlToolTable.setPhy_tab_name(plSqlToolTable.getDispose_list().get(0).getDb_user()+"."+plSqlToolTable.getPhy_tab_name());
                }else if(testGrammarBean.getDbType().equals("gbase")&&StringUtils.isNotBlank(plSqlToolTable.getDispose_list().get(0).getDb_name())){
                    plSqlToolTable.setPhy_tab_name(plSqlToolTable.getDispose_list().get(0).getDb_user()+"."+plSqlToolTable.getPhy_tab_name());
                }else{
                    continue;
                }
            }
            for (PLSqlToolTable plSqlToolTable : definitions){
                if(testGrammarBean.getDbType().equals("hive")&&StringUtils.isNotBlank(plSqlToolTable.getDispose_list().get(0).getDb_name())){
                    plSqlToolTable.setPhy_tab_name(plSqlToolTable.getDispose_list().get(0).getDb_name()+"."+plSqlToolTable.getPhy_tab_name());
                }else if(testGrammarBean.getDbType().equals("oracle")&&StringUtils.isNotBlank(plSqlToolTable.getDispose_list().get(0).getDb_user())){
                    plSqlToolTable.setPhy_tab_name(plSqlToolTable.getDispose_list().get(0).getDb_user()+"."+plSqlToolTable.getPhy_tab_name());
                }else if(testGrammarBean.getDbType().equals("db2")&&StringUtils.isNotBlank(plSqlToolTable.getDispose_list().get(0).getDb_user())){
                    plSqlToolTable.setPhy_tab_name(plSqlToolTable.getDispose_list().get(0).getDb_user()+"."+plSqlToolTable.getPhy_tab_name());
                }else if(testGrammarBean.getDbType().equals("greenplum")&&StringUtils.isNotBlank(plSqlToolTable.getDispose_list().get(0).getDb_user())){
                    plSqlToolTable.setPhy_tab_name(plSqlToolTable.getDispose_list().get(0).getDb_user()+"."+plSqlToolTable.getPhy_tab_name());
                }else if(testGrammarBean.getDbType().equals("mysql")&&StringUtils.isNotBlank(plSqlToolTable.getDispose_list().get(0).getDb_name())){
                    plSqlToolTable.setPhy_tab_name(plSqlToolTable.getDispose_list().get(0).getDb_user()+"."+plSqlToolTable.getPhy_tab_name());
                }else if(testGrammarBean.getDbType().equals("gbase")&&StringUtils.isNotBlank(plSqlToolTable.getDispose_list().get(0).getDb_name())){
                    plSqlToolTable.setPhy_tab_name(plSqlToolTable.getDispose_list().get(0).getDb_user()+"."+plSqlToolTable.getPhy_tab_name());
                }else{
                    continue;
                }
            }
            //添加前缀 end
            String data_range = this.plSqlToolQueryService.selectDataRange(testGrammarBean.getTenant_id());
            boolean is_data_range = false;
            String TAB_VIEW_RESOURCE_TYPE = RestFulServiceUtils.getconfig("TAB_VIEW_RESOURCE_TYPE");
            String TAB_VIEW_COL_NAME = RestFulServiceUtils.getconfig("TAB_VIEW_COL_NAME");
            //满足当前租户有做权限控制 表视图配置失效 以及数据库类型一致情况
            if((data_range!=null&&!data_range.equals(""))){
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
                        for (int m=0;m<definitions.size();m++){
                            if(tableNameAndOpt.getTableName().equalsIgnoreCase(definitions.get(m).getPhy_tab_name())) {
                                //定义
                                flag = false;
                                TestSqlDetailBean testSqlDetailBean = new TestSqlDetailBean();
                                if (tableNameAndOpt.getTableOpt().isDDL()) {
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
                                }else if(tableNameAndOpt.getTableOpt().isDML()) {
                                    testSqlDetailBean.setExecuteType("DML");
                                    testSqlDetailBean.setKeyCol(tableNameAndOpt.getTableOpt().name());
                                    testSqlDetailBean.setTableName(tableNameAndOpt.getTableName());
                                    if (RestFulServiceUtils.getconfig("DE_TABLE_TRUNCATE") == null || !RestFulServiceUtils.getconfig("DE_TABLE_TRUNCATE").equals("1")) {
                                        if (testSqlDetailBean.getKeyCol().equalsIgnoreCase("TRUNCATE")) {
                                            respInfo.setRespResult("0");
                                            respInfo.setRespErrorCode("0");
                                            respInfo.setRespErrorDesc(tableNameAndOpt.getTableName() + "，定义实体TRUNCATE操作未开启，无法执行");
                                            return respInfo;
                                        }
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
                        for (int n=0;n<subscriptions.size();n++){
                            if(tableNameAndOpt.getTableName().equalsIgnoreCase(subscriptions.get(n).getPhy_tab_name())){
                                //订阅
                                subscriptions.get(n).setPhy_tab_name(tableNameAndOpt.getTableName());
                                tablist.add(subscriptions.get(n));
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
                        respInfo.setRespResult("0");
                        respInfo.setRespErrorCode("0");
                        respInfo.setRespErrorDesc(tableNameAndOpt.getTableName()+"，您未订阅该表或该表不存在");
                        return respInfo;
                    }
                }
                //添加后缀
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
            }
            //语法通过情况下，并且权限校验关闭
        }else if(testGrammarBean.getTestResult().equals("true")&&!testGrammarBean.isTestAuthorityVer()){
            for (int i=0;i<testGrammarBean.getTestSqlBeans().size();i++){
                String sql = testGrammarBean.getTestSqlBeans().get(i).getSql();
                Pair<String, List<TableNameAndOpt>> pair = SqlInspect.inspect(sql, dbType ,true);//允许select *
                List<TableNameAndOpt> tableNameAndOptList = pair.getRight();
                testGrammarBean.getTestSqlBeans().get(i).setTestSqlDetailBeans(new ArrayList<>());
                for (TableNameAndOpt tableNameAndOpt: tableNameAndOptList){
                    TestSqlDetailBean testSqlDetailBean = new TestSqlDetailBean();
                    if(tableNameAndOpt.getTableOpt().isDDL()){
                        testSqlDetailBean.setExecuteType("DDL");
                    }
                    if(tableNameAndOpt.getTableOpt().isDML()){
                        testSqlDetailBean.setExecuteType("DML");
                    }
                    if(tableNameAndOpt.getTableOpt().isDQL()){
                        testSqlDetailBean.setExecuteType("DQL");
                    }
                    testSqlDetailBean.setTableName(tableNameAndOpt.getTableName());
                    testSqlDetailBean.setKeyCol(tableNameAndOpt.getTableOpt().name());
                    testGrammarBean.getTestSqlBeans().get(i).getTestSqlDetailBeans().add(testSqlDetailBean);
                }
            }

        }
        //语法校验通过并且开启权限校验
        if(testGrammarBean.getTestResult().equals("true")&&testGrammarBean.isTestAuthorityVer()){
            //判断表量列表是否为空
            if(testGrammarBean.getExpVarBeans()!=null&&!testGrammarBean.getExpVarBeans().isEmpty()){
                //变量校验
                String sql = testGrammarBean.getSql();
                String regex = "\\$\\{\\w*\\}";
                Pattern pattern = Pattern.compile(regex);
                Matcher n = pattern.matcher(sql);
                List<String> list = new ArrayList<>();
                while (n.find()){
                    String str = n.group(0);
                    list.add(str);
                }
                String url = RestFulServiceUtils.getServiceUrl("edc-pub-system-ms")+"/expression/translate";
                TranslateReq translateReq = new TranslateReq();
                translateReq.setTenantId(testGrammarBean.getTenant_id());
                translateReq.setExpressionList(list);
                Map<String,String> expVarMap = new HashMap<>();
                if(testGrammarBean.getExpVarBeans()!=null&&!testGrammarBean.getExpVarBeans().isEmpty()){
                    List<TranslateParam> varList = new ArrayList<>();
                    for (ExpVarBean expVarBean : testGrammarBean.getExpVarBeans()){
                        TranslateParam translateParam = new TranslateParam();
                        translateParam.setName(expVarBean.getName().replace("${","").replace("}",""));
                        translateParam.setValue(expVarBean.getValue());
                        varList.add(translateParam);
                        expVarMap.put(expVarBean.getName(),expVarBean.getValue());
                    }
                    translateReq.setVarList(varList);
                }
                log.info("请求翻译服务url："+url+" 请求报文："+JSON.toJSONString(translateReq));
                Map<String, String> headerMap = new HashMap<>();
                headerMap.put("X-NG-Token", TokenUtil.getToken());
                RespInfo respInfo1 = RestClientUtil.sendRestClient(TranslateResult.class,url,testGrammarBean.getUser_id(),JSON.toJSONString(translateReq),headerMap,60*1000);
                if(respInfo1.getRespResult().equals("0")){
                    //获取变量信息失败
                    return respInfo1;
                }
                List<TranslateResult> translateResults = (List<TranslateResult>)respInfo1.getRespData();
                if(translateResults!=null&&!translateResults.isEmpty()){
                    //合并前端传过来的变量
                    for (int i=0;i<translateResults.size();i++){
                        //过滤变量查询结果中无法翻译
                        if(translateResults.get(i).getOriginal().equals(translateResults.get(i).getResult())){
                            throw new Exception("变量"+translateResults.get(i).getOriginal()+"无法翻译");
                        }
                        //翻译结果为空或翻译后仍有$
                        if(StringUtils.isBlank(translateResults.get(i).getResult())||translateResults.get(i).getResult().indexOf("$")>=0){
                            //前端有传配置参数
                            if(expVarMap.get(translateResults.get(i).getOriginal())!=null){
                                //替换参数值
                                translateResults.get(i).setResult(expVarMap.get(translateResults.get(i).getOriginal()));
                            }else{
                                //空处理
                                translateResults.get(i).setResult("");
                            }
                        }
                        for (int j=0;j<testGrammarBean.getTestSqlBeans().size();j++){
                            testGrammarBean.getTestSqlBeans().get(j).setReal_sql(testGrammarBean.getTestSqlBeans().get(j).getReal_sql().replace(translateResults.get(i).getOriginal(),translateResults.get(i).getResult()));
                        }
                    }
                }
            }
        }
        return respInfo;
    }


    @RequestMapping(value = "/getEntityDataComponent", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo getEntityData(@RequestBody EntityDataReq entityDataReq,
                                   @RequestHeader(value = "X-NG-SessionId", required = false) String sessionId) {
        RespInfo respInfo = new RespInfo();
        if(StringUtils.isBlank(entityDataReq.getUser_id())){
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc("请求用户id为空");
            return respInfo;
        }
        if(StringUtils.isBlank(entityDataReq.getTenant_id())){
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc("请求租户id为空");
            return respInfo;
        }
        if(StringUtils.isBlank(entityDataReq.getResource_id())){
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc("请求资源id为空");
            return respInfo;
        }
        if(StringUtils.isBlank(entityDataReq.getDb_type())){
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc("请求数据库类型为空");
            return respInfo;
        }
        if(StringUtils.isBlank(entityDataReq.getComponentService())){
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc("未提供列表提取服务名");
            return respInfo;
        }
        try {
            String url = RestFulServiceUtils.getconfig(entityDataReq.getComponentService());
            Map<String, String> headerMap = new HashMap<>();
            headerMap.put("X-UserId",entityDataReq.getUser_id());
            headerMap.put("X-NG-SessionId",sessionId);
            String json = "{\"isQueryDetail\":\"1\",\"resourceId\":\""+entityDataReq.getResource_id()+"\",\"tenantId\":\""+entityDataReq.getTenant_id()+"\",\"userId\":\""+entityDataReq.getUser_id()+"\"}";
            log.info("请求地址："+url+" 请求报文："+json);
            RespInfo respInfo1 = RestClientUtil.sendRestClient(null,url,entityDataReq.getUser_id(),json,headerMap,60*1000);
            if(respInfo1.getRespResult().equals("0")){
                //获取校验列表信息异常
                return respInfo1;
            }
            JSONArray objects = JSONArray.parseArray(respInfo1.getRespData().toString());
            List<PLSqlToolTable> definitions = new ArrayList<>();//定义集合
            List<PLSqlToolTable> subscriptions = new ArrayList<>();//订阅集合
            List<PLSqlToolTable> temporarytabs = new ArrayList<>();//临时表
            for (int m = 0;m<objects.size();m++){
                JSONObject object = JSONObject.parseObject(objects.getString(m));
                if("definitions".equals(object.getString("name"))){
                    definitions = JSONArray.parseArray(object.getString("tables"), PLSqlToolTable.class);
                }
                if("subscriptions".equals(object.getString("name"))){
                    subscriptions = JSONArray.parseArray(object.getString("tables"), PLSqlToolTable.class);
                }
                if("temporarytabs".equals(object.getString("name"))){
                    temporarytabs = JSONArray.parseArray(object.getString("tables"), PLSqlToolTable.class);
                }
            }
            //添加前缀
            for (PLSqlToolTable plSqlToolTable : subscriptions){
                if(entityDataReq.getDb_type().equals("hive")&&StringUtils.isNotBlank(plSqlToolTable.getDispose_list().get(0).getDb_name())){
                    plSqlToolTable.setPhy_tab_name(plSqlToolTable.getDispose_list().get(0).getDb_name()+"."+plSqlToolTable.getPhy_tab_name());
                }else if(entityDataReq.getDb_type().equals("oracle")&&StringUtils.isNotBlank(plSqlToolTable.getDispose_list().get(0).getDb_user())){
                    plSqlToolTable.setPhy_tab_name(plSqlToolTable.getDispose_list().get(0).getDb_user()+"."+plSqlToolTable.getPhy_tab_name());
                }else if(entityDataReq.getDb_type().equals("db2")&&StringUtils.isNotBlank(plSqlToolTable.getDispose_list().get(0).getDb_user())){
                    plSqlToolTable.setPhy_tab_name(plSqlToolTable.getDispose_list().get(0).getDb_user()+"."+plSqlToolTable.getPhy_tab_name());
                }else if(entityDataReq.getDb_type().equals("greenplum")&&StringUtils.isNotBlank(plSqlToolTable.getDispose_list().get(0).getDb_user())){
                    plSqlToolTable.setPhy_tab_name(plSqlToolTable.getDispose_list().get(0).getDb_user()+"."+plSqlToolTable.getPhy_tab_name());
                }else if(entityDataReq.getDb_type().equals("mysql")&&StringUtils.isNotBlank(plSqlToolTable.getDispose_list().get(0).getDb_name())){
                    plSqlToolTable.setPhy_tab_name(plSqlToolTable.getDispose_list().get(0).getDb_user()+"."+plSqlToolTable.getPhy_tab_name());
                }else if(entityDataReq.getDb_type().equals("gbase")&&StringUtils.isNotBlank(plSqlToolTable.getDispose_list().get(0).getDb_name())){
                    plSqlToolTable.setPhy_tab_name(plSqlToolTable.getDispose_list().get(0).getDb_user()+"."+plSqlToolTable.getPhy_tab_name());
                }else{
                    continue;
                }
            }
            for (PLSqlToolTable plSqlToolTable : definitions){
                if(entityDataReq.getDb_type().equals("hive")&&StringUtils.isNotBlank(plSqlToolTable.getDispose_list().get(0).getDb_name())){
                    plSqlToolTable.setPhy_tab_name(plSqlToolTable.getDispose_list().get(0).getDb_name()+"."+plSqlToolTable.getPhy_tab_name());
                }else if(entityDataReq.getDb_type().equals("oracle")&&StringUtils.isNotBlank(plSqlToolTable.getDispose_list().get(0).getDb_user())){
                    plSqlToolTable.setPhy_tab_name(plSqlToolTable.getDispose_list().get(0).getDb_user()+"."+plSqlToolTable.getPhy_tab_name());
                }else if(entityDataReq.getDb_type().equals("db2")&&StringUtils.isNotBlank(plSqlToolTable.getDispose_list().get(0).getDb_user())){
                    plSqlToolTable.setPhy_tab_name(plSqlToolTable.getDispose_list().get(0).getDb_user()+"."+plSqlToolTable.getPhy_tab_name());
                }else if(entityDataReq.getDb_type().equals("greenplum")&&StringUtils.isNotBlank(plSqlToolTable.getDispose_list().get(0).getDb_user())){
                    plSqlToolTable.setPhy_tab_name(plSqlToolTable.getDispose_list().get(0).getDb_user()+"."+plSqlToolTable.getPhy_tab_name());
                }else if(entityDataReq.getDb_type().equals("mysql")&&StringUtils.isNotBlank(plSqlToolTable.getDispose_list().get(0).getDb_name())){
                    plSqlToolTable.setPhy_tab_name(plSqlToolTable.getDispose_list().get(0).getDb_user()+"."+plSqlToolTable.getPhy_tab_name());
                }else if(entityDataReq.getDb_type().equals("gbase")&&StringUtils.isNotBlank(plSqlToolTable.getDispose_list().get(0).getDb_name())){
                    plSqlToolTable.setPhy_tab_name(plSqlToolTable.getDispose_list().get(0).getDb_user()+"."+plSqlToolTable.getPhy_tab_name());
                }else{
                    continue;
                }
            }
            PLSqlToolDataAssetEntity returnData = new PLSqlToolDataAssetEntity();
            returnData.setDefinitions(definitions);
            returnData.setSubscriptions(subscriptions);
            respInfo.setRespData(returnData);
            respInfo.setRespResult("1");
        }catch (Exception e){
            log.error(e.getMessage(),e);
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc(e.getMessage());
        }
        return respInfo;
    }

}
