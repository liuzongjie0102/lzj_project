package com.newland.edc.cct.plsqltool.interactive.api;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.newland.bd.ms.core.model.RespInfo;
import com.newland.bi.util.logger.BaseLogger;
import com.newland.edc.cct.dataasset.dispose.model.javabean.DataDispBean;
import com.newland.edc.cct.dataasset.entity.model.javabean.TabInfoBean;
import com.newland.edc.cct.dgw.utils.rest.RestFulServiceUtils;
import com.newland.edc.cct.plsqltool.interactive.common.ResAddress;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.*;
import com.newland.edc.cct.plsqltool.interactive.service.*;
import com.newland.edc.cct.plsqltool.interactive.util.DataCatch;
import com.newland.edc.cct.plsqltool.interactive.util.DataSourceAccess;
import com.newland.edc.cct.plsqltool.interactive.util.HttpUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

@RestController
@RequestMapping("${cmcc.web.servlet-path}/querydata")
public class PLSqlToolQueryControl {

    private static BaseLogger log = (BaseLogger) BaseLogger.getLogger(PLSqlToolQueryControl.class);

    @Resource(name ="com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolQueryServiceImpl")
    private PLSqlToolQueryService plSqlToolQueryService;

    @Resource(name ="com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolTestServiceImpl")
    private PLSqlToolTestService plSqlToolTestService;

    @Resource(name ="com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolExecuteServiceImpl")
    private PLSqlToolExecuteService plSqlToolExecuteService;

    @Resource(name = "com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolCloseConnServiceImpl")
    private IPLSqlToolCloseConnService iplSqlToolCloseConnService;

    @Resource(name ="com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolTaskServiceImpl")
    private PLSqlToolTaskService plSqlToolTaskService;

    /**
     * 根据租户 资源 连接获取实体
     * @param conn_id
     * @param tenant_id
     * @param resource_id
     * @return
     */
    @RequestMapping(value = "/getEntityData/{tenant_id}/{resource_id}/{conn_id}", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo getEntityData(@PathVariable String conn_id,@PathVariable String tenant_id,@PathVariable String resource_id) {
        RespInfo respInfo = new RespInfo();
        if(conn_id==null||conn_id.equals("")||tenant_id==null||tenant_id.equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("连接id或租户id为空");
            return respInfo;
        }
        try{
//            DataAssetEntity returnData = plSqlToolQueryService.getCatchData(tenant_id,resource_id,conn_id);
            PLSqlToolDataAssetEntity returnData = plSqlToolQueryService.getTabEntity(tenant_id,resource_id,conn_id);
            if(returnData==null){
                returnData = new PLSqlToolDataAssetEntity();
            }
            respInfo.setRespData(returnData);
            respInfo.setRespResult("1");
            respInfo.setRespErrorDesc("");
        }catch (Exception e){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc(e.getMessage());
        }
        return respInfo;
    }

    /**
     * 根据租户 用户获取实体
     * @param user_id
     * @param tenant_id
     * @return
     */
    @RequestMapping(value = "/queryEntityData/{tenant_id}/{user_id}", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo queryEntityData(@PathVariable String user_id,@PathVariable String tenant_id) {
        RespInfo respInfo = new RespInfo();
        if(user_id==null||user_id.equals("")||tenant_id==null||tenant_id.equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("用户id或租户id为空");
            return respInfo;
        }
        try{
            DataAssetEntity dataAssetEntity = plSqlToolQueryService.queryEntityData(user_id,tenant_id);
            DataCatch.getDataAssetEntityMap().put(tenant_id,dataAssetEntity);
            respInfo.setRespData(dataAssetEntity);
            respInfo.setRespResult("1");
        }catch (Exception e){
            log.error(e.getMessage(),e);
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc(e.getMessage());
        }
        return respInfo;
    }

    /**
     * 根据数据库类型获取租户资源连接
     * @param db_type
     * @return
     */
    @RequestMapping(value = "/getTenants/{db_type}", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo getTenants(@PathVariable String db_type){
        RespInfo respInfo = new RespInfo();
        try {
            List<DBTenantBean> dbTenantBeans = plSqlToolQueryService.getTenants(db_type);
            respInfo.setRespData(dbTenantBeans);
            respInfo.setRespResult("1");
            respInfo.setRespErrorDesc("");
        }catch (Exception e){
            log.error(e.getMessage(),e);
            List<DBTenantBean> tabTenantBeans = new ArrayList<>();
            respInfo.setRespData(tabTenantBeans);
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc(e.getMessage());
        }
        return respInfo;
    }

    /**
     * 根据用户 登陆校验串 数据库类型获取租户资源连接
     * @param user_id
     * @param tokencode
     * @param db_type
     * @return
     */
    @RequestMapping(value = "/getTenantsByUserId/{user_id}/{tokencode}/{db_type}", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo getTenantsByUserId(@PathVariable String user_id,@PathVariable String tokencode,@PathVariable String db_type){
        RespInfo respInfo = new RespInfo();
        if(StringUtils.isBlank(user_id)||StringUtils.isBlank(tokencode)){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("用户id或tokencode为空");
            return respInfo;
        }
        try {
            List<DBTenantBean> dbTenantBeans = plSqlToolQueryService.getTenantsByUserId(user_id,tokencode,db_type);
            respInfo.setRespResult("1");
            respInfo.setRespData(dbTenantBeans);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            List<DBTenantBean> tabTenantBeans = new ArrayList<>();
            respInfo.setRespData(tabTenantBeans);
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc(e.getMessage());
        }
        return respInfo;
    }
    /**
     * 根据数据库类型获取资源
     * @param db_type
     * @return
     */
    @RequestMapping(value = "/queryResource/{db_type}", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo queryResource(@PathVariable String db_type){
        RespInfo respInfo = new RespInfo();
        try {
            Map<String,List<DBResurceBean>> map = plSqlToolQueryService.queryResource(db_type);
            respInfo.setRespData(map);
            respInfo.setRespResult("1");
        }catch (Exception e){
            log.error(e.getMessage(),e);
            Map<String,List<DBResurceBean>> stringListMap = new HashMap<>();
            respInfo.setRespData(stringListMap);
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc(e.getMessage());
        }
        return respInfo;
    }

    /**
     * 根据租户 用户 数据库类型获取资源
     * @param tenant_id
     * @param user_id
     * @param db_type
     * @return
     */
    @RequestMapping(value = "/queryTenantResouce/{tenant_id}/{user_id}/{db_type}", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo queryTenantResouce(@PathVariable String tenant_id,@PathVariable String user_id,@PathVariable String db_type){
        RespInfo respInfo = new RespInfo();
        if(user_id==null||user_id.equals("")||tenant_id==null||tenant_id.equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("租户id或用户id为空");
            return respInfo;
        }
        try {
            List<DataDispBean> list = new ArrayList<>();
            List<DataDispBean> dispose_list = plSqlToolQueryService.queryTenantResouce(tenant_id,user_id);
            if(!dispose_list.isEmpty()){
                for (int i=0;i<dispose_list.size();i++){
                    if(dispose_list.get(i).getEntity_type().equalsIgnoreCase(db_type)){
                        list.add(dispose_list.get(i));
                    }
                }
            }
            respInfo.setRespData(list);
            respInfo.setRespResult("1");
            respInfo.setRespErrorDesc("");
        }catch (Exception e){
            log.error(e.getMessage(),e);
            List<DataDispBean> list = new ArrayList<>();
            respInfo.setRespData(list);
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc(e.getMessage());
        }
        return respInfo;
    }

    /**
     * 根据租户 资源获取连接
     * @param tenant_id
     * @param resource_id
     * @return
     */
    @RequestMapping(value = "/queryTenantConn/{tenant_id}/{resource_id}", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo queryTenantConn(@PathVariable String tenant_id,@PathVariable String resource_id){
        RespInfo respInfo = new RespInfo();
        if(tenant_id==null||tenant_id.equals("")||resource_id==null||resource_id.equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("租户id或资源id为空");
            return respInfo;
        }
        try{
            List<com.newland.edc.cct.dataasset.entity.model.javabean.DataDispBean> list = plSqlToolQueryService.queryTenantConn(tenant_id,resource_id);
            respInfo.setRespData(list);
            respInfo.setRespResult("1");
            respInfo.setRespErrorCode("1");
            respInfo.setRespErrorDesc("");
        }catch (Exception e){
            log.error(e.getMessage(),e);
            List<com.newland.edc.cct.dataasset.entity.model.javabean.DataDispBean> list = new ArrayList<>();
            respInfo.setRespData(list);
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc(e.getMessage());
        }
        return respInfo;
    }

    @RequestMapping(value = "/queryTenantsByUserId/{user_id}", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo queryTenantsByUserId(@PathVariable String user_id){
        RespInfo respInfo = new RespInfo();
        if(StringUtils.isBlank(user_id)){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("用户id为空");
            return respInfo;
        }
        try {
            List<DBTenantBean> dbTenantBeans = plSqlToolQueryService.queryTenantPub(user_id);
            respInfo.setRespResult("1");
            respInfo.setRespData(dbTenantBeans);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc(e.getMessage());
        }
        return respInfo;
    }

    @RequestMapping(value = "/queryResourceAndConn/{tenant_id}/{db_type}", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo queryResourceAndConn(@PathVariable String tenant_id,@PathVariable String db_type){
        RespInfo respInfo = new RespInfo();
        if(StringUtils.isBlank(tenant_id)||StringUtils.isBlank(db_type)){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("租户id或数据库类型为空");
            return respInfo;
        }
        try {
            List<DBResurceBean> dbResurceBeans = plSqlToolQueryService.queryResourceAndConn(tenant_id,db_type,null);
            respInfo.setRespResult("1");
            respInfo.setRespData(dbResurceBeans);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc(e.getMessage());
        }
        return respInfo;
    }

    /**
     * 获取项目部署地址的配置信息
     * @return
     */
    @RequestMapping(value = "/getResAddressVersion", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo getResAddressVersion(){
        RespInfo respInfo = new RespInfo();
        try{
            ResAddress resAddress = plSqlToolQueryService.getCurrentResAddressVersion();
            respInfo.setRespData(resAddress);
            respInfo.setRespResult("1");
            respInfo.setRespErrorCode("1");
            respInfo.setRespErrorDesc("");
        }catch(Exception e){
            log.error(e.getMessage(),e);
            respInfo.setRespData("");
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc(e.getMessage());
        }
        return respInfo;
    }

    /**
     * 语法、执行权限校验接口
     * @param testGrammarBean
     * @return
     */
    @RequestMapping(value = "/testGrammar", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo testGrammar(@RequestBody TestGrammarBean testGrammarBean) {
        RespInfo respInfo = new RespInfo();
        if (testGrammarBean.getSql().equals("")) {
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("执行语句为空");
            return respInfo;
        }
        if (testGrammarBean.getDbType().equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("执行数据库类型为空");
            return respInfo;
        }
        if (testGrammarBean.getTenant_id().equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("租户id为空");
            return respInfo;
        }
//        if (testGrammarBean.getUser_id().equals("")){
//            respInfo.setRespResult("0");
//            respInfo.setRespErrorCode("0");
//            respInfo.setRespErrorDesc("用户id为空");
//            return respInfo;
//        }
//        if (testGrammarBean.getConn_id().equals("")){
//            respInfo.setRespResult("0");
//            respInfo.setRespErrorCode("0");
//            respInfo.setRespErrorDesc("连接id为空");
//            return respInfo;
//        }
        return plSqlToolTestService.grammarAdapter(testGrammarBean);
    }

    /**
     * 查询临时表信息
     * @param temporaryTabInfo
     * @return
     */
    @RequestMapping(value = "/getTemporaryInfo", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo getTemporaryInfo(@RequestBody TemporaryTabInfo temporaryTabInfo) {
        RespInfo respInfo = new RespInfo();
        try {
            List<TemporaryTabInfo> list = plSqlToolQueryService.getTemporaryInfo(temporaryTabInfo);
            if(list==null){
                list = new ArrayList<>();
            }
            List<TabInfoBean> tabInfoBeanList = plSqlToolQueryService.trans(list);
            respInfo.setRespData(tabInfoBeanList);
            respInfo.setRespResult("1");
        }catch (Exception e ){
            log.error(e.getMessage(),e);
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc(e.getMessage());
            return  respInfo;
        }
        return respInfo;
    }

    /**
     * 查询临时表信息分页版
     * @param temporaryReq
     * @return
     */
    @RequestMapping(value = "/getTemporaryInfoByPage", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo getTemporaryInfoByPage(@RequestBody TemporaryReq temporaryReq) {
        RespInfo respInfo = new RespInfo();
        try{
            List<TemporaryTabInfo> list = plSqlToolQueryService.getTemporaryInfo(temporaryReq);
            if(list==null){
                list = new ArrayList<>();
                respInfo.setDataTotalCount(0);
            }else{
                int num = plSqlToolQueryService.getTemporaryCount(temporaryReq);
                respInfo.setDataTotalCount(num);
            }
            List<PLSqlToolTable> tabInfoBeanList = plSqlToolQueryService.transPLSqlToolTable(list);
            respInfo.setRespData(tabInfoBeanList);
            respInfo.setRespResult("1");
        }catch (Exception e){
            log.error(e.getMessage(),e);
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc(e.getMessage());
            return  respInfo;
        }
        return respInfo;
    }

    /**
     * 删除临时表
     * @param temporaryReq
     * @return
     */
    @RequestMapping(value = "/delTemporaryInfo", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo delTemporaryInfo(@RequestBody TemporaryReq temporaryReq){
        RespInfo respInfo = new RespInfo();
        if(StringUtils.isBlank(temporaryReq.getTab_id())){
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc("临时表tab_id为空");
            return respInfo;
        }
        if(StringUtils.isBlank(temporaryReq.getConn_id())){
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc("连接id为空");
            return respInfo;
        }
        if(StringUtils.isBlank(temporaryReq.getDb_type())){
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc("数据库类型为空");
            return respInfo;
        }
        try {
            List<TemporaryTabInfo> temporaryTabInfos = plSqlToolQueryService.getTemporaryInfo(temporaryReq);
            if(temporaryTabInfos.isEmpty()){
                respInfo.setRespResult("0");
                respInfo.setRespErrorDesc("该对象已被删除,无法获取");
                return respInfo;
            }
            TemporaryTabInfo temporaryTabInfo = temporaryTabInfos.get(0);
            temporaryReq.setTab_name(temporaryTabInfo.getTab_name());
            plSqlToolExecuteService.dropExecute(temporaryReq);
            plSqlToolQueryService.deleteTemporaryCol(temporaryTabInfo.getTab_id());
            plSqlToolQueryService.deleteTemporaryTab(temporaryTabInfo);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc(e.getMessage());
        }
        return respInfo;
    }

    /**
     * 查询标签卡片
     * @param tabInfo
     * @return
     */
    @RequestMapping(value = "/queryTabInfo", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo queryTabInfo(@RequestBody TabInfo tabInfo) {
        RespInfo respInfo = new RespInfo();
        if(tabInfo.getTab_id() == null || tabInfo.getTab_id().trim().equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("tab_id为空");
            return respInfo;
        }

        try{
            TabInfo respTabInfo = plSqlToolQueryService.queryTabInfo(tabInfo);
            respInfo.setRespData(respTabInfo);
            respInfo.setRespResult("1");
        }catch (Exception e){
            log.error(e.getMessage(),e);
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc(e.getMessage());
            return  respInfo;
        }
        return respInfo;
    }

    /**
     * 查询标签卡片分页版
     * @param tabInfoReq
     * @return
     */
    @RequestMapping(value = "/queryTabInfoListByPage", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo queryTabInfoListByPage(@RequestBody TabInfoReq tabInfoReq){
        return plSqlToolQueryService.queryTabInfoListByPage(tabInfoReq);
    }

    /**
     * 保存标签卡片
     * @param tabInfo
     * @return
     */
    @RequestMapping(value = "/saveTabInfo", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo saveTabInfo(@RequestBody TabInfo tabInfo) {
        RespInfo respInfo = new RespInfo();
        try{
            plSqlToolQueryService.saveTabInfo(tabInfo);
            tabInfo.setSqlstring(null);
            respInfo.setRespData(tabInfo);
            respInfo.setRespResult("1");
        }catch (Exception e){
            log.error(e.getMessage(),e);
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc(e.getMessage());
            return  respInfo;
        }
        return respInfo;
    }

    /**
     * 删除标签卡片
     * @param tabInfo
     * @return
     */
    @RequestMapping(value = "/deleteTabInfo", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo deleteTabInfo(@RequestBody TabInfo tabInfo) {
        RespInfo respInfo = new RespInfo();
        if(tabInfo.getTab_id() == null || tabInfo.getTab_id().trim().equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("tab_id为空");
            return respInfo;
        }
        try{
            plSqlToolQueryService.deleteTabInfo(tabInfo);
            respInfo.setRespResult("1");
        }catch (Exception e){
            log.error(e.getMessage(),e);
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc(e.getMessage());
            return  respInfo;
        }
        return respInfo;
    }

    /**
     * 任务日志提取
     * @param executeLog
     * @return
     */
    @RequestMapping(value = "/queryExecuteLogs", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo queryExecuteLogs(@RequestBody ExecuteLog executeLog) {
        RespInfo respInfo = new RespInfo();
        if(executeLog.getUser_id().equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("用户id为空");
            return respInfo;
        }
        try {
            List<ExecuteGroupLog> executeGroupLogs = this.plSqlToolQueryService.queryExecuteLogs(executeLog);
            respInfo.setRespResult("1");
            respInfo.setRespData(executeGroupLogs);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc(e.getMessage());
            return  respInfo;
        }
        return respInfo;
    }

    /**
     * 根据实体id获取实体
     * @param tabId
     * @return
     */
    @RequestMapping(value = "/getTabEntityInfo/{tabId}", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo getTabEntityInfo(@PathVariable String tabId) {
        RespInfo respInfo = new RespInfo();

        try{
            TabInfoBean tabInfoBean = plSqlToolQueryService.getTabEntityInfo(tabId);
            respInfo.setRespData(tabInfoBean);
            respInfo.setRespResult("1");
        }catch (Exception e){
            log.error(e.getMessage(),e);
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc(e.getMessage());
            return  respInfo;
        }
        return respInfo;
    }

    /**
     * 异步执行适配器
     * @param testGrammarBean
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "/executeAdapter", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo executeAdapter(@RequestBody TestGrammarBean testGrammarBean) throws Exception{
        RespInfo respInfo = new RespInfo();
        if(testGrammarBean.getDbType().equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("数据库类型为空");
            return respInfo;
        }
        if(testGrammarBean.getResource_id().equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("资源id为空");
            return respInfo;
        }
        if(testGrammarBean.getConn_id().equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("连接id为空");
            return respInfo;
        }
        if(testGrammarBean.getTenant_id().equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("租户id为空");
            return respInfo;
        }
        if(testGrammarBean.getUser_id().equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("用户id为空");
            return respInfo;
        }
        try {
            plSqlToolTaskService.executeAdapter(testGrammarBean);
            respInfo.setRespResult("1");
            respInfo.setRespData("请等待任务执行完成");
        }catch (Exception e){
            log.error(e.getMessage(),e);
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc(e.getMessage());
        }
        return respInfo;
    }

    /**
     * 异步执行入口
     * @param testGrammarBean
     * @return
     * @throws Exception
     */
    @RequestMapping(value = "/asyexecute", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo asyexecute(@RequestBody TestGrammarBean testGrammarBean) throws Exception{
        RespInfo respInfo = new RespInfo();
        if(testGrammarBean.getDbType().equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("数据库类型为空");
            return respInfo;
        }
        if(testGrammarBean.getResource_id().equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("资源id为空");
            return respInfo;
        }
        if(testGrammarBean.getConn_id().equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("连接id为空");
            return respInfo;
        }
        if(testGrammarBean.getTenant_id().equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("租户id为空");
            return respInfo;
        }
        if(testGrammarBean.getUser_id().equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("用户id为空");
            return respInfo;
        }
        try {
            plSqlToolTaskService.asyexecute(testGrammarBean);
            respInfo.setRespResult("1");
            respInfo.setRespData("请等待任务执行完成");
        }catch (Exception e){
            if(e instanceof TimeoutException){
                respInfo.setRespResult("0");
                respInfo.setRespErrorCode("0");
                respInfo.setRespErrorDesc("任务正在执行，请等候");
                return respInfo;
            }
            log.error(e.getMessage(),e);
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc(e.getMessage());
        }
        return respInfo;
    }

    /**
     * 同步任务入口
     * @param testSqlBean
     * @return
     */
    @RequestMapping(value = "/synexecute", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo synexecute(@RequestBody TestSqlBean testSqlBean) {
        RespInfo respInfo = new RespInfo();
        if(testSqlBean.getGroup_id().equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("sql："+testSqlBean.getSql()+"，因无法获取执行组id，任务无法提交执行");
            return respInfo;
        }
        if(testSqlBean.getTask_id().equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("sql："+testSqlBean.getSql()+"，因无法获取任务id，任务无法提交执行");
            return respInfo;
        }
        if(testSqlBean.getDbType().equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("数据库类型为空");
            return respInfo;
        }
        if(testSqlBean.getSql().equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("执行语句为空");
            return respInfo;
        }
        if(testSqlBean.getResource_id().equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("资源id为空");
            return respInfo;
        }
        if(testSqlBean.getConn_id().equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("连接id为空");
            return respInfo;
        }
        if(testSqlBean.getTenant_id().equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("租户id为空");
            return respInfo;
        }
        if(testSqlBean.getUser_id().equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("用户id为空");
            return respInfo;
        }
        try{
            com.newland.edc.cct.plsqltool.interactive.model.javabean.DgwSqlToolResult dgwSqlToolResult = plSqlToolTaskService.synexecute(testSqlBean);
            respInfo.setRespResult("1");
            respInfo.setRespData(dgwSqlToolResult);
        } catch (Exception e){
            log.error(e.getMessage(),e);
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc(e.getMessage());
        }
        return respInfo;
    }

    /**
     * 关闭连接
     * @param testSqlBean
     * @return
     */
    @RequestMapping(value = "/close", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo close(@RequestBody TestSqlBean testSqlBean){
        RespInfo respInfo = new RespInfo();
        if(StringUtils.isBlank(testSqlBean.getGroup_id())){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("执行失败，任务组id为空");
            return respInfo;
        }
        if(StringUtils.isBlank(testSqlBean.getTask_id())){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("执行失败，任务id为空");
            return respInfo;
        }
        if(StringUtils.isBlank(testSqlBean.getDbType())){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("执行失败，数据库类型为空");
            return respInfo;
        }
        try {
            String node_id = this.plSqlToolQueryService.queryExecuteIp(testSqlBean.getGroup_id());
            String errMsg = iplSqlToolCloseConnService.sendCloseMessage(node_id,testSqlBean);
            if(StringUtils.isNotBlank(errMsg)){
                respInfo.setRespResult("0");
                respInfo.setRespErrorCode("0");
                respInfo.setRespErrorDesc(errMsg);
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc(e.getMessage());
        }
        return respInfo;
    }

    /**
     * 关闭连接
     * @param testSqlBean
     * @return
     */
    @RequestMapping(value = "/closeConn", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo closeConn(@RequestBody TestSqlBean testSqlBean) {
        RespInfo respInfo = new RespInfo();
        if(testSqlBean.getGroup_id().equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("执行失败，任务组id为空");
            return respInfo;
        }
        try {
            String ip = this.plSqlToolQueryService.queryExecuteIp(testSqlBean.getGroup_id());
            String hostip = this.plSqlToolQueryService.getIp();
            //创建的连接刚好在本机
            if(ip.equalsIgnoreCase(hostip)){
                if(DataCatch.getConMap().get(testSqlBean.getGroup_id())!=null){
                    DataCatch.getConMap().get(testSqlBean.getGroup_id()).close();
                    respInfo.setRespResult("1");
                }
            }else{//创建的连接可能不在本机
                if(RestFulServiceUtils.getconfig(ip)!=null&&!RestFulServiceUtils.getconfig(ip).equals("")){
                    String url = RestFulServiceUtils.getconfig(ip)+"/v1/querydata/closeConn";
                    String reqData = JSON.toJSONString(testSqlBean);
                    log.info("请求集群服务url："+url);
                    log.info("请求集群服务报文："+reqData);
                    JSONObject respData = HttpUtil.httpRequest(url,"POST",reqData);
                    log.info("返回结果："+respData.toString());
                    respInfo = JSON.toJavaObject(respData,RespInfo.class);
                    return respInfo;
                }else{
                    respInfo.setRespResult("0");
                    respInfo.setRespErrorDesc("当前机器为"+hostip+"未在配置文件中添加"+ip+"无法提交关闭请求");
                }
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc(e.getMessage());
        }
        return respInfo;
    }

    /**
     * 关闭statement
     * @param testSqlBean
     * @return
     */
    @RequestMapping(value = "/closeStatement", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo closeStatement(@RequestBody TestSqlBean testSqlBean) {
        RespInfo respInfo = new RespInfo();
        if(testSqlBean.getTask_id().equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("执行失败，任务id为空");
            return respInfo;
        }
        try {
            String ip = this.plSqlToolQueryService.queryExecuteIp(testSqlBean.getGroup_id());
            String hostip = this.plSqlToolQueryService.getIp();
            //创建的连接刚好在本机
            if(ip.equalsIgnoreCase(hostip)){
                if(DataCatch.getPsMap().get(testSqlBean.getTask_id())!=null){
                    DataCatch.getPsMap().get(testSqlBean.getTask_id()).cancel();
                    respInfo.setRespResult("1");
                }else{
                    //该statement可能执行完成被关闭
                    respInfo.setRespResult("1");
                    respInfo.setRespData("该statement可能执行完成被关闭");
                }
            }else {//创建的连接可能不在本机
                if(RestFulServiceUtils.getconfig(ip)!=null&&!RestFulServiceUtils.getconfig(ip).equals("")){
                    String url = RestFulServiceUtils.getconfig(ip)+"/v1/querydata/closeStatement";
                    String reqData = JSON.toJSONString(testSqlBean);
                    log.info("请求集群服务url："+url);
                    log.info("请求集群服务报文："+reqData);
                    JSONObject respData = HttpUtil.httpRequest(url,"POST",reqData);
                    log.info("返回结果："+respData.toString());
                    respInfo = JSON.toJavaObject(respData,RespInfo.class);
                    return respInfo;
                }else{
                    respInfo.setRespResult("0");
                    respInfo.setRespErrorDesc("当前机器为"+hostip+"未在配置文件中添加"+ip+"无法提交关闭请求");
                }
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc(e.getMessage());
        }
        return respInfo;
    }

    /**
     * 创建导出任务
     * @param exportLogBean
     * @return
     */
    @RequestMapping(value = "/createExportTask", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo createExportTask(@RequestBody ExportLog exportLogBean) {
        RespInfo respInfo = new RespInfo();
        if(exportLogBean==null){
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc("请求内容为空");
            return respInfo;
        }
        if(StringUtils.isBlank(exportLogBean.getUser_id())){
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc("用户id为空");
            return respInfo;
        }
        if(StringUtils.isBlank(exportLogBean.getTenant_id())){
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc("租户id为空");
            return respInfo;
        }
        if(StringUtils.isBlank(exportLogBean.getResource_id())){
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc("资源id为空");
            return respInfo;
        }
        if(StringUtils.isBlank(exportLogBean.getConn_id())){
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc("连接id为空");
            return respInfo;
        }
        if(StringUtils.isBlank(exportLogBean.getDb_type())){
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc("执行库类型为空");
            return respInfo;
        }
        if(StringUtils.isBlank(exportLogBean.getSqlstring())){
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc("执行语句为空");
            return respInfo;
        }
        try {
            plSqlToolTaskService.createExportTask(exportLogBean);
            respInfo.setRespResult("1");
        }catch (Exception e){
            log.error(e.getMessage(),e);
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc(e.getMessage());
        }
        return respInfo;
    }

    /**
     * 查询导出任务
     * @param exportLogBean
     * @return
     */
    @RequestMapping(value = "/queryExportTask", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo queryExportTask(@RequestBody ExportLogReq exportLogBean) {
        RespInfo respInfo = new RespInfo();
        if(exportLogBean==null){
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc("请求内容为空");
            return respInfo;
        }
        try {
            ExportLogReq exportLogBeans = this.plSqlToolQueryService.queryExportLog(exportLogBean);
            respInfo.setRespResult("1");
            respInfo.setRespData(exportLogBeans);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc(e.getMessage());
        }
        return respInfo;
    }

    /**
     * 删除导出任务
     * @param exportLogReq
     * @return
     */
    @RequestMapping(value = "/deleteExportTask", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo deleteExportTask(@RequestBody ExportLogReq exportLogReq) {
        RespInfo respInfo = new RespInfo();
        if(exportLogReq==null){
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc("请求内容为空");
            return respInfo;
        }
        if(StringUtils.isBlank(exportLogReq.getExport_id())){
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc("请求执行删除的导出流水id为空");
            return respInfo;
        }
        try {
            plSqlToolTaskService.deleteExportTask(exportLogReq);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc(e.getMessage());
        }
        return respInfo;
    }

    /**
     * 插入下载日志
     * @param exportDownloadLog
     * @return
     */
    @RequestMapping(value = "/insertExportDownloadLog", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo insertExportDownloadLog(@RequestBody ExportDownloadLog exportDownloadLog) {
        RespInfo respInfo = new RespInfo();
        if(exportDownloadLog==null){
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc("请求内容为空，无法执行导出日志插入");
            return respInfo;
        }
        try {
            this.plSqlToolQueryService.insertExportDownloadLog(exportDownloadLog);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc(e.getMessage());
        }
        return respInfo;
    }

    /**
     * 执行计划
     * @param testGrammarBean
     * @return
     */
    @RequestMapping(value = "/executeExplain", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo executeExplain(@RequestBody TestGrammarBean testGrammarBean){
        RespInfo respInfo = new RespInfo();
        if (StringUtils.isBlank(testGrammarBean.getSql())) {
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("执行语句为空");
            return respInfo;
        }
        if (StringUtils.isBlank(testGrammarBean.getDbType())){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("执行数据库未空");
            return respInfo;
        }
        if (StringUtils.isBlank(testGrammarBean.getTenant_id())){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("租户id为空");
            return respInfo;
        }
        if (StringUtils.isBlank(testGrammarBean.getResource_id())){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("资源id为空");
            return respInfo;
        }
        if (StringUtils.isBlank(testGrammarBean.getConn_id())){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("连接id为空");
            return respInfo;
        }
        for (int i=0;i<testGrammarBean.getTestSqlBeans().size();i++){
            testGrammarBean.getTestSqlBeans().get(i).setDbType(testGrammarBean.getDbType());
            testGrammarBean.getTestSqlBeans().get(i).setTenant_id(testGrammarBean.getTenant_id());
            testGrammarBean.getTestSqlBeans().get(i).setResource_id(testGrammarBean.getResource_id());
            testGrammarBean.getTestSqlBeans().get(i).setConn_id(testGrammarBean.getConn_id());
            testGrammarBean.getTestSqlBeans().get(i).setUser_id(testGrammarBean.getUser_id());
            testGrammarBean.getTestSqlBeans().get(i).setSys_id(testGrammarBean.getSys_id());
        }
        try {
            List<ExplainInfo> explainInfos = plSqlToolExecuteService.executeExplain(testGrammarBean.getTestSqlBeans(),testGrammarBean.getConn_id());
            respInfo.setRespData(explainInfos);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc(e.getMessage());
        }
        return respInfo;
    }

    /**
     * 查询ftp配置
     * @return
     */
    @RequestMapping(value = "/queryFtpConfig", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo queryFtpConfig(){
        RespInfo respInfo = new RespInfo();
        try {
            FtpConfig ftpConfig = new FtpConfig();
            List<FtpConfig> ftpConfigs = this.plSqlToolQueryService.queryFtpConfig(ftpConfig);
            respInfo.setRespData(ftpConfigs);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc(e.getMessage());
        }
        return respInfo;
    }

    /**
     * 查询文件同步信息
     * @param ftpFileSynchronousReq
     * @return
     */
    @RequestMapping(value = "/queryFtpFileSynchronousLog", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo queryFtpFileSynchronousLog(@RequestBody FtpFileSynchronousReq ftpFileSynchronousReq){
        RespInfo respInfo = new RespInfo();
        if(StringUtils.isBlank(ftpFileSynchronousReq.getExport_id())){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("未选择查询记录");
            return respInfo;
        }
        try {
            FtpSynchronousLog ftpSynchronousLog = new FtpSynchronousLog();
            ftpSynchronousLog.setExport_id(ftpFileSynchronousReq.getExport_id());
            List<FtpSynchronousLog> ftpSynchronousLogs = this.plSqlToolQueryService.queryFtpSynchronousLog(ftpSynchronousLog);
            respInfo.setRespData(ftpSynchronousLogs);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc(e.getMessage());
        }
        return respInfo;
    }

    /**
     * 执行文件同步
     * @param ftpFileSynchronousReq
     * @return
     */
    @RequestMapping(value = "/doFtpFileSynchronous", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo doFtpFileSynchronous(@RequestBody FtpFileSynchronousReq ftpFileSynchronousReq){
        RespInfo respInfo = new RespInfo();
        if(StringUtils.isBlank(ftpFileSynchronousReq.getExport_id())){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("未选择同步记录");
            return respInfo;
        }
        if(StringUtils.isBlank(ftpFileSynchronousReq.getFile_name())){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("未填写同步文件名");
            return respInfo;
        }
        if(StringUtils.isBlank(ftpFileSynchronousReq.getFtp_config_id())){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("未选择同步ftp");
            return respInfo;
        }
        try {
            FtpSynchronousLog ftpSynchronousLog = plSqlToolTaskService.doFtpFileSynchronous(ftpFileSynchronousReq);
            respInfo.setRespResult("1");
            respInfo.setRespData(ftpSynchronousLog);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc(e.getMessage());
        }
        return respInfo;
    }

    /**
     * 获取本机资源集群版
     * @param resource_id
     * @return
     */
    @GetMapping(value = "/getEnvTypeByResourceId/{resource_id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo getEnvTypeByResourceId(@PathVariable String resource_id) {
        RespInfo respInfo = new RespInfo();
        try {
            respInfo.setRespResult(RespInfo.RESP_SUCCESS);
            respInfo.setRespData(DataSourceAccess.getEnvTypeByResourceId(resource_id));
        }catch (Exception e){
            log.error(e.getMessage(),e);
            respInfo.setRespResult(RespInfo.RESP_FAILURE);
            respInfo.setRespErrorDesc(e.getMessage());
        }
        return respInfo;
    }

    @RequestMapping(value = "/getServiceConfig", method = { RequestMethod.GET, RequestMethod.POST }, produces = MediaType.APPLICATION_JSON_VALUE)
    public RespInfo getServiceConfig(@RequestBody List<String> keyList){
        RespInfo respInfo = new RespInfo();
        Map<String, String> respMap = new HashMap<>();
        for (String s : keyList) {
            String config = RestFulServiceUtils.getconfig(s);
            respMap.put(s, config);
        }
        respInfo.setRespData(respMap);
        respInfo.setRespResult("1");

        return respInfo;
    }
}
