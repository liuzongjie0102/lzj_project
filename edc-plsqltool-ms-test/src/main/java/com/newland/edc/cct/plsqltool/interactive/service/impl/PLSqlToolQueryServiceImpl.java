package com.newland.edc.cct.plsqltool.interactive.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.newland.bd.ms.core.model.RespInfo;
import com.newland.bi.util.logger.BaseLogger;
import com.newland.bi.webservice.common.MessageHandler;
import com.newland.bi.webservice.model.User20;
import com.newland.edc.cct.dataasset.dispose.model.DataAssetDisposeServiceReqData;
import com.newland.edc.cct.dataasset.dispose.model.DataAssetDisposeServiceRequestObject;
import com.newland.edc.cct.dataasset.dispose.model.DataAssetDisposeServiceRespData;
import com.newland.edc.cct.dataasset.dispose.model.DataAssetDisposeServiceResponseObject;
import com.newland.edc.cct.dataasset.dispose.model.javabean.DataDispBean;
import com.newland.edc.cct.dataasset.dispose.service.IDataAssetEntityDisposeService;
import com.newland.edc.cct.dataasset.entity.model.DataAssetServiceReqData;
import com.newland.edc.cct.dataasset.entity.model.DataAssetServiceRequestObject;
import com.newland.edc.cct.dataasset.entity.model.DataAssetServiceRespData;
import com.newland.edc.cct.dataasset.entity.model.DataAssetServiceResponseObject;
import com.newland.edc.cct.dataasset.entity.model.javabean.*;
import com.newland.edc.cct.dataasset.entity.service.IDataAssetEntityService;
import com.newland.edc.cct.dataasset.subscribe.model.DataAssetSubscribeServiceReqData;
import com.newland.edc.cct.dataasset.subscribe.model.DataAssetSubscribeServiceRequestObject;
import com.newland.edc.cct.dataasset.subscribe.model.DataAssetSubscribeServiceRespData;
import com.newland.edc.cct.dataasset.subscribe.model.DataAssetSubscribeServiceResponseObject;
import com.newland.edc.cct.dataasset.subscribe.service.IDataAssetSubscribeService;
import com.newland.edc.cct.dgw.utils.rest.RestClientUtil;
import com.newland.edc.cct.dgw.utils.rest.RestFulServiceUtils;
import com.newland.edc.cct.dgw.utils.token.TokenUtil;
import com.newland.edc.cct.frame.soap.model.*;
import com.newland.edc.cct.frame.soap.model.javabean.AuthReqBean;
import com.newland.edc.cct.frame.soap.model.javabean.TenantBean;
import com.newland.edc.cct.frame.soap.service.ICctAuthService;
import com.newland.edc.cct.plsqltool.interactive.common.ResAddress;
import com.newland.edc.cct.plsqltool.interactive.dao.PLSqlToolBaseDao;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.*;
import com.newland.edc.cct.plsqltool.interactive.service.IPLSqlToolCloseConnService;
import com.newland.edc.cct.plsqltool.interactive.service.PLSqlToolQueryService;
import com.newland.edc.cct.plsqltool.interactive.util.ClassUtil;
import com.newland.edc.cct.plsqltool.interactive.util.DataCatch;
import com.newland.edc.cct.plsqltool.interactive.util.HttpUtil;
import com.newland.edc.pub.system.conn.model.ConnBean;
import com.newland.edc.pub.system.conn.model.ParamConn;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Repository("com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolQueryServiceImpl")
public class PLSqlToolQueryServiceImpl implements PLSqlToolQueryService {

    private static BaseLogger log = (BaseLogger) BaseLogger.getLogger(PLSqlToolQueryServiceImpl.class);

    @Resource(name = "com.newland.edc.cct.plsqltool.interactive.dao.impl.PLSqlToolBaseDaoImpl")
    private PLSqlToolBaseDao plSqlToolBaseDao;

    @Resource(name = "com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolCloseConnServiceImpl")
    private IPLSqlToolCloseConnService iplSqlToolCloseConnService;

    @Resource
    private ResAddress resAddress;

    @Autowired
    private IDataAssetEntityService iDataAssetEntityService;

    @Autowired
    private IDataAssetSubscribeService iDataAssetSubscribeService;

    @Autowired
    private IDataAssetEntityDisposeService iDataAssetEntityDisposeService;

    @Autowired
    private ICctAuthService iCctAuthService;

    @Override
    public DataAssetEntity queryEntityData(String user_id, String tenant_id) {
        DataAssetEntity dataAssetEntity = new DataAssetEntity();
        try{
            TabReqBean tabReqBean = new TabReqBean();
            tabReqBean.setIs_query_detail("1");
            tabReqBean.setTenant_id(tenant_id);
            log.info("正在加载租户信息，tenant_id："+tenant_id);
            if(user_id!=null&&!user_id.equals("")){
                tabReqBean.setUser_id(user_id);
            }
            DataAssetSubscribeServiceRequestObject requestObject = new DataAssetSubscribeServiceRequestObject();
            DataAssetSubscribeServiceReqData reqData = new DataAssetSubscribeServiceReqData();
            reqData.setTab_info_req_bean(tabReqBean);
            int statPage = 1;
            List<TabInfoBean> definitions = new ArrayList<>();//定义集合
            List<TabInfoBean> subscriptions = new ArrayList<>();//订阅集合
            int entitycount = 0;
            while (statPage>0){
                //设置请求对象的请求数据
                MessageHandler.newInstance().setReqData(requestObject, reqData);
                //如果为分页查询,设置分页查询的起始行
                MessageHandler.newInstance().setStart(requestObject,statPage+"");
                //如果为分页查询,设置分页查询的每页纪录数
                MessageHandler.newInstance().setPageCount(requestObject,"300");
                if(user_id!=null&&!user_id.equals("")){
                    User20 user = new User20();
                    user.setClientId(user_id);
                    requestObject.getHeaderReq().setUser(user);
                }
                DataAssetSubscribeServiceResponseObject responseObject = iDataAssetSubscribeService.querySubEntityList(requestObject);
                DataAssetSubscribeServiceRespData respData = (DataAssetSubscribeServiceRespData) MessageHandler.newInstance().getRespData(responseObject);
                List<TabInfoBean> resList = respData.getTab_info_list();
                if(!resList.isEmpty()){
                    entitycount+=resList.size();
                    if(resList.size()==300){
                        statPage++;
                    }else{
                        statPage = 0;
                    }
                    for (int i=0;i<resList.size();i++){
                        TabInfoBean tabInfoBean = resList.get(i);
                        dataAssetEntity.tabmap.put(tabInfoBean.getTab_chs_name(),tabInfoBean);
                        if(tabInfoBean.getTenant_id().equals(tenant_id)){
                            definitions.add(tabInfoBean);
                        }else{
                            subscriptions.add(tabInfoBean);
                        }
                    }
                }else{
                    statPage = 0;
                }
            }
            dataAssetEntity.setDefinitions(definitions);
            dataAssetEntity.setSubscriptions(subscriptions);
            List<TemporaryTabInfo> temporaryTabInfos = getTemporaryInfo(tenant_id,user_id);
            if(!temporaryTabInfos.isEmpty()){

            }else{
                temporaryTabInfos = new ArrayList<>();
            }
            List<TabInfoBean> tabInfoBeanList = trans(temporaryTabInfos);
            dataAssetEntity.setTemporarytabs(tabInfoBeanList);
            log.info("租户："+tenant_id+"实体信息加载完成，查询资产实体"+entitycount+"个，其中订阅实体："+subscriptions.size()+"，定义实体："+definitions.size()+"，加载数据临时表信息："+tabInfoBeanList.size());
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
        return dataAssetEntity;
    }

    @Override
    public List<TemporaryTabInfo> getTemporaryInfo(String tenant_id,String user_id){
        if(tenant_id.equals("")&&user_id.equals("")){
            List<TemporaryTabInfo> list = new ArrayList<>();
            log.info("租户id 用户id为空");
            return list;
        }
        List<TemporaryTabInfo> list = plSqlToolBaseDao.selectTemporaryInfo(tenant_id,user_id);
        if(list==null){
            list = new ArrayList<>();
        }
        return list;
    }

    @Override
    public List<TemporaryTabInfo> getTemporaryInfo(TemporaryTabInfo temporaryTabInfo){
        if(temporaryTabInfo==null){
            List<TemporaryTabInfo> list = new ArrayList<>();
            log.info("temporaryTabInfo为空");
            return list;
        }
        List<TemporaryTabInfo> list = plSqlToolBaseDao.selectTemporaryInfo(temporaryTabInfo);
        if(list==null){
            list = new ArrayList<>();
        }
        return list;
    }

    @Override
    public List<TemporaryTabInfo> getTemporaryInfo(TemporaryReq temporaryReq){
        if(temporaryReq==null){
            List<TemporaryTabInfo> list = new ArrayList<>();
            log.info("temporaryReq为空");
            return list;
        }
        List<TemporaryTabInfo> list = plSqlToolBaseDao.selectTemporaryInfo(temporaryReq);
        if(list==null){
            list = new ArrayList<>();
        }
        return list;
    }

    @Override
    public Integer getTemporaryCount(TemporaryReq temporaryReq){
        if(temporaryReq==null){
            log.info("temporaryReq为空");
            return 0;
        }
        Integer num = plSqlToolBaseDao.getTemporaryCount(temporaryReq);
        if(num==null){
            num = 0;
        }
        return num;
    }

    @Override
    public void updateTemporaryTableName(String tenant_id,String user_id,String resource_id,String conn_id,String fromTable,String toTable) throws Exception{
        try {
            this.plSqlToolBaseDao.updateTemporaryTableName(tenant_id,user_id,resource_id,conn_id,fromTable,toTable);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
    }

    @Override
    public List<DataDispBean> queryTenantResouce(String tenant_id,String user_id){
        List<DataDispBean> dispose_list = new ArrayList<>();
        try {
            DataAssetDisposeServiceRequestObject requestObject = new DataAssetDisposeServiceRequestObject();
            DataDispBean req_bean = new DataDispBean();
            req_bean.setTenant_id(tenant_id);
            DataAssetDisposeServiceReqData reqData = new DataAssetDisposeServiceReqData();
            reqData.setDispose_req_bean(req_bean);
            MessageHandler.newInstance().setReqData(requestObject,reqData);
            User20 user = new User20();
            user.setClientId(user_id);
            requestObject.getHeaderReq().setUser(user);
            DataAssetDisposeServiceResponseObject responseObject = iDataAssetEntityDisposeService.getEntityDisposeCfg(requestObject);
            DataAssetDisposeServiceRespData respData = (DataAssetDisposeServiceRespData)MessageHandler.newInstance().getRespData(responseObject);
            dispose_list = respData.getDispose_list();
//            List<DataDispBean> entity_list = respData.getEntity_list();
            if(dispose_list==null){
                dispose_list = new ArrayList<>();
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
        return dispose_list;
    }

    @Override
    public List<com.newland.edc.cct.dataasset.entity.model.javabean.DataDispBean> queryTenantConn(String tenant_id,String resource_id){
        List<com.newland.edc.cct.dataasset.entity.model.javabean.DataDispBean> list = new ArrayList<>();
        try{
            DataAssetServiceRequestObject requestObject = new DataAssetServiceRequestObject();
            com.newland.edc.cct.dataasset.entity.model.javabean.DataDispBean req_bean = new com.newland.edc.cct.dataasset.entity.model.javabean.DataDispBean();
            req_bean.setTenant_id(tenant_id);
            req_bean.setDispose_type(resource_id);
            DataAssetServiceReqData reqData = new DataAssetServiceReqData();
            reqData.setDispose_req_bean(req_bean);
            MessageHandler.newInstance().setReqData(requestObject,reqData);
            DataAssetServiceResponseObject responseObject = iDataAssetEntityService.getResourceConn(requestObject);
            DataAssetServiceRespData respData = (DataAssetServiceRespData)MessageHandler.newInstance().getRespData(responseObject);
            list = respData.getConn_list();
            if(list==null){
                list = new ArrayList<>();
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
        }
        return list;
    }

    @Override
    public List<TabTenantBean> getTenantsByUserId(String user_id,String tokencode){
        List<TabTenantBean> list = new ArrayList<>();
        try {
            FrameReqObject reqObj = new FrameReqObject();
            FrameReqData reqData = new FrameReqData();
            FrameReqBean reqBean = new FrameReqBean();
            AuthReqBean authReqBean = new AuthReqBean();
            authReqBean.setVerify_code(tokencode);
            authReqBean.setUser_id(user_id);
            reqBean.setAuthReqBean(authReqBean);
            reqData.setReqBean(reqBean);
            MessageHandler.newInstance().setReqData(reqObj, reqData);
            FrameRespObject respObj = iCctAuthService.qryUserTenantList(reqObj);
            if (MessageHandler.newInstance().IsResponseError(respObj)) {
                throw new RuntimeException(MessageHandler.newInstance().getResponseErrorMsg(respObj));
            }
            FrameRespData respData = (FrameRespData)MessageHandler.newInstance().getRespData(respObj);
            List<TenantBean> tenantList = respData.getRespBean().getTenantList();
            if (CollectionUtils.isNotEmpty(tenantList)) {
                for (TenantBean tenant : tenantList){
                    TabTenantBean tabTenantBean = new TabTenantBean();
                    tabTenantBean.setTenant_id(tenant.getTenantId());
                    tabTenantBean.setTenant_name(tenant.getTenantName());
                    tabTenantBean.setStatus(tenant.getIs_apply());
                    list.add(tabTenantBean);
                }
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
        return list;
    }

    @Override
    public List<TabTenantBean> getTenanetList(){
        List<TabTenantBean> tenantList = new ArrayList<>();
        try{
            String url = RestFulServiceUtils.getServiceUrl("nl-edc-cct-sys-ms")+"users/tenantqeurylist";
            String reqData = "{\"auth_code\":\"nl-edc-plsql-tool\"}";
            com.alibaba.fastjson.JSONObject respData = HttpUtil.httpRequest(url,"GET",reqData);
            RespInfo respInfo = JSON.toJavaObject(respData,RespInfo.class);
//            com.newland.edc.cct.frame.soap.model.javabean.TenantBean tenantBean = new TenantBean();
            if(respData.get("respResult").equals("1")){
                List<TenantBean> tenantBeans = JSON.parseArray(JSON.toJSONString(respInfo.getRespData()),TenantBean.class);
                if(!tenantBeans.isEmpty()){
                    for (int i=0;i<tenantBeans.size();i++){
                        TabTenantBean tabTenantBean = new TabTenantBean();
                        tabTenantBean.setTenant_id(tenantBeans.get(i).getTenantId());
                        tabTenantBean.setTenant_name(tenantBeans.get(i).getTenant_ename());
                        tenantList.add(tabTenantBean);
                    }
                }
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
        return tenantList;
    }


    @Override
    public List<TabTenantBean> getTenants(){
        List<TabTenantBean> tenantList = new ArrayList<>();
        try {
            DataAssetServiceRequestObject requestObject = new DataAssetServiceRequestObject();
            DataAssetServiceReqData reqData = new DataAssetServiceReqData();
            reqData.setTab_info_req_bean(new TabReqBean());
            //设置请求对象的请求数据
            MessageHandler.newInstance().setReqData(requestObject, reqData);
            DataAssetServiceResponseObject responseObject = iDataAssetEntityService.getTabTenantList(requestObject);
            DataAssetServiceRespData respData = (DataAssetServiceRespData) MessageHandler
                    .newInstance().getRespData(responseObject);
            tenantList = respData.getTab_tenant_list();
            if(tenantList==null){
                tenantList = new ArrayList<>();
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
        return tenantList;
    }

    @Override
    public DataAssetEntity getCatchData(String tenant_id,String resource_id,String conn_id){
        DataAssetEntity returnData = new DataAssetEntity();
        String TAB_VIEW_SWITCH = RestFulServiceUtils.getconfig("TAB_VIEW_SWITCH");
        if(DataCatch.getDataAssetEntityMap().get(tenant_id)!=null){
            DataAssetEntity dataAssetEntity = DataCatch.getDataAssetEntityMap().get(tenant_id);
            List<TabInfoBean> definitions = new ArrayList<>();//定义集合
            List<TabInfoBean> subscriptions = new ArrayList<>();//订阅集合
            if(!dataAssetEntity.getDefinitions().isEmpty()){
                for (int i=0;i<dataAssetEntity.getDefinitions().size();i++){
                    TabInfoBean tabInfoBean = dataAssetEntity.getDefinitions().get(i);
                    if (tabInfoBean.getDispose_list().get(0).getDispose_type()!=null&&tabInfoBean.getDispose_list().get(0).getDispose_type().equals(resource_id)){
                        if(tabInfoBean.getDispose_list().get(0).getDb_name()!=null&&!tabInfoBean.getDispose_list().get(0).getDb_name().equals("")){
                            TabInfoBean tabInfoBean1 = new TabInfoBean();
                            if(TAB_VIEW_SWITCH!=null&&TAB_VIEW_SWITCH.equals("1")){
                                tabInfoBean1.setPhy_tab_name(transVariable(tabInfoBean.getDispose_list().get(0).getDb_name())+"."+tabInfoBean.getPhy_tab_name());
                            }else{
                                tabInfoBean1.setPhy_tab_name(tabInfoBean.getDispose_list().get(0).getDb_name()+"."+tabInfoBean.getPhy_tab_name());
                            }
                            tabInfoBean1.setDispose_list(tabInfoBean.getDispose_list());
                            tabInfoBean1.setOper_id(tabInfoBean.getOper_id());
                            tabInfoBean1.setCreate_time(tabInfoBean.getCreate_time());
                            tabInfoBean1.setTab_id(tabInfoBean.getTab_id());
                            if(tabInfoBean.getTab_col_list()!=null){
                                tabInfoBean1.setTab_col_list(tabInfoBean.getTab_col_list());
                            }
                            definitions.add(tabInfoBean1);
                        }
                    }
                }
                returnData.setDefinitions(definitions);
            }
            if(dataAssetEntity.getSubscriptions()!=null&&dataAssetEntity.getSubscriptions().size()>0){
                for (int i=0;i<dataAssetEntity.getSubscriptions().size();i++){
                    TabInfoBean tabInfoBean = dataAssetEntity.getSubscriptions().get(i);
                    if(!tabInfoBean.getDispose_list().isEmpty()){
                        if(tabInfoBean.getDispose_list().get(0).getEntity_type().equalsIgnoreCase("HIVE")){
                            if(tabInfoBean.getDispose_list().get(0).getDb_name()!=null&&!tabInfoBean.getDispose_list().get(0).getDb_name().equals("")){
                                TabInfoBean tabInfoBean1 = new TabInfoBean();
                                if(TAB_VIEW_SWITCH!=null&&TAB_VIEW_SWITCH.equals("1")){
                                    tabInfoBean1.setPhy_tab_name(transVariable(tabInfoBean.getDispose_list().get(0).getDb_name())+"."+tabInfoBean.getPhy_tab_name());
                                }else{
                                    tabInfoBean1.setPhy_tab_name(tabInfoBean.getDispose_list().get(0).getDb_name()+"."+tabInfoBean.getPhy_tab_name());
                                }
                                tabInfoBean1.setDispose_list(tabInfoBean.getDispose_list());
                                tabInfoBean1.setOper_id(tabInfoBean.getOper_id());
                                tabInfoBean1.setCreate_time(tabInfoBean.getCreate_time());
                                tabInfoBean1.setTab_id(tabInfoBean.getTab_id());
                                if(tabInfoBean.getTab_col_list()!=null){
                                    tabInfoBean1.setTab_col_list(tabInfoBean.getTab_col_list());
                                }
                                subscriptions.add(tabInfoBean1);
                            }
                        }
                    }
                }
                returnData.setSubscriptions(subscriptions);
            }
        }
        TemporaryTabInfo temporaryTabInfo = new TemporaryTabInfo();
        temporaryTabInfo.setTenant_id(tenant_id);
        temporaryTabInfo.setResource_id(resource_id);
        temporaryTabInfo.setConn_id(conn_id);
        List<TemporaryTabInfo> temporaryTabInfos = getTemporaryInfo(temporaryTabInfo);
        if(!temporaryTabInfos.isEmpty()){

        }else{
            temporaryTabInfos = new ArrayList<>();
        }
        List<TabInfoBean> tabInfoBeanList = trans(temporaryTabInfos);
        returnData.setTemporarytabs(tabInfoBeanList);
        return returnData;
    }

    public List<TabInfoBean> trans(List<TemporaryTabInfo> temporaryTabInfos){
        List<TabInfoBean> tabInfoBeans = new ArrayList<>();
        if(!temporaryTabInfos.isEmpty()){
            for (int i=0;i<temporaryTabInfos.size();i++){
                TabInfoBean tabInfoBean = new TabInfoBean();
                tabInfoBean.setTab_id(temporaryTabInfos.get(i).getTab_id());
                tabInfoBean.setTab_chs_name(temporaryTabInfos.get(i).getTab_name());
                tabInfoBean.setPhy_tab_name(temporaryTabInfos.get(i).getTab_name());
                tabInfoBean.setTenant_id(temporaryTabInfos.get(i).getTenant_id());
                tabInfoBean.setOper_id(temporaryTabInfos.get(i).getUser_id());
                if(StringUtils.isNotBlank(temporaryTabInfos.get(i).getCreate_time())){
                    tabInfoBean.setCreate_time(temporaryTabInfos.get(i).getCreate_time());
                }else{
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    tabInfoBean.setCreate_time(sdf.format(new Date()));
                }
                if(!temporaryTabInfos.get(i).getColInfos().isEmpty()){
                    List<TabColBean> tab_col_list = new ArrayList<>();
                    for (int j=0;j<temporaryTabInfos.get(i).getColInfos().size();j++){
                        TabColBean tabColBean = new TabColBean();
                        tabColBean.setCol_id(temporaryTabInfos.get(i).getColInfos().get(j).getCol_name());
                        tabColBean.setCol_name(temporaryTabInfos.get(i).getColInfos().get(j).getCol_name());
                        tabColBean.setCol_chs_name(temporaryTabInfos.get(i).getColInfos().get(j).getCol_chs_name());
                        tabColBean.setCol_type(temporaryTabInfos.get(i).getColInfos().get(j).getCol_type());
                        tabColBean.setCol_length(temporaryTabInfos.get(i).getColInfos().get(j).getCol_length());
                        tabColBean.setCol_precise(temporaryTabInfos.get(i).getColInfos().get(j).getCol_precise());
                        if(temporaryTabInfos.get(i).getColInfos().get(j).getIs_partition()!=null&&temporaryTabInfos.get(i).getColInfos().get(j).getIs_partition().equals("1")){
                            tabColBean.setIs_partition(1);
                        }
                        if(temporaryTabInfos.get(i).getColInfos().get(j).getIs_key()!=null&&temporaryTabInfos.get(i).getColInfos().get(j).getIs_key().equals("1")){
                            tabColBean.setIs_primary_key(1);
                        }
                        if(temporaryTabInfos.get(i).getColInfos().get(j).getIs_index()!=null&&temporaryTabInfos.get(i).getColInfos().get(j).getIs_index().equals("1")){
                            tabColBean.setIs_index("1");
                        }
                        tabColBean.setSecurity_level("1");
                        tab_col_list.add(tabColBean);
                    }
                    tabInfoBean.setTab_col_list(tab_col_list);
                }else{
                    tabInfoBean.setTab_col_list(new ArrayList<>());
                }
                tabInfoBean.setDispose_list(new ArrayList<>());
                com.newland.edc.cct.dataasset.entity.model.javabean.DataDispBean dataDispBean = new com.newland.edc.cct.dataasset.entity.model.javabean.DataDispBean();
                dataDispBean.setPhy_tab_name(temporaryTabInfos.get(i).getTab_name());
                dataDispBean.setResource_id(temporaryTabInfos.get(i).getResource_id());
                dataDispBean.setConn_id(temporaryTabInfos.get(i).getConn_id());
                dataDispBean.setEntity_type(temporaryTabInfos.get(i).getDb_type());
                tabInfoBean.getDispose_list().add(dataDispBean);
                tabInfoBeans.add(tabInfoBean);
            }
        }
        return tabInfoBeans;
    }

    public List<PLSqlToolTable> transPLSqlToolTable(List<TemporaryTabInfo> temporaryTabInfos){
        List<PLSqlToolTable> tabInfoBeans = new ArrayList<>();
        if(!temporaryTabInfos.isEmpty()){
            for (int i=0;i<temporaryTabInfos.size();i++){
                PLSqlToolTable tabInfoBean = new PLSqlToolTable();
                tabInfoBean.setTab_id(temporaryTabInfos.get(i).getTab_id());
                tabInfoBean.setTab_chs_name(temporaryTabInfos.get(i).getTab_name());
                //表别名
                tabInfoBean.setPhy_tab_name(temporaryTabInfos.get(i).getTab_name());
                tabInfoBean.setTenant_id(temporaryTabInfos.get(i).getTenant_id());
                tabInfoBean.setOper_id(temporaryTabInfos.get(i).getUser_id());
                if(StringUtils.isNotBlank(temporaryTabInfos.get(i).getCreate_time())){
                    tabInfoBean.setCreate_time(temporaryTabInfos.get(i).getCreate_time());
                }else{
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    tabInfoBean.setCreate_time(sdf.format(new Date()));
                }
                if(!temporaryTabInfos.get(i).getColInfos().isEmpty()){
                    List<PLSqlToolColumn> tab_col_list = new ArrayList<>();
                    for (int j=0;j<temporaryTabInfos.get(i).getColInfos().size();j++){
                        PLSqlToolColumn tabColBean = new PLSqlToolColumn();
                        tabColBean.setCol_id(temporaryTabInfos.get(i).getColInfos().get(j).getCol_name());
                        tabColBean.setCol_name(temporaryTabInfos.get(i).getColInfos().get(j).getCol_name());
                        tabColBean.setCol_chs_name(temporaryTabInfos.get(i).getColInfos().get(j).getCol_chs_name());
                        tabColBean.setCol_type(temporaryTabInfos.get(i).getColInfos().get(j).getCol_type());
                        tabColBean.setCol_length(temporaryTabInfos.get(i).getColInfos().get(j).getCol_length());
                        tabColBean.setCol_precise(temporaryTabInfos.get(i).getColInfos().get(j).getCol_precise());
                        if(temporaryTabInfos.get(i).getColInfos().get(j).getIs_partition()!=null&&temporaryTabInfos.get(i).getColInfos().get(j).getIs_partition().equals("1")){
                            tabColBean.setIs_partition("1");
                        }
                        if(temporaryTabInfos.get(i).getColInfos().get(j).getIs_key()!=null&&temporaryTabInfos.get(i).getColInfos().get(j).getIs_key().equals("1")){
                            tabColBean.setIs_primary_key("1");
                        }
                        if(temporaryTabInfos.get(i).getColInfos().get(j).getIs_index()!=null&&temporaryTabInfos.get(i).getColInfos().get(j).getIs_index().equals("1")){
                            tabColBean.setIs_index("1");
                        }
                        tabColBean.setSecurity_level("1");
                        tab_col_list.add(tabColBean);
                    }
                    tabInfoBean.setTab_col_list(tab_col_list);
                }else{
                    tabInfoBean.setTab_col_list(new ArrayList<>());
                }
                tabInfoBean.setDispose_list(new ArrayList<>());
                PLSqlToolDataDisp dataDispBean = new PLSqlToolDataDisp();
                dataDispBean.setPhy_tab_name(temporaryTabInfos.get(i).getTab_name());
                dataDispBean.setResource_id(temporaryTabInfos.get(i).getResource_id());
                dataDispBean.setConn_id(temporaryTabInfos.get(i).getConn_id());
                dataDispBean.setEntity_type(temporaryTabInfos.get(i).getDb_type());
                tabInfoBean.getDispose_list().add(dataDispBean);
                tabInfoBeans.add(tabInfoBean);
            }
        }
        return tabInfoBeans;
    }

    @Override
    public void deleteTemporaryTab(TemporaryTabInfo temporaryTabInfo) throws Exception{
        try {
            plSqlToolBaseDao.deleteTemporaryTab(temporaryTabInfo);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
    }

    @Override
    public void deleteTemporaryCol(String tab_id) throws Exception{
        try {
            plSqlToolBaseDao.deleteTemporaryCol(tab_id);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
    }

    @Override
    public void insertTemporaryInfo(TemporaryTabInfo temporaryTabInfo) throws Exception{
        try {
            plSqlToolBaseDao.insertTemporaryInfo(temporaryTabInfo);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
    }

    @Override
    public void insertTemporaryCol(TemporaryColInfo temporaryColInfo)throws Exception{
        try {
            plSqlToolBaseDao.insertTemporaryCol(temporaryColInfo);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
    }

    public String transVariable(String var){
        String regex = "\\$\\[\\w*\\]";
        String regex1 = "\\$\\{\\w*\\}";
        Pattern pattern = Pattern.compile(regex);
        Pattern pattern1 = Pattern.compile(regex1);
        Matcher m = pattern.matcher(var);
        Matcher n = pattern1.matcher(var);
        if(m.find()){
            try {
                if(DataCatch.getVarsMap().get(var)!=null&&!DataCatch.getVarsMap().get(var).equals("")){
                    return DataCatch.getVarsMap().get(var);
                }
                String url = RestFulServiceUtils.getServiceUrl("services-env")+"/env/getStaticVarByName?varName="+var;
                log.info("变量翻译："+url);
                String resultinfo = HttpUtil.httpRequest(url);
                if(resultinfo!=null&&!resultinfo.equals("")){
                    List<CrModuleBean> crModuleBeans = JSONArray.parseArray(resultinfo,CrModuleBean.class);
                    if(!crModuleBeans.isEmpty()){
                        for (int i=0;i<crModuleBeans.size();i++){
                            DataCatch.getVarsMap().put("$["+crModuleBeans.get(i).getVarName()+"]",crModuleBeans.get(i).getVarValue());
                            var = var.replace("$["+crModuleBeans.get(i).getVarName()+"]",crModuleBeans.get(i).getVarValue());
                        }
                    }
                }
            }catch (Exception e){
                log.error(e.getMessage(),e);
            }
        }
        if(n.find()){
            try {
                if(DataCatch.getVarsMap().get(var)!=null&&!DataCatch.getVarsMap().get(var).equals("")){
                    return DataCatch.getVarsMap().get(var);
                }
                String url = RestFulServiceUtils.getServiceUrl("services-env")+"/env/getStaticVarByName?varName="+var.replace("{","[").replace("}","]");
                log.info("变量翻译："+url);
                String resultinfo = HttpUtil.httpRequest(url);
                if(resultinfo!=null&&!resultinfo.equals("")){
                    List<CrModuleBean> crModuleBeans = JSONArray.parseArray(resultinfo,CrModuleBean.class);
                    if(!crModuleBeans.isEmpty()){
                        for (int i=0;i<crModuleBeans.size();i++){
                            DataCatch.getVarsMap().put("${"+crModuleBeans.get(i).getVarName()+"}",crModuleBeans.get(i).getVarValue());
                            var = var.replace("${"+crModuleBeans.get(i).getVarName()+"}",crModuleBeans.get(i).getVarValue());
                        }
                    }
                }
            }catch (Exception e){
                log.error(e.getMessage(),e);
            }
        }
        return var;
    }

    @Override
    public String transVariableAll(String var){
        String regex = "\\$\\[\\w*\\]";
        String regex1 = "\\$\\{\\w*\\}";
        Pattern pattern = Pattern.compile(regex);
        Pattern pattern1 = Pattern.compile(regex1);
        Matcher m = pattern.matcher(var);
        Matcher n = pattern1.matcher(var);
        List<String> list1 = new ArrayList<>();
        List<String> list2 = new ArrayList<>();
        while (m.find()){
            String str = m.group(0);
            if(DataCatch.getVarsMap().get(str)!=null&&!DataCatch.getVarsMap().get(str).equals("")){
                var = var.replace(str,DataCatch.getVarsMap().get(str));
            }else{
                list1.add(str);
            }
        }
        while (n.find()){
            String str = n.group(0);
            if(DataCatch.getVarsMap().get(str)!=null&&!DataCatch.getVarsMap().get(str).equals("")){
                var = var.replace(str,DataCatch.getVarsMap().get(str));
            }else{
                list2.add(str);
            }
        }
        if(!list1.isEmpty()){
            String param = "";
            for (int i=0;i<list1.size();i++){
                if(param.equals("")){
                    param = param + "varName="+list1.get(i);
                }else{
                    param = param + "&varName="+list1.get(i);
                }
            }
            String url = RestFulServiceUtils.getServiceUrl("services-env")+"/env/getStaticVarByName?"+param;
            log.info("变量翻译："+url);
            String resultinfo = HttpUtil.httpRequest(url);
            if(resultinfo!=null&&!resultinfo.equals("")){
                List<CrModuleBean> crModuleBeans = JSONArray.parseArray(resultinfo,CrModuleBean.class);
                if(!crModuleBeans.isEmpty()){
                    for (int i=0;i<crModuleBeans.size();i++){
                        DataCatch.getVarsMap().put("$["+crModuleBeans.get(i).getVarName()+"]",crModuleBeans.get(i).getVarValue());
                        var = var.replace("$["+crModuleBeans.get(i).getVarName()+"]",crModuleBeans.get(i).getVarValue());
                    }
                }
            }
        }
        if(!list2.isEmpty()){
            String param = "";
            for (int i=0;i<list2.size();i++){
                if(param.equals("")){
                    param = param + "varName="+list2.get(i);
                }else{
                    param = param + "&varName="+list2.get(i);
                }
            }
            String url = RestFulServiceUtils.getServiceUrl("services-env")+"/env/getStaticVarByName?"+param.replace("{","[").replace("}","]");
            log.info("变量翻译："+url);
            String resultinfo = HttpUtil.httpRequest(url);
            if(resultinfo!=null&&!resultinfo.equals("")){
                List<CrModuleBean> crModuleBeans = JSONArray.parseArray(resultinfo,CrModuleBean.class);
                if(!crModuleBeans.isEmpty()){
                    for (int i=0;i<crModuleBeans.size();i++){
                        DataCatch.getVarsMap().put("${"+crModuleBeans.get(i).getVarName()+"}",crModuleBeans.get(i).getVarValue());
                        var = var.replace("${"+crModuleBeans.get(i).getVarName()+"}",crModuleBeans.get(i).getVarValue());
                    }
                }
            }
        }
        return var;
    }

    @Override
    public PLSqlToolDataAssetEntity getTabEntity(String tenant_id,String resource_id,String conn_id)throws Exception{
        PLSqlToolDataAssetEntity returnData = new PLSqlToolDataAssetEntity();
        try {
            String db_type = "";
            List<DBResurceBean> dbResurceBeans = queryResourceAndConn(tenant_id,null,resource_id);
            if(dbResurceBeans!=null&&dbResurceBeans.size()>0){
                for (int i=0;i<dbResurceBeans.size();i++){
                    if(dbResurceBeans.get(i).getResource_id().equals(resource_id)){
                        log.info("资源名称："+dbResurceBeans.get(i).getResource_name()+"资源类型："+dbResurceBeans.get(i).getResource_type());
                        db_type = dbResurceBeans.get(i).getResource_type();
                    }
                }
            }else{
                throw new Exception("查无改资源");
            }
            if(StringUtils.isBlank(db_type)){
                throw new Exception("数据库类型为空");
            }
            List<PLSqlToolTable> tabInfoBeanList = this.plSqlToolBaseDao.getTabEntity(tenant_id,db_type,resource_id);
            String TAB_VIEW_SWITCH = RestFulServiceUtils.getconfig("TAB_VIEW_SWITCH");
            if(!tabInfoBeanList.isEmpty()){
                List<PLSqlToolTable> definitions = new ArrayList<>();//定义集合
                List<PLSqlToolTable> subscriptions = new ArrayList<>();//订阅集合
                for (int i=0;i<tabInfoBeanList.size();i++){
                    if(db_type.equals("hive")&&!tabInfoBeanList.get(i).getDispose_list().get(0).getDb_name().equals("")){
                        if(TAB_VIEW_SWITCH!=null&&TAB_VIEW_SWITCH.equals("1")){
                            String realtabname = transVariable(tabInfoBeanList.get(i).getDispose_list().get(0).getDb_name())+"."+tabInfoBeanList.get(i).getPhy_tab_name();
                            tabInfoBeanList.get(i).setPhy_tab_name(realtabname);
                        }else{
                            tabInfoBeanList.get(i).setPhy_tab_name(tabInfoBeanList.get(i).getDispose_list().get(0).getDb_name()+"."+tabInfoBeanList.get(i).getPhy_tab_name());
                        }
                    }else if(db_type.equals("oracle")&&!tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user().equals("")){
                        if(TAB_VIEW_SWITCH!=null&&TAB_VIEW_SWITCH.equals("1")){
                            String realtabname = transVariable(tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user())+"."+tabInfoBeanList.get(i).getPhy_tab_name();
                            tabInfoBeanList.get(i).setPhy_tab_name(realtabname);
                        }else{
                            tabInfoBeanList.get(i).setPhy_tab_name(tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user()+"."+tabInfoBeanList.get(i).getPhy_tab_name());
                        }
                    }else if(db_type.equals("db2")&&!tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user().equals("")){
                        if(TAB_VIEW_SWITCH!=null&&TAB_VIEW_SWITCH.equals("1")){
                            String realtabname = transVariable(tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user())+"."+tabInfoBeanList.get(i).getPhy_tab_name();
                            tabInfoBeanList.get(i).setPhy_tab_name(realtabname);
                        }else{
                            tabInfoBeanList.get(i).setPhy_tab_name(tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user()+"."+tabInfoBeanList.get(i).getPhy_tab_name());
                        }
                    }else if(db_type.equals("greenplum")&&!tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user().equals("")){
                        if(TAB_VIEW_SWITCH!=null&&TAB_VIEW_SWITCH.equals("1")){
                            String realtabname = transVariable(tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user())+"."+tabInfoBeanList.get(i).getPhy_tab_name();
                            tabInfoBeanList.get(i).setPhy_tab_name(realtabname);
                        }else{
                            tabInfoBeanList.get(i).setPhy_tab_name(tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user()+"."+tabInfoBeanList.get(i).getPhy_tab_name());
                        }
                    }else if(db_type.equals("mysql")&&!tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user().equals("")){
                        if(TAB_VIEW_SWITCH!=null&&TAB_VIEW_SWITCH.equals("1")){
                            String realtabname = transVariable(tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user())+"."+tabInfoBeanList.get(i).getPhy_tab_name();
                            tabInfoBeanList.get(i).setPhy_tab_name(realtabname);
                        }else{
                            tabInfoBeanList.get(i).setPhy_tab_name(tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user()+"."+tabInfoBeanList.get(i).getPhy_tab_name());
                        }
                    }else if(db_type.equals("gbase")&&!tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user().equals("")){
                        if(TAB_VIEW_SWITCH!=null&&TAB_VIEW_SWITCH.equals("1")){
                            String realtabname = transVariable(tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user())+"."+tabInfoBeanList.get(i).getPhy_tab_name();
                            tabInfoBeanList.get(i).setPhy_tab_name(realtabname);
                        }else{
                            tabInfoBeanList.get(i).setPhy_tab_name(tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user()+"."+tabInfoBeanList.get(i).getPhy_tab_name());
                        }
                    }else{
                        continue;
                    }
                    if(tabInfoBeanList.get(i).getTenant_id().equals(tenant_id)){
                        if(tabInfoBeanList.get(i).getDispose_list()!=null&&tabInfoBeanList.get(i).getDispose_list().get(0).getResource_id().equals(resource_id)){
                            definitions.add(tabInfoBeanList.get(i));
                        }
                    }else{
                        subscriptions.add(tabInfoBeanList.get(i));
                    }
                }
                returnData.setDefinitions(definitions);
                returnData.setSubscriptions(subscriptions);
            }
            TemporaryTabInfo temporaryTabInfo = new TemporaryTabInfo();
            temporaryTabInfo.setTenant_id(tenant_id);
            temporaryTabInfo.setResource_id(resource_id);
            temporaryTabInfo.setConn_id(conn_id);
            List<TemporaryTabInfo> temporaryTabInfos = getTemporaryInfo(temporaryTabInfo);
            if(!temporaryTabInfos.isEmpty()){

            }else{
                temporaryTabInfos = new ArrayList<>();
            }
            List<PLSqlToolTable> tabInfoBeanList1 = transPLSqlToolTable(temporaryTabInfos);
            returnData.setTemporarytabs(tabInfoBeanList1);
            log.info("租户："+tenant_id+"资源："+resource_id+"定义："+returnData.getDefinitions().size()+"订阅："+returnData.getSubscriptions().size()+"临时："+returnData.getTemporarytabs().size());
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
        return returnData;
    }

    @Override
    public PLSqlToolDataAssetEntity getTabEntity(TestGrammarBean testGrammarBean)throws Exception{
        PLSqlToolDataAssetEntity returnData = new PLSqlToolDataAssetEntity();
        try {
            String db_type = testGrammarBean.getDbType();
            if(StringUtils.isBlank(db_type)){
                throw new Exception("无数据类型");
            }
            List<PLSqlToolTable> tabInfoBeanList = this.plSqlToolBaseDao.getTabEntity(testGrammarBean.getTenant_id(),db_type,testGrammarBean.getResource_id());
            String TAB_VIEW_SWITCH = RestFulServiceUtils.getconfig("TAB_VIEW_SWITCH");
            if(!tabInfoBeanList.isEmpty()){
                List<PLSqlToolTable> definitions = new ArrayList<>();//定义集合
                List<PLSqlToolTable> subscriptions = new ArrayList<>();//订阅集合
                for (int i=0;i<tabInfoBeanList.size();i++){
                    if(db_type.equals("hive")&&!tabInfoBeanList.get(i).getDispose_list().get(0).getDb_name().equals("")){
                        if(TAB_VIEW_SWITCH!=null&&TAB_VIEW_SWITCH.equals("1")){
                            String realtabname = transVariable(tabInfoBeanList.get(i).getDispose_list().get(0).getDb_name())+"."+tabInfoBeanList.get(i).getPhy_tab_name();
                            tabInfoBeanList.get(i).setPhy_tab_name(realtabname);
                        }else{
                            tabInfoBeanList.get(i).setPhy_tab_name(tabInfoBeanList.get(i).getDispose_list().get(0).getDb_name()+"."+tabInfoBeanList.get(i).getPhy_tab_name());
                        }
                    }else if(db_type.equals("oracle")&&!tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user().equals("")){
                        if(TAB_VIEW_SWITCH!=null&&TAB_VIEW_SWITCH.equals("1")){
                            String realtabname = transVariable(tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user())+"."+tabInfoBeanList.get(i).getPhy_tab_name();
                            tabInfoBeanList.get(i).setPhy_tab_name(realtabname);
                        }else{
                            tabInfoBeanList.get(i).setPhy_tab_name(tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user()+"."+tabInfoBeanList.get(i).getPhy_tab_name());
                        }
                    }else if(db_type.equals("db2")&&!tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user().equals("")){
                        if(TAB_VIEW_SWITCH!=null&&TAB_VIEW_SWITCH.equals("1")){
                            String realtabname = transVariable(tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user())+"."+tabInfoBeanList.get(i).getPhy_tab_name();
                            tabInfoBeanList.get(i).setPhy_tab_name(realtabname);
                        }else{
                            tabInfoBeanList.get(i).setPhy_tab_name(tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user()+"."+tabInfoBeanList.get(i).getPhy_tab_name());
                        }
                    }else if(db_type.equals("greenplum")&&!tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user().equals("")){
                        if(TAB_VIEW_SWITCH!=null&&TAB_VIEW_SWITCH.equals("1")){
                            String realtabname = transVariable(tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user())+"."+tabInfoBeanList.get(i).getPhy_tab_name();
                            tabInfoBeanList.get(i).setPhy_tab_name(realtabname);
                        }else{
                            tabInfoBeanList.get(i).setPhy_tab_name(tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user()+"."+tabInfoBeanList.get(i).getPhy_tab_name());
                        }
                    }else if(db_type.equals("mysql")&&!tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user().equals("")){
                        if(TAB_VIEW_SWITCH!=null&&TAB_VIEW_SWITCH.equals("1")){
                            String realtabname = transVariable(tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user())+"."+tabInfoBeanList.get(i).getPhy_tab_name();
                            tabInfoBeanList.get(i).setPhy_tab_name(realtabname);
                        }else{
                            tabInfoBeanList.get(i).setPhy_tab_name(tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user()+"."+tabInfoBeanList.get(i).getPhy_tab_name());
                        }
                    }else if(db_type.equals("gbase")&&!tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user().equals("")){
                        if(TAB_VIEW_SWITCH!=null&&TAB_VIEW_SWITCH.equals("1")){
                            String realtabname = transVariable(tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user())+"."+tabInfoBeanList.get(i).getPhy_tab_name();
                            tabInfoBeanList.get(i).setPhy_tab_name(realtabname);
                        }else{
                            tabInfoBeanList.get(i).setPhy_tab_name(tabInfoBeanList.get(i).getDispose_list().get(0).getDb_user()+"."+tabInfoBeanList.get(i).getPhy_tab_name());
                        }
                    }else{
                        continue;
                    }
                    if(tabInfoBeanList.get(i).getTenant_id().equals(testGrammarBean.getTenant_id())){
                        if(tabInfoBeanList.get(i).getDispose_list()!=null&&tabInfoBeanList.get(i).getDispose_list().get(0).getResource_id().equals(testGrammarBean.getResource_id())){
                            definitions.add(tabInfoBeanList.get(i));
                        }
                    }else{
                        subscriptions.add(tabInfoBeanList.get(i));
                    }
                }
                returnData.setDefinitions(definitions);
                returnData.setSubscriptions(subscriptions);
            }
            TemporaryTabInfo temporaryTabInfo = new TemporaryTabInfo();
            temporaryTabInfo.setTenant_id(testGrammarBean.getTenant_id());
            temporaryTabInfo.setResource_id(testGrammarBean.getResource_id());
            temporaryTabInfo.setConn_id(testGrammarBean.getConn_id());
            List<TemporaryTabInfo> temporaryTabInfos = getTemporaryInfo(temporaryTabInfo);
            if(!temporaryTabInfos.isEmpty()){

            }else{
                temporaryTabInfos = new ArrayList<>();
            }
            List<PLSqlToolTable> tabInfoBeanList1 = transPLSqlToolTable(temporaryTabInfos);
            returnData.setTemporarytabs(tabInfoBeanList1);
            log.info("租户："+testGrammarBean.getTenant_id()+"资源："+testGrammarBean.getResource_id()+"定义："+returnData.getDefinitions().size()+"订阅："+returnData.getSubscriptions().size()+"临时："+returnData.getTemporarytabs().size());
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
        return returnData;
    }

    @Override
    public void insertExecuteLog(ExecuteLog executeLog) throws Exception{
        try {
            this.plSqlToolBaseDao.insertExecuteLog(executeLog);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
    }

    @Override
    public List<ExecuteGroupLog> queryExecuteLogs(ExecuteLog executeLog) throws Exception{
        List<ExecuteGroupLog> executeGroupLogs = new ArrayList<>();
        try {
            executeGroupLogs = this.plSqlToolBaseDao.queryExecuteLogs(executeLog);
            if(executeGroupLogs==null){
                executeGroupLogs = new ArrayList<>();
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
        return executeGroupLogs;
    }

    @Override
    public List<ExecuteLog> selectExecuteLog(ExecuteLog executeLog) throws Exception{
        List<ExecuteLog> list = new ArrayList<>();
        try {
            list = this.plSqlToolBaseDao.selectExecuteLogs(executeLog);
            if(list==null){
                list = new ArrayList<>();
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
        return list;
    }

    @Override
    public void updateExecuteLog(ExecuteLog executeLog,String status) throws Exception{
        try {
            this.plSqlToolBaseDao.updateExecuteLog(executeLog,status);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
    }

    @Override
    public void cleanExecuteLog(String time) throws Exception{
        try {
            this.plSqlToolBaseDao.cleanExecuteLog(time);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
    }

    @Override
    public List<ExecuteErrorInfo> selectErrorInfo(ExecuteErrorInfo executeErrorInfo){
        List<ExecuteErrorInfo> executeErrorInfos = new ArrayList<>();
        try {
            executeErrorInfos = this.plSqlToolBaseDao.selectErrorInfos(executeErrorInfo);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
        return executeErrorInfos;
    }

    @Override
    public void insertErrorInfo(ExecuteErrorInfo executeErrorInfo) throws Exception{
        try {
            this.plSqlToolBaseDao.insertErrorInfo(executeErrorInfo);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
    }

    @Override
    public List<ExecuteResult> selectExecuteResult(String task_id){
        try {
            return this.plSqlToolBaseDao.selectExecuteResult(task_id);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
    }

    @Override
    public void insertExecuteResult(ExecuteResult executeResult) throws Exception{
        try {
            this.plSqlToolBaseDao.insertExecuteResult(executeResult);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
    }


    @Override
    public String getIp() throws Exception{
        //获取机器ip方法关闭  改为获取手工添加的机器ip
//        InetAddress addr = InetAddress.getLocalHost();
//        String ip=addr.getHostAddress().toString(); //获取本机ip
        String ip = "";
        ip = iplSqlToolCloseConnService.getCurrentNodeId();
        if(StringUtils.isBlank(ip)){
            if(RestFulServiceUtils.getconfig("EDC_PLSQLTOOL_IP")!=null&&!RestFulServiceUtils.getconfig("EDC_PLSQLTOOL_IP").equals("")){
                ip = RestFulServiceUtils.getconfig("EDC_PLSQLTOOL_IP");
            }else{
                ip = "";
            }
        }
        return ip;
    }

    @Override
    public void cleanExecuteResult(String time) throws Exception{
        try {
            this.plSqlToolBaseDao.cleanExecuteResult(time);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
    }

    @Override
    public String selectDataRange(String tenant_id){
        String data_range = "";
        try {
            List<String> list = this.plSqlToolBaseDao.selectDataRange(tenant_id);
            if(!list.isEmpty()){
                data_range = list.get(0);
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
        return data_range;
    }

    @Override
    public List<TabColBean> selectTabEentityCols(String tab_id){
        List<TabColBean> list;
        try {
            list = this.plSqlToolBaseDao.selectTabEntityCols(tab_id);

        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
        return list;
    }

    @Override
    public TableOptAuthority selectAuthority(String user_id) throws Exception{
//        if(DataCatch.getTableOptAuthorityMap().get(user_id)!=null){
//            return DataCatch.getTableOptAuthorityMap().get(user_id);
//        }
        TableOptAuthority tableOptAuthority = new TableOptAuthority();
        try {
            List<TableOpt> tableOpts = this.plSqlToolBaseDao.selectAuthority(user_id);
            for (int i=0;i<tableOpts.size();i++){
                ClassUtil.invoke(tableOptAuthority,"set"+tableOpts.get(i).getExecute_type(),tableOpts.get(i).getAuthority_value());
            }
//            DataCatch.getTableOptAuthorityMap().put(user_id,tableOptAuthority);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
        return tableOptAuthority;
    }

    @Override
    public TabInfo queryTabInfo(TabInfo tabInfo) throws Exception {
        return plSqlToolBaseDao.selectTabInfo(tabInfo);
    }

    @Override
    public RespInfo queryTabInfoListByPage(TabInfoReq tabInfoReq){
        RespInfo respInfo = new RespInfo();
        if(tabInfoReq.getUser_id() == null || tabInfoReq.getUser_id().trim().equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("user_id为空");
            return respInfo;
        }
        if(tabInfoReq.getTenant_id() == null || tabInfoReq.getTenant_id().trim().equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("tenant_id为空");
            return respInfo;
        }
        if(tabInfoReq.getResource_id() == null || tabInfoReq.getResource_id().trim().equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("resource_id为空");
            return respInfo;
        }
        if(tabInfoReq.getConn_id() == null || tabInfoReq.getConn_id().trim().equals("")){
            respInfo.setRespResult("0");
            respInfo.setRespErrorCode("0");
            respInfo.setRespErrorDesc("conn_id为空");
            return respInfo;
        }

        try{
            int count = plSqlToolBaseDao.getTabInfoCount(tabInfoReq);
            respInfo.setDataTotalCount(count);
            if (count == 0){
                respInfo.setRespData(new ArrayList<TabInfo>());
            }else {
                List<TabInfo> tabInfoList = plSqlToolBaseDao.selectTabInfoListByPage(tabInfoReq);
                respInfo.setRespData(tabInfoList);
            }
            respInfo.setRespResult("1");
        }catch (Exception e){
            log.error(e.getMessage(),e);
            respInfo.setRespResult("0");
            respInfo.setRespErrorDesc(e.getMessage());
            return  respInfo;
        }
        return respInfo;
    }

    @Override
    public void saveTabInfo(TabInfo tabInfo) throws Exception {

//        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        String updateTime = format.format(new Date());
//        tabInfo.setUpdate_time(updateTime);
        if (tabInfo.getTab_id() == null || tabInfo.getTab_id().trim().equals("")){
            String tabId = UUID.randomUUID().toString();
            tabId = tabId.replaceAll("-", "");
            tabInfo.setTab_id(tabId);
//            if (tabInfo.getIs_open() == null || tabInfo.getIs_open().trim().equals("")){
//                tabInfo.setIs_open("1");
//            }
            plSqlToolBaseDao.insertTabInfo(tabInfo);
        }else {
            plSqlToolBaseDao.updateTabInfo(tabInfo);
        }
    }

    @Override
    public void deleteTabInfo(TabInfo tabInfo) throws Exception {
        plSqlToolBaseDao.deleteTabInfo(tabInfo);
    }

    @Override
    public List<ExecutePoolHeartbeat> selectPoolHeartbeat(ExecutePoolHeartbeat executePoolHeartbeat){
        List<ExecutePoolHeartbeat> executePoolHeartbeats = new ArrayList<>();
        try {
            executePoolHeartbeats = this.plSqlToolBaseDao.selectPoolHeartbeat(executePoolHeartbeat);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
        return executePoolHeartbeats;
    }

    @Override
    public int updatePoolHeartbeat(ExecutePoolHeartbeat executePoolHeartbeat,String heartbeat)throws Exception{
        int num = 0;
        try {
            num = this.plSqlToolBaseDao.updatePoolHeartbeat(executePoolHeartbeat,heartbeat);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
        return num;
    }

    @Override
    public void insertPoolHeartbeat(ExecutePoolHeartbeat executePoolHeartbeat) throws Exception{
        try {
            this.plSqlToolBaseDao.insertPoolHeartbeat(executePoolHeartbeat);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
    }

    @Override
    public List<ExecuteLog> insertExecuteLogs(List<TestSqlBean> testSqlBeans,String executeIP,String status) throws Exception{
        List<ExecuteLog> executeLogs = new ArrayList<>();
        for (int m=0;m<testSqlBeans.size();m++){
            String execute_type = "DQL";
            String key_col = "SELECT";
            for (int n=0;n<testSqlBeans.get(m).getTestSqlDetailBeans().size();n++){
                if(!testSqlBeans.get(m).getTestSqlDetailBeans().get(n).getExecuteType().equalsIgnoreCase("DQL")&&execute_type.equalsIgnoreCase("DQL")){
                    execute_type = testSqlBeans.get(m).getTestSqlDetailBeans().get(n).getExecuteType();
                    key_col = testSqlBeans.get(m).getTestSqlDetailBeans().get(n).getKeyCol();
                }
            }
            ExecuteLog executeLog = new ExecuteLog();
            executeLog.setId(testSqlBeans.get(m).getTask_id());
            executeLog.setGroup_id(testSqlBeans.get(m).getGroup_id());
            executeLog.setUser_id(testSqlBeans.get(m).getUser_id());
            executeLog.setTenant_id(testSqlBeans.get(m).getTenant_id());
            executeLog.setDb_type(testSqlBeans.get(m).getDbType());
            executeLog.setExecute_type(execute_type);
            executeLog.setKeycol(key_col);
            executeLog.setRun_mode(testSqlBeans.get(m).getRun_mode());
            executeLog.setSqlstring(testSqlBeans.get(m).getSql());
            executeLog.setReal_sqlstring(testSqlBeans.get(m).getReal_sql());
            executeLog.setResource_id(testSqlBeans.get(m).getResource_id());
            executeLog.setConn_id(testSqlBeans.get(m).getConn_id());
            executeLog.setSys_id(testSqlBeans.get(m).getSys_id());
            if(executeIP!=null&&!executeIP.equals("")){
                executeLog.setIp(executeIP);
            }else{
                executeLog.setIp(getIp());
            }
            executeLog.setStatus(status);
            this.plSqlToolBaseDao.insertExecuteLog(executeLog);
            executeLogs.add(executeLog);
        }
        return executeLogs;
    }

    @Override
    public ExecutePoolHeartbeat queryExecutePool(String db_type){
        ExecutePoolHeartbeat executePoolHeartbeat = null;
        try {
            executePoolHeartbeat = this.plSqlToolBaseDao.queryExecutePool(db_type);
            if(executePoolHeartbeat!=null){
            }else{
                executePoolHeartbeat = new ExecutePoolHeartbeat();
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
        }
        return executePoolHeartbeat;
    }

    @Override
    public TabInfoBean getTabEntityInfo(String tabId) {

        TabInfoBean tabInfoBean = plSqlToolBaseDao.getTabEntityInfo(tabId);

        if (tabInfoBean != null){
            tabInfoBean.setTab_col_list(plSqlToolBaseDao.selectTabEntityCols(tabId));
        }

        return tabInfoBean;
    }

    @Override
    public Map<String, ConnectTnsConf> queryTnsConfigue(List<DataDispBean> dataDispBeans){
        Map<String, ConnectTnsConf> map = new HashMap<>();
        List<ConnectTnsConf> connectTnsConfs = this.plSqlToolBaseDao.queryConnConf(dataDispBeans);
        if(!connectTnsConfs.isEmpty()){
            String DBNAME = RestFulServiceUtils.getconfig("PLS_IMPORTANT_DBNAME");
            String[] arr_DBNAME = DBNAME.split(",");
            for (int i=0;i<connectTnsConfs.size();i++){
                for (int j=0;j<arr_DBNAME.length;j++){
                    if(connectTnsConfs.get(i).getTns()!=null&&!connectTnsConfs.get(i).getTns().equals("")){
                        if(connectTnsConfs.get(i).getTns().toUpperCase().indexOf(arr_DBNAME[j].toUpperCase())>=0){
                            log.info("连接名："+connectTnsConfs.get(i).getConn_name()+" 连接id："+connectTnsConfs.get(i).getConn_id()+" tns："+connectTnsConfs.get(i).getTns()+" 匹配字串："+arr_DBNAME[j]);
                            map.put(connectTnsConfs.get(i).getConn_id(),connectTnsConfs.get(i));
                            break;
                        }
                    }
                }
            }
        }
        return map;
    }

    @Override
    public String queryExecuteIp(String group_id){
        //根据任务组id获取提交执行的机器的ip
        String ip = this.plSqlToolBaseDao.queryExecuteIp(group_id);
        return ip;
    }

    @Override
    public void insertExportLog(ExportLog exportLogBean) throws Exception{
        this.plSqlToolBaseDao.insertExportLog(exportLogBean);
    }

    @Override
    public List<ExportLog> queryExportLog(ExportLog exportLog){
        return this.plSqlToolBaseDao.queryExportLog(exportLog);
    }

    @Override
    public ExportLogReq queryExportLog(ExportLogReq exportLogBean){
        return this.plSqlToolBaseDao.queryExportLog(exportLogBean);
    }

    @Override
    public List<ExportLog> queryExportLogQuartz(){
        return this.plSqlToolBaseDao.queryExportLogQuartz();
    }

    @Override
    public void updateExportLog(String export_id,String status,String export_result,String download_status) throws Exception{
        this.plSqlToolBaseDao.updateExportLog(export_id,status,export_result,download_status);
    }

    @Override
    public void updateExportLog(ExportLog exportLog,String export_id) throws Exception{
        this.plSqlToolBaseDao.updateExportLog(exportLog,export_id);
    }

    @Override
    public void updateExportLogQuartz() throws Exception{
        this.plSqlToolBaseDao.updateExportLogQuartz();
    }

    @Override
    public void deleteExportLog(String export_id) throws Exception{
        this.plSqlToolBaseDao.deleteExportLog(export_id);
    }

    @Override
    public List<ExportLog> queryTimeoutExportLog(int time){
        return this.plSqlToolBaseDao.queryTimeoutExportLog(time);
    }

    @Override
    public void insertExportDownloadLog(ExportDownloadLog exportDownloadLog) throws Exception{
        this.plSqlToolBaseDao.insertExportDownloadLog(exportDownloadLog);
    }

    @Override
    public RespInfo getTemporaryInfoByTrans(TemporaryTabInfo temporaryTabInfo) {
        RespInfo respInfo = new RespInfo();
        try {
            List<TemporaryTabInfo> list = getTemporaryInfo(temporaryTabInfo);
            if(list==null){
                list = new ArrayList<>();
            }
            List<TabInfoBean> tabInfoBeanList = trans(list);
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
    @Override
    public List<FtpConfig> queryFtpConfig(FtpConfig ftpConfig){
        return this.plSqlToolBaseDao.queryFtpConfig(ftpConfig);
    }

    @Override
    public void insertFtpSynchronousLog(FtpSynchronousLog ftpSynchronousLog) throws Exception{
        this.plSqlToolBaseDao.insertFtpSynchronousLog(ftpSynchronousLog);
    }

    @Override
    public List<FtpSynchronousLog> queryFtpSynchronousLog(FtpSynchronousLog ftpSynchronousLog){
        return this.plSqlToolBaseDao.queryFtpSynchronousLog(ftpSynchronousLog);
    }

    @Override
    public void updateFtpSynchronousLog(String log_id,String status,String error_info) throws Exception{
        this.plSqlToolBaseDao.updateFtpSynchronousLog(log_id,status,error_info);
    }

    /**
     * 获取项目部署地址的配置信息
     * @throws Exception
     */
    @Override
    public ResAddress getCurrentResAddressVersion() throws Exception{
        return resAddress;
    }

    @Override
    public String queryConnIdByConnNameEn(String conn_name_en){
        return this.plSqlToolBaseDao.queryConnIdByConnNameEn(conn_name_en);
    }

    @Override
    public List<DBTenantBean> getTenants(String db_type) throws Exception{
        List<DBTenantBean> dbTenantBeans = new ArrayList<>();
        try {
            List<TabTenantBean> tabTenantBeans = getTenants();
            if(tabTenantBeans==null){
                tabTenantBeans = new ArrayList<>();
            }
            if(!tabTenantBeans.isEmpty()){
                for (int i=0;i<tabTenantBeans.size();i++){
                    DBTenantBean dbTenantBean = new DBTenantBean();
                    dbTenantBean.setTenant_id(tabTenantBeans.get(i).getTenant_id());
                    dbTenantBean.setTenant_name(tabTenantBeans.get(i).getTenant_name());
                    List<DataDispBean> dispose_list = queryTenantResouce(tabTenantBeans.get(i).getTenant_id(),null);
                    String PLS_IMPORTANT_DBRESOURCE = RestFulServiceUtils.getconfig("PLS_IMPORTANT_DBRESOURCE");
                    Map<String, ConnectTnsConf> map = null;
                    if(PLS_IMPORTANT_DBRESOURCE!=null&&PLS_IMPORTANT_DBRESOURCE.equals("1")&&db_type.equalsIgnoreCase("oracle")){
                        //获取配置重要数据库配置
                        map = queryTnsConfigue(dispose_list);
                    }
                    dbTenantBean.setDbResourceBeans(new ArrayList<>());
                    if(!dispose_list.isEmpty()){
                        for (int j=0;j<dispose_list.size();j++){
                            if(dispose_list.get(j).getEntity_type().equalsIgnoreCase(db_type)){
                                DBResurceBean dbResurceBean =new DBResurceBean();
                                dbResurceBean.setResource_id(dispose_list.get(j).getDispose_type());
                                dbResurceBean.setResource_name(dispose_list.get(j).getDispose_desc());
                                dbResurceBean.setResource_type(dispose_list.get(j).getEntity_type());
                                List<com.newland.edc.cct.dataasset.entity.model.javabean.DataDispBean> con_list = queryTenantConn(tabTenantBeans.get(i).getTenant_id(),dispose_list.get(j).getDispose_type());
                                dbResurceBean.setDbConnectionBeans(new ArrayList<>());
                                if(!con_list.isEmpty()){
                                    for (int m=0;m<con_list.size();m++){
                                        //过滤重要数据配置
                                        if(map!=null){
                                            if(map.get(con_list.get(m).getConn_id())!=null){
                                                DBConnectionBean dbConnectionBean = new DBConnectionBean();
                                                dbConnectionBean.setConn_id(con_list.get(m).getConn_id());
                                                dbConnectionBean.setConn_name(con_list.get(m).getConn_name());
                                                dbResurceBean.getDbConnectionBeans().add(dbConnectionBean);
                                            }
                                        }else{
                                            DBConnectionBean dbConnectionBean = new DBConnectionBean();
                                            dbConnectionBean.setConn_id(con_list.get(m).getConn_id());
                                            dbConnectionBean.setConn_name(con_list.get(m).getConn_name());
                                            dbResurceBean.getDbConnectionBeans().add(dbConnectionBean);
                                        }
                                    }
                                }
                                dbTenantBean.getDbResourceBeans().add(dbResurceBean);
                            }
                        }
                    }
                    dbTenantBeans.add(dbTenantBean);
                }
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
        return dbTenantBeans;
    }

    @Override
    public List<DBTenantBean> getTenantsByUserId(String user_id,String tokencode,String db_type) throws Exception{
        List<DBTenantBean> dbTenantBeans = new ArrayList<>();
        try {
            List<TabTenantBean> tabTenantBeans = getTenantsByUserId(user_id,tokencode);
            if(tabTenantBeans==null){
                tabTenantBeans = new ArrayList<>();
            }
            if(!tabTenantBeans.isEmpty()){
                for (int i=0;i<tabTenantBeans.size();i++){
                    DBTenantBean dbTenantBean = new DBTenantBean();
                    dbTenantBean.setTenant_id(tabTenantBeans.get(i).getTenant_id());
                    dbTenantBean.setTenant_name(tabTenantBeans.get(i).getTenant_name());
                    List<DataDispBean> dispose_list = queryTenantResouce(tabTenantBeans.get(i).getTenant_id(),null);
                    String PLS_IMPORTANT_DBRESOURCE = RestFulServiceUtils.getconfig("PLS_IMPORTANT_DBRESOURCE");
                    Map<String, ConnectTnsConf> map = null;
                    if(PLS_IMPORTANT_DBRESOURCE!=null&&PLS_IMPORTANT_DBRESOURCE.equals("1")&&db_type.equalsIgnoreCase("oracle")){
                        //获取配置重要数据库配置
                        map = queryTnsConfigue(dispose_list);
                    }
                    dbTenantBean.setDbResourceBeans(new ArrayList<>());
                    if(!dispose_list.isEmpty()){
                        for (int j=0;j<dispose_list.size();j++){
                            if(dispose_list.get(j).getEntity_type().equalsIgnoreCase(db_type)){
                                DBResurceBean dbResurceBean =new DBResurceBean();
                                dbResurceBean.setResource_id(dispose_list.get(j).getDispose_type());
                                dbResurceBean.setResource_name(dispose_list.get(j).getDispose_desc());
                                dbResurceBean.setResource_type(dispose_list.get(j).getEntity_type());
                                List<com.newland.edc.cct.dataasset.entity.model.javabean.DataDispBean> con_list = queryTenantConn(tabTenantBeans.get(i).getTenant_id(),dispose_list.get(j).getDispose_type());
                                dbResurceBean.setDbConnectionBeans(new ArrayList<>());
                                if(!con_list.isEmpty()){
                                    for (int m=0;m<con_list.size();m++){
                                        //过滤重要数据配置
                                        if(map!=null){
                                            if(map.get(con_list.get(m).getConn_id())!=null){
                                                DBConnectionBean dbConnectionBean = new DBConnectionBean();
                                                dbConnectionBean.setConn_id(con_list.get(m).getConn_id());
                                                dbConnectionBean.setConn_name(con_list.get(m).getConn_name());
                                                dbResurceBean.getDbConnectionBeans().add(dbConnectionBean);
                                            }
                                        }else{
                                            DBConnectionBean dbConnectionBean = new DBConnectionBean();
                                            dbConnectionBean.setConn_id(con_list.get(m).getConn_id());
                                            dbConnectionBean.setConn_name(con_list.get(m).getConn_name());
                                            dbResurceBean.getDbConnectionBeans().add(dbConnectionBean);
                                        }
                                    }
                                }
                                dbTenantBean.getDbResourceBeans().add(dbResurceBean);
                            }
                        }
                    }
                    dbTenantBeans.add(dbTenantBean);
                }
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
        return dbTenantBeans;
    }

    @Override
    public Map<String,List<DBResurceBean>> queryResource(String db_type) throws Exception{
        Map<String,List<DBResurceBean>> map = new HashMap<>();
        try {
            List<TabTenantBean> tabTenantBeans = getTenants();
            if(!tabTenantBeans.isEmpty()){
                for (int i=0;i<tabTenantBeans.size();i++){
                    List<DBResurceBean> list = new ArrayList<>();
                    List<DataDispBean> dispose_list = queryTenantResouce(tabTenantBeans.get(i).getTenant_id(),null);
                    String PLS_IMPORTANT_DBRESOURCE = RestFulServiceUtils.getconfig("PLS_IMPORTANT_DBRESOURCE");
                    Map<String, ConnectTnsConf> map1 = null;
                    if(PLS_IMPORTANT_DBRESOURCE!=null&&PLS_IMPORTANT_DBRESOURCE.equals("1")&&db_type.equalsIgnoreCase("oracle")){
                        //获取配置重要数据库配置
                        map1 = queryTnsConfigue(dispose_list);
                    }
                    if(!dispose_list.isEmpty()){
                        for (int j=0;j<dispose_list.size();j++){
                            if(dispose_list.get(j).getEntity_type().equalsIgnoreCase(db_type)){
                                DBResurceBean dbResurceBean =new DBResurceBean();
                                dbResurceBean.setResource_id(dispose_list.get(j).getDispose_type());
                                dbResurceBean.setResource_name(dispose_list.get(j).getDispose_desc());
                                dbResurceBean.setResource_type(dispose_list.get(j).getEntity_type());
                                list.add(dbResurceBean);
                                List<com.newland.edc.cct.dataasset.entity.model.javabean.DataDispBean> con_list = queryTenantConn(tabTenantBeans.get(i).getTenant_id(),dispose_list.get(j).getDispose_type());
                                if(!con_list.isEmpty()){
                                    dbResurceBean.setDbConnectionBeans(new ArrayList<>());
                                    for (int m=0;m<con_list.size();m++){
                                        //过滤重要数据配置
                                        if(map1!=null){
                                            if(map1.get(con_list.get(m).getConn_id())!=null){
                                                DBConnectionBean dbConnectionBean = new DBConnectionBean();
                                                dbConnectionBean.setConn_id(con_list.get(m).getConn_id());
                                                dbConnectionBean.setConn_name(con_list.get(m).getConn_name());
                                                dbResurceBean.getDbConnectionBeans().add(dbConnectionBean);
                                            }
                                        }else{
                                            DBConnectionBean dbConnectionBean = new DBConnectionBean();
                                            dbConnectionBean.setConn_id(con_list.get(m).getConn_id());
                                            dbConnectionBean.setConn_name(con_list.get(m).getConn_name());
                                            dbResurceBean.getDbConnectionBeans().add(dbConnectionBean);
                                        }
                                    }
                                }
                                map.put(tabTenantBeans.get(i).getTenant_id(),list);
                            }
                        }
                    }
                }
            }else{
                throw new Exception("查询租户信息失败");
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
        return map;
    }

    @Override
    public List<DBTenantBean> queryTenantPub(String userId) throws Exception{
        List<DBTenantBean> dbTenantBeans = new ArrayList<>();
        try{
            String url = RestFulServiceUtils.getServiceUrl("edc-pub-system-ms")+"/tenant/getUserTenantList";
            com.newland.edc.pub.system.tenant.model.TenantBean tenantBean = new com.newland.edc.pub.system.tenant.model.TenantBean();
            tenantBean.setUserId(Long.parseLong(userId));
            Map<String,String> headerMap = new HashMap<>();
            headerMap.put("X-SystemId","73191008");
            headerMap.put("X-NG-Token", TokenUtil.getToken());
            log.info("=====请求服务："+url);
            log.info("=====请求报文："+JSON.toJSONString(tenantBean));
            RespInfo respInfo = RestClientUtil.sendRestClient(com.newland.edc.pub.system.tenant.model.TenantBean.class,url,userId,JSON.toJSONString(tenantBean),headerMap,30*1000);
            if("0".equals(respInfo.getRespResult())){
                throw new Exception(respInfo.getRespErrorDesc());
            }
            List<com.newland.edc.pub.system.tenant.model.TenantBean> tenantBeans = (List<com.newland.edc.pub.system.tenant.model.TenantBean>)respInfo.getRespData();
            if(tenantBeans!=null&&!tenantBeans.isEmpty()){
                for (int i=0;i<tenantBeans.size();i++){
                    DBTenantBean dbTenantBean = new DBTenantBean();
                    dbTenantBean.setTenant_id(tenantBeans.get(i).getTenantId());
                    dbTenantBean.setTenant_name(tenantBeans.get(i).getTenantName());
                    dbTenantBeans.add(dbTenantBean);
                }
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
        return dbTenantBeans;
    }

    @Override
    public List<DBResurceBean> queryResourceAndConn(String tenantId,String dbType,String resource_id) throws Exception{
        List<DBResurceBean> dbResurceBeans = new ArrayList<>();
        try {
            String url = RestFulServiceUtils.getServiceUrl("edc-pub-system-ms")+"/conn/conn";
            ParamConn paramConn = new ParamConn();
            paramConn.setTenantId(tenantId);
            if(StringUtils.isNotBlank(dbType)){
                paramConn.setResourceType(dbType);
            }
            if(StringUtils.isNotBlank(resource_id)){
                paramConn.setResourceId(resource_id);
            }
            Map<String,String> headerMap = new HashMap<>();
            headerMap.put("X-SystemId","73191008");
            headerMap.put("X-NG-Token",TokenUtil.getToken());
            log.info("=====请求服务："+url);
            log.info("=====请求报文："+JSON.toJSONString(paramConn));
            RespInfo respInfo = RestClientUtil.sendRestClient(ConnBean.class,url,null,JSON.toJSONString(paramConn),headerMap,30*1000);
            if("0".equals(respInfo.getRespResult())){
                throw new Exception(respInfo.getRespErrorDesc());
            }
            List<ConnBean> connBeans = (List<ConnBean>)respInfo.getRespData();
            if(connBeans!=null&&!connBeans.isEmpty()){
                Map<String, DBResurceBean> dbResurceBeanMap = new HashMap<>();
                //提取资源
                for (int i=0;i<connBeans.size();i++){
                    if(dbResurceBeanMap.get(connBeans.get(i).getResourceId())==null){
                        DBResurceBean dbResurceBean = new DBResurceBean();
                        dbResurceBean.setResource_id(connBeans.get(i).getResourceId());
                        dbResurceBean.setResource_name(connBeans.get(i).getResourceName());
                        dbResurceBean.setResource_type(connBeans.get(i).getResourceType());
                        dbResurceBeans.add(dbResurceBean);
                        dbResurceBeanMap.put(connBeans.get(i).getResourceId(),dbResurceBean);
                    }
                }
                //提取连接
                for (int i=0;i<connBeans.size();i++){
                    if(dbResurceBeanMap.get(connBeans.get(i).getResourceId())!=null){
                        DBConnectionBean dbConnectionBean = new DBConnectionBean();
                        dbConnectionBean.setConn_id(connBeans.get(i).getConnId());
                        dbConnectionBean.setConn_name(connBeans.get(i).getConnName());
                        if(dbResurceBeanMap.get(connBeans.get(i).getResourceId()).getDbConnectionBeans()==null){
                            dbResurceBeanMap.get(connBeans.get(i).getResourceId()).setDbConnectionBeans(new ArrayList<>());
                        }
                        dbResurceBeanMap.get(connBeans.get(i).getResourceId()).getDbConnectionBeans().add(dbConnectionBean);
                    }
                }
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
        return dbResurceBeans;
    }



}
