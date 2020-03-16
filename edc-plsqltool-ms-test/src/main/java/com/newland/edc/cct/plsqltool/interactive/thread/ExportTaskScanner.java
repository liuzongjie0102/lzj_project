package com.newland.edc.cct.plsqltool.interactive.thread;

import com.alibaba.fastjson.JSON;
import com.newland.bd.ms.core.model.RespInfo;
import com.newland.bi.util.logger.BaseLogger;
import com.newland.edc.cct.dgw.api.model.DgwTaskStatusReqBean;
import com.newland.edc.cct.dgw.api.model.DgwTaskStatusRespBean;
import com.newland.edc.cct.dgw.utils.rest.RestClientUtil;
import com.newland.edc.cct.dgw.utils.rest.RestFulServiceUtils;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.ExportLog;
import com.newland.edc.cct.plsqltool.interactive.service.PLSqlToolQueryService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component // 此注解必加
public class ExportTaskScanner {

    private static BaseLogger log = (BaseLogger) BaseLogger.getLogger(ExportTaskScanner.class);

    @Resource(name ="com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolQueryServiceImpl")
    private PLSqlToolQueryService plSqlToolQueryService;

    @Value("${ExportTaskScanner}")
    private String quartzSwitch;

    private boolean synLock = false;

//    @Scheduled(cron = "${Export.Task.Scanner}")
    public void start() {
        if("true".equalsIgnoreCase(quartzSwitch)&&!synLock){
            synLock = true;
            try {
                //更新导出超时记录
                this.plSqlToolQueryService.updateExportLogQuartz();
                List<ExportLog> exportLogBeanList = this.plSqlToolQueryService.queryExportLogQuartz();
                Map<String, ExportLog> stringExportLogBeanMap = new HashMap<>();
                if(exportLogBeanList.isEmpty()){
                    synLock = false;
                    log.info("===========================未检索到需要刷新的记录");
                }else{
                    int querySize =100;
                    List<String> logIds = new ArrayList<>();
                    for (int i=0;i<exportLogBeanList.size();i++){
                        logIds.add(exportLogBeanList.get(i).getLog_id());
                        stringExportLogBeanMap.put(exportLogBeanList.get(i).getLog_id(),exportLogBeanList.get(i));
                        if(logIds.size()==querySize){
                            DgwTaskStatusReqBean dgwTaskStatusReqBean = new DgwTaskStatusReqBean();
                            dgwTaskStatusReqBean.setLog_list(logIds);
                            String dgwUrl = RestFulServiceUtils.getServiceUrl("edc-dgw-ms")+"/dataGatewayQuery/queryDgwFtpFileTaskStatus";
                            RespInfo respInfo = RestClientUtil.sendRestClient(DgwTaskStatusRespBean.class, dgwUrl ,null, JSON.toJSONString(dgwTaskStatusReqBean));
                            if(respInfo==null){
                                synLock = false;
                                throw new Exception("===========================请求网关接口失败");
                            }
                            if(!respInfo.getRespResult().equals("1")){
                                synLock = false;
                                throw new Exception("==========================="+respInfo.getRespErrorDesc());
                            }
                            if(respInfo.getRespData()==null){
                                synLock = false;
                                throw new Exception("===========================返回数据为空");
                            }
                            List<DgwTaskStatusRespBean> dgwTaskStatusRespBeans = (List<DgwTaskStatusRespBean>)respInfo.getRespData();
                            //更新导出流水状态
                            if(dgwTaskStatusRespBeans!=null){
                                for (int j=0;j<dgwTaskStatusRespBeans.size();j++){
                                    try {
                                        if(dgwTaskStatusRespBeans.get(j).getLog_status().equals("2")){//文件上传成功
                                            this.plSqlToolQueryService.updateExportLog(stringExportLogBeanMap.get(dgwTaskStatusRespBeans.get(j).getLog_id()).getExport_id(),"2",null,null);
                                        }else if(dgwTaskStatusRespBeans.get(j).getLog_status().equals("3")){//文件上传异常
                                            if(StringUtils.isBlank(dgwTaskStatusRespBeans.get(j).getErr_msg())){
                                                this.plSqlToolQueryService.updateExportLog(stringExportLogBeanMap.get(dgwTaskStatusRespBeans.get(j).getLog_id()).getExport_id(),"0","文件导出异常","0");
                                            }else{
                                                this.plSqlToolQueryService.updateExportLog(stringExportLogBeanMap.get(dgwTaskStatusRespBeans.get(j).getLog_id()).getExport_id(),"0",dgwTaskStatusRespBeans.get(j).getErr_msg(),"0");
                                            }
                                        }
                                    }catch (Exception e){
                                        log.info("===========================更新"+dgwTaskStatusRespBeans.get(j).getLog_id()+"上传状态失败，失败原因："+e.getMessage());
                                    }
                                }
                            }
                        }else if(i+1==exportLogBeanList.size()){
                            DgwTaskStatusReqBean dgwTaskStatusReqBean = new DgwTaskStatusReqBean();
                            dgwTaskStatusReqBean.setLog_list(logIds);
                            String dgwUrl = RestFulServiceUtils.getServiceUrl("edc-dgw-ms")+"/dataGatewayQuery/queryDgwFtpFileTaskStatus";
                            RespInfo respInfo = RestClientUtil.sendRestClient(DgwTaskStatusRespBean.class, dgwUrl ,null, JSON.toJSONString(dgwTaskStatusReqBean));
                            if(respInfo==null){
                                synLock = false;
                                throw new Exception("===========================请求网关接口失败");
                            }
                            if(!respInfo.getRespResult().equals("1")){
                                synLock = false;
                                throw new Exception("==========================="+respInfo.getRespErrorDesc());
                            }
                            if(respInfo.getRespData()==null){
                                synLock = false;
                                throw new Exception("===========================返回数据为空");
                            }
                            List<DgwTaskStatusRespBean> dgwTaskStatusRespBeans = (List<DgwTaskStatusRespBean>)respInfo.getRespData();
                            //更新导出流水状态
                            if(dgwTaskStatusRespBeans!=null){
                                for (int j=0;j<dgwTaskStatusRespBeans.size();j++){
                                    try {
                                        if(dgwTaskStatusRespBeans.get(j).getLog_status().equals("2")){//文件上传成功
                                            this.plSqlToolQueryService.updateExportLog(stringExportLogBeanMap.get(dgwTaskStatusRespBeans.get(j).getLog_id()).getExport_id(),"2",null,null);
                                        }else if(dgwTaskStatusRespBeans.get(j).getLog_status().equals("3")){//文件上传异常
                                            if(StringUtils.isBlank(dgwTaskStatusRespBeans.get(j).getErr_msg())){
                                                this.plSqlToolQueryService.updateExportLog(stringExportLogBeanMap.get(dgwTaskStatusRespBeans.get(j).getLog_id()).getExport_id(),"0","文件导出异常","0");
                                            }else{
                                                this.plSqlToolQueryService.updateExportLog(stringExportLogBeanMap.get(dgwTaskStatusRespBeans.get(j).getLog_id()).getExport_id(),"0",dgwTaskStatusRespBeans.get(j).getErr_msg(),"0");
                                            }
                                        }
                                    }catch (Exception e){
                                        log.info("===========================更新"+dgwTaskStatusRespBeans.get(j).getLog_id()+"上传状态失败，失败原因："+e.getMessage());
                                    }
                                }
                            }
                        }
                    }
                }
                synLock = false;
            }catch (Exception e){
                synLock = false;
                log.error(e.getMessage(),e);
            }
        }
    }

}
