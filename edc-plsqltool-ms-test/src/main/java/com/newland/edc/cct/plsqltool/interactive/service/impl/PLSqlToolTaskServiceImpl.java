package com.newland.edc.cct.plsqltool.interactive.service.impl;

import com.alibaba.fastjson.JSON;
import com.newland.bd.ms.core.model.RespInfo;
import com.newland.bd.utils.commons.UUIDUtils;
import com.newland.bi.util.logger.BaseLogger;
import com.newland.edc.cct.dgw.query.model.javabean.DgwQueryBean;
import com.newland.edc.cct.dgw.utils.ftputils.FTPClientFactory;
import com.newland.edc.cct.dgw.utils.ftputils.FTPClientUtil;
import com.newland.edc.cct.dgw.utils.rest.RestClientUtil;
import com.newland.edc.cct.dgw.utils.rest.RestFulServiceUtils;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.*;
import com.newland.edc.cct.plsqltool.interactive.service.PLSqlToolExecuteService;
import com.newland.edc.cct.plsqltool.interactive.service.PLSqlToolQueryService;
import com.newland.edc.cct.plsqltool.interactive.service.PLSqlToolTaskService;
import com.newland.edc.cct.plsqltool.interactive.service.PLSqlToolTestService;
import com.newland.edc.cct.plsqltool.interactive.tool.CallableTask;
import com.newland.edc.cct.plsqltool.interactive.tool.FtpFileSynchronousTask;
import com.newland.edc.cct.plsqltool.interactive.util.DataCatch;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.text.SimpleDateFormat;
import java.util.*;

@Repository("com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolTaskServiceImpl")
public class PLSqlToolTaskServiceImpl implements PLSqlToolTaskService {

    private static BaseLogger log = (BaseLogger) BaseLogger.getLogger(PLSqlToolTaskServiceImpl.class);

    @Resource(name ="com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolQueryServiceImpl")
    private PLSqlToolQueryService plSqlToolQueryService;

    @Resource(name ="com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolTestServiceImpl")
    private PLSqlToolTestService plSqlToolTestService;

    @Resource(name ="com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolExecuteServiceImpl")
    private PLSqlToolExecuteService plSqlToolExecuteService;

    @Override
    public void executeAdapter(TestGrammarBean testGrammarBean) throws Exception{
        try {
            for (int i=0;i<testGrammarBean.getTestSqlBeans().size();i++){
                testGrammarBean.getTestSqlBeans().get(i).setUser_id(testGrammarBean.getUser_id());
                testGrammarBean.getTestSqlBeans().get(i).setTenant_id(testGrammarBean.getTenant_id());
                testGrammarBean.getTestSqlBeans().get(i).setResource_id(testGrammarBean.getResource_id());
                testGrammarBean.getTestSqlBeans().get(i).setConn_id(testGrammarBean.getConn_id());
                testGrammarBean.getTestSqlBeans().get(i).setDbType(testGrammarBean.getDbType());
                testGrammarBean.getTestSqlBeans().get(i).setSys_id(testGrammarBean.getSys_id());
                testGrammarBean.getTestSqlBeans().get(i).setRun_mode("asy");
            }

            //插入任务日志
            List<ExecuteLog> executeLogs = plSqlToolQueryService.insertExecuteLogs(testGrammarBean.getTestSqlBeans(),plSqlToolQueryService.getIp(),"0");
            //更新执行状态
            for(int i=0;i<executeLogs.size();i++){
                plSqlToolQueryService.updateExecuteLog(executeLogs.get(i),"1");
            }
            //放入线程池
            CallableTask callableTask = new CallableTask(testGrammarBean.getTestSqlBeans());
            if(testGrammarBean.getDbType().equalsIgnoreCase("oracle")){
                DataCatch.getOracleAsyExecutePools().execute(callableTask);
            }else if(testGrammarBean.getDbType().equalsIgnoreCase("hive")){
                DataCatch.getHiveAsyExecutePools().execute(callableTask);
            }else if(testGrammarBean.getDbType().equalsIgnoreCase("db2")){
                DataCatch.getDb2AsyExecutePools().execute(callableTask);
            }else if(testGrammarBean.getDbType().equalsIgnoreCase("greenplum")){
                DataCatch.getGreenplumAsyExecutePools().execute(callableTask);
            }else if(testGrammarBean.getDbType().equalsIgnoreCase("mysql")){
                DataCatch.getMysqlAsyExecutePools().execute(callableTask);
            }else if(testGrammarBean.getDbType().equalsIgnoreCase("gbase")){
                DataCatch.getGbaseAsyExecutePools().execute(callableTask);
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
    }

    @Override
    public void asyexecute(TestGrammarBean testGrammarBean) throws Exception{
        try {
            for (int i=0;i<testGrammarBean.getTestSqlBeans().size();i++){
                testGrammarBean.getTestSqlBeans().get(i).setUser_id(testGrammarBean.getUser_id());
                testGrammarBean.getTestSqlBeans().get(i).setTenant_id(testGrammarBean.getTenant_id());
                testGrammarBean.getTestSqlBeans().get(i).setResource_id(testGrammarBean.getResource_id());
                testGrammarBean.getTestSqlBeans().get(i).setConn_id(testGrammarBean.getConn_id());
                testGrammarBean.getTestSqlBeans().get(i).setDbType(testGrammarBean.getDbType());
                testGrammarBean.getTestSqlBeans().get(i).setSys_id(testGrammarBean.getSys_id());
                testGrammarBean.getTestSqlBeans().get(i).setRun_mode("asy");
            }
            ExecuteLog executeLog = new ExecuteLog();
            executeLog.setGroup_id(testGrammarBean.getTestSqlBeans().get(0).getGroup_id());
            this.plSqlToolQueryService.updateExecuteLog(executeLog,"1");
            CallableTask callableTask = new CallableTask(testGrammarBean.getTestSqlBeans());
            if(testGrammarBean.getDbType().equalsIgnoreCase("oracle")){
                DataCatch.getOracleAsyExecutePools().execute(callableTask);
            }else if(testGrammarBean.getDbType().equalsIgnoreCase("hive")){
                DataCatch.getHiveAsyExecutePools().execute(callableTask);
            }else if(testGrammarBean.getDbType().equalsIgnoreCase("db2")){
                DataCatch.getDb2AsyExecutePools().execute(callableTask);
            }else if(testGrammarBean.getDbType().equalsIgnoreCase("greenplum")){
                DataCatch.getGreenplumAsyExecutePools().execute(callableTask);
            }else if(testGrammarBean.getDbType().equalsIgnoreCase("mysql")){
                DataCatch.getMysqlAsyExecutePools().execute(callableTask);
            }else if(testGrammarBean.getDbType().equalsIgnoreCase("gbase")){
                DataCatch.getGbaseAsyExecutePools().execute(callableTask);
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
    }

    @Override
    public DgwSqlToolResult synexecute(TestSqlBean testSqlBean) throws Exception{
        DgwSqlToolResult dgwSqlToolResult = new DgwSqlToolResult();
        try{
            testSqlBean.setRun_mode("syn");
            List<TestSqlBean> testSqlBeans = new ArrayList<>();
            testSqlBeans.add(testSqlBean);
            List<ExecuteLog> executeLogs = this.plSqlToolQueryService.insertExecuteLogs(testSqlBeans,this.plSqlToolQueryService.getIp(),"0");
            List<DgwSqlToolResult> dgwSqlToolResults = new ArrayList<>();
            CallableTask callableTask = new CallableTask(testSqlBeans);
            if(testSqlBean.getDbType().equalsIgnoreCase("oracle")){
                for (int i=0;i<executeLogs.size();i++){
                    this.plSqlToolQueryService.updateExecuteLog(executeLogs.get(i),"1");
                }
                DataCatch.getOracleSynExecutePools().execute(callableTask);
                //调用get方法阻塞代码，任务结束后继续执行后部分或断开连接强制报错提前结束等待
                dgwSqlToolResults = (List<DgwSqlToolResult>) DataCatch.getOracleSynExecutePools().getResult(testSqlBean.getGroup_id());
            }else if(testSqlBean.getDbType().equalsIgnoreCase("hive")){
                for (int i=0;i<executeLogs.size();i++){
                    this.plSqlToolQueryService.updateExecuteLog(executeLogs.get(i),"1");
                }
                DataCatch.getHiveSynExecutePools().execute(callableTask);
                //调用get方法阻塞代码，任务结束后继续执行后部分或断开连接强制报错提前结束等待
                dgwSqlToolResults = (List<DgwSqlToolResult>) DataCatch.getHiveSynExecutePools().getResult(testSqlBean.getGroup_id());
            }else if(testSqlBean.getDbType().equalsIgnoreCase("db2")){
                for (int i=0;i<executeLogs.size();i++){
                    this.plSqlToolQueryService.updateExecuteLog(executeLogs.get(i),"1");
                }
                DataCatch.getDb2SynExecutePools().execute(callableTask);
                //调用get方法阻塞代码，任务结束后继续执行后部分或断开连接强制报错提前结束等待
                dgwSqlToolResults = (List<DgwSqlToolResult>) DataCatch.getDb2SynExecutePools().getResult(testSqlBean.getGroup_id());
            }else if(testSqlBean.getDbType().equalsIgnoreCase("greenplum")){
                for (int i=0;i<executeLogs.size();i++){
                    this.plSqlToolQueryService.updateExecuteLog(executeLogs.get(i),"1");
                }
                DataCatch.getGreenplumSynExecutePools().execute(callableTask);
                //调用get方法阻塞代码，任务结束后继续执行后部分或断开连接强制报错提前结束等待
                dgwSqlToolResults = (List<DgwSqlToolResult>) DataCatch.getGreenplumSynExecutePools().getResult(testSqlBean.getGroup_id());
            }else if(testSqlBean.getDbType().equalsIgnoreCase("mysql")){
                for (int i=0;i<executeLogs.size();i++){
                    this.plSqlToolQueryService.updateExecuteLog(executeLogs.get(i),"1");
                }
                DataCatch.getMysqlSynExecutePools().execute(callableTask);
                //调用get方法阻塞代码，任务结束后继续执行后部分或断开连接强制报错提前结束等待
                dgwSqlToolResults = (List<DgwSqlToolResult>) DataCatch.getMysqlSynExecutePools().getResult(testSqlBean.getGroup_id());
            }else if(testSqlBean.getDbType().equalsIgnoreCase("gbase")){
                for (int i=0;i<executeLogs.size();i++){
                    this.plSqlToolQueryService.updateExecuteLog(executeLogs.get(i),"1");
                }
                DataCatch.getGbaseSynExecutePools().execute(callableTask);
                //调用get方法阻塞代码，任务结束后继续执行后部分或断开连接强制报错提前结束等待
                dgwSqlToolResults = (List<DgwSqlToolResult>) DataCatch.getGbaseSynExecutePools().getResult(testSqlBean.getGroup_id());
            }
            if(!dgwSqlToolResults.isEmpty()){
                dgwSqlToolResult = dgwSqlToolResults.get(0);
                if(dgwSqlToolResult!=null){
                    if(dgwSqlToolResult.getIs_success()!=null&&dgwSqlToolResult.getIs_success()){
                        return dgwSqlToolResult;
                    }else{
                        throw new Exception(dgwSqlToolResult.getErr_msg());
                    }
                }
            }else{
                throw new Exception("任务异常无法获取执行结果");
            }
        } catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
        return dgwSqlToolResult;
    }

    @Override
    public void createExportTask(ExportLog exportLogBean) throws Exception{
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            exportLogBean.setStatus("3");
            exportLogBean.setCreate_time(sdf.format(new Date()));
            exportLogBean.setExport_id(UUIDUtils.getUUID());
            TestGrammarBean testGrammarBean = new TestGrammarBean();
            testGrammarBean.setSql(exportLogBean.getSqlstring());
            testGrammarBean.setDbType(exportLogBean.getDb_type());
            testGrammarBean.setTenant_id(exportLogBean.getTenant_id());
            testGrammarBean.setResource_id(exportLogBean.getResource_id());
            testGrammarBean.setConn_id(exportLogBean.getConn_id());
            RespInfo respInfo = plSqlToolTestService.grammarAdapter(testGrammarBean);
            if(respInfo.getRespResult().equals("0")){
                exportLogBean.setExport_result("校验异常");
                exportLogBean.setStatus("0");
                exportLogBean.setDownload_status("0");
                this.plSqlToolQueryService.insertExportLog(exportLogBean);
                throw new Exception("校验异常");
            }
            if(testGrammarBean.getTestResult().equals("false")){
                exportLogBean.setExport_result(testGrammarBean.getTestErrorBean().getErrorMsg());
                exportLogBean.setStatus("0");
                exportLogBean.setDownload_status("0");
                this.plSqlToolQueryService.insertExportLog(exportLogBean);
                throw new Exception(testGrammarBean.getTestErrorBean().getErrorMsg());
            }
            if(testGrammarBean==null||testGrammarBean.getTestSqlBeans()==null){
                exportLogBean.setExport_result("校验异常");
                exportLogBean.setStatus("0");
                exportLogBean.setDownload_status("0");
                this.plSqlToolQueryService.insertExportLog(exportLogBean);
                throw new Exception("校验异常");
            }
            if(testGrammarBean.getCount()>1||testGrammarBean.getTestSqlBeans().size()>1){
                exportLogBean.setExport_result("执行导出的语句超过1");
                exportLogBean.setStatus("0");
                exportLogBean.setDownload_status("0");
                this.plSqlToolQueryService.insertExportLog(exportLogBean);
                throw new Exception("执行导出的语句超过1");
            }
            exportLogBean.setReal_sqlstring(testGrammarBean.getTestSqlBeans().get(0).getReal_sql());
            exportLogBean.setDownload_status("1");
            String MAXCOUNT = RestFulServiceUtils.getconfig("MAXCOUNT");
            if(StringUtils.isBlank(MAXCOUNT)||"-1".equals(MAXCOUNT)){
                //不做限制
            }else{
                long count = this.plSqlToolExecuteService.sqlCountTask(exportLogBean.getSqlstring(),exportLogBean.getConn_id(),exportLogBean.getDb_type());
                if(count>Long.parseLong(MAXCOUNT)){
                    exportLogBean.setExport_result("导出任务创建失败,查询数据量为"+count+"条，超出系统限制"+MAXCOUNT+"条");
                    exportLogBean.setStatus("0");
                    exportLogBean.setDownload_status("0");
                    this.plSqlToolQueryService.insertExportLog(exportLogBean);
                    throw new Exception("导出任务创建失败,查询数据量为"+count+"条，超出系统限制"+MAXCOUNT+"条");
                }
            }
            this.plSqlToolQueryService.insertExportLog(exportLogBean);
            DgwQueryBean dgwQueryBean = new DgwQueryBean();
            Map<String,String> headerMap = new HashMap<>();
            headerMap.put("X-UserId",exportLogBean.getUser_id());
            headerMap.put("X-SystemId","73191008");
            dgwQueryBean.setExecuteSql(exportLogBean.getReal_sqlstring());
            dgwQueryBean.setExecuteConnectId(exportLogBean.getConn_id());
            dgwQueryBean.setExecuteDbType(exportLogBean.getDb_type());
            dgwQueryBean.setResult_type("5");
            if("1".equals(exportLogBean.getIs_compression())){
                dgwQueryBean.setExport_is_zip("1");
            }else{
                dgwQueryBean.setExport_is_zip("0");
            }
            String reqStr = JSON.toJSONString(dgwQueryBean);
            log.info("请求网关日志："+reqStr);
            respInfo = RestClientUtil.sendRestClient(com.newland.edc.cct.dgw.query.model.bean.DgwResult.class,RestFulServiceUtils.getServiceUrl("edc-dgw-ms")+"/dgwQueryApi/exportDataBySql",exportLogBean.getUser_id(),reqStr,headerMap,30*1000);
            if(respInfo.getRespResult().equals("0")){
                exportLogBean.setStatus("0");
                exportLogBean.setDownload_status("0");
                exportLogBean.setExport_result(respInfo.getRespErrorDesc());
                this.plSqlToolQueryService.updateExportLog(exportLogBean,exportLogBean.getExport_id());
                throw new Exception(respInfo.getRespErrorDesc());
            }else{
                exportLogBean.setStatus("1");
            }
            com.newland.edc.cct.dgw.query.model.bean.DgwResult result = (com.newland.edc.cct.dgw.query.model.bean.DgwResult)respInfo.getRespData();
            if(result!=null){
                exportLogBean.setLog_id(result.getLog_id());
                exportLogBean.setTotal_count(result.getTotal_count());
            }else{
                log.info("网关返回信息为空");
                exportLogBean.setStatus("0");
                exportLogBean.setExport_result("网关返回信息为空");
                this.plSqlToolQueryService.updateExportLog(exportLogBean,exportLogBean.getExport_id());
                throw new Exception("网关返回信息为空");
            }
            if(result.getResult_ftp_info()!=null){
                exportLogBean.setFtp_filepath(result.getResult_ftp_info().getFtp_path());
                exportLogBean.setFtp_filename(result.getResult_ftp_info().getFile_name());
            }else{
                log.info("网关返回报文ftp文件信息为空");
                exportLogBean.setStatus("0");
                exportLogBean.setExport_result("网关返回报文ftp文件信息为空");
                this.plSqlToolQueryService.updateExportLog(exportLogBean,exportLogBean.getExport_id());
                throw new Exception("网关返回报文ftp文件信息为空");
            }
            this.plSqlToolQueryService.updateExportLog(exportLogBean,exportLogBean.getExport_id());
        }catch (Exception e){
            log.error(e.getMessage(),e);
            exportLogBean.setExport_result(e.getMessage());
            exportLogBean.setStatus("0");
            try {
                this.plSqlToolQueryService.updateExportLog(exportLogBean,exportLogBean.getExport_id());
            }catch (Exception e1){
                log.error(e1.getMessage(),e1);
            }
            throw e;
        }
    }

    @Value("${pls.ftp.host}")
    private String ftpHost;
    @Value("${pls.ftp.type}")
    private String ftpType;
    @Value("${pls.ftp.port}")
    private String ftpPort;
    @Value("${pls.ftp.username}")
    private String ftpUser;
    @Value("${pls.ftp.password}")
    private String ftpPwd;

    @Override
    public FtpSynchronousLog doFtpFileSynchronous(FtpFileSynchronousReq ftpFileSynchronousReq) throws Exception{
        FtpSynchronousLog ftpSynchronousLog = new FtpSynchronousLog();
        try {
            ExportLog exportLog = new ExportLog();
            exportLog.setExport_id(ftpFileSynchronousReq.getExport_id());
            List<ExportLog> exportLogs = this.plSqlToolQueryService.queryExportLog(exportLog);
            if(exportLogs.isEmpty()){
                throw new Exception("该条导出记录不存在或已被删除");
            }else if(exportLogs.size()>1){
                throw new Exception("数据库记录中存在相同id数据库记录，请排查");
            }
            exportLog = exportLogs.get(0);
            FtpConfig ftpConfig = new FtpConfig();
            ftpConfig.setId(ftpFileSynchronousReq.getFtp_config_id());
            List<FtpConfig> ftpConfigs = this.plSqlToolQueryService.queryFtpConfig(ftpConfig);
            if(ftpConfigs.isEmpty()){
                throw new Exception("查无ftp配置信息");
            }else if(ftpConfigs.size()>1){
                throw new Exception("数据库配置中存在相同id数据库记录，请排查");
            }
            FtpConfig remoteConfig = ftpConfigs.get(0);
            FtpConfig localConfig = new FtpConfig();
            localConfig.setFtp_host(ftpHost);
            localConfig.setFtp_type(ftpType);
            localConfig.setFtp_port(ftpPort);
            localConfig.setFtp_user(ftpUser);
            localConfig.setFtp_pwd(ftpPwd);
            localConfig.setDefault_path(exportLog.getFtp_filepath());
            String synTaskId = UUIDUtils.getUUID();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            ftpSynchronousLog.setLog_id(synTaskId);
            ftpSynchronousLog.setCreate_time(sdf.format(new Date()));
            ftpSynchronousLog.setStatus("0");
            ftpSynchronousLog.setUpload_filename(ftpFileSynchronousReq.getFile_name());
            ftpSynchronousLog.setExport_id(ftpFileSynchronousReq.getExport_id());
            ftpSynchronousLog.setFtp_config_id(ftpFileSynchronousReq.getFtp_config_id());
            ftpSynchronousLog.setFtp_config_name(remoteConfig.getFtp_name());
            this.plSqlToolQueryService.insertFtpSynchronousLog(ftpSynchronousLog);
            FtpFileSynchronousTask ftpFileSynchronousTask = new FtpFileSynchronousTask(synTaskId,localConfig,remoteConfig,exportLog.getFtp_filename(),ftpFileSynchronousReq.getFile_name());
            DataCatch.getFtpFileSynchronousPools().execute(ftpFileSynchronousTask);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
        return ftpSynchronousLog;
    }

    @Override
    public void deleteExportTask(ExportLogReq exportLogReq) throws Exception{
        try {
            exportLogReq = this.plSqlToolQueryService.queryExportLog(exportLogReq);
            if(Integer.parseInt(exportLogReq.getAmount())>0){
                ExportLog exportLog = exportLogReq.getExportLogs().get(0);
                if(exportLog!=null&&StringUtils.isNotBlank(exportLog.getFtp_filepath())&&StringUtils.isNotBlank(exportLog.getFtp_filename())){
                    FTPClientUtil ftpClientUtil = FTPClientFactory.getFTPClient(ftpType);
                    ftpClientUtil.connectFtp(ftpHost,ftpPort,ftpUser,ftpPwd);
                    //判断文件是否存在  并删除
                    if(ftpClientUtil.exists(exportLog.getFtp_filepath()+"/"+exportLog.getFtp_filename())){
                        ftpClientUtil.delete(exportLog.getFtp_filepath(),exportLog.getFtp_filename());
                    }
                }
//                //删除记录
//                this.plSqlToolQueryService.deleteExportLog(exportLogReq.getExport_id());
                //影藏记录
                this.plSqlToolQueryService.updateExportLog(exportLogReq.getExport_id(),"-1","","0");
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }

    }
}
