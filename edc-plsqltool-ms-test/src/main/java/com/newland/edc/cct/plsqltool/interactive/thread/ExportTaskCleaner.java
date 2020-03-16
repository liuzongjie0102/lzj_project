package com.newland.edc.cct.plsqltool.interactive.thread;

import com.newland.bi.util.logger.BaseLogger;
import com.newland.edc.cct.dgw.utils.ftputils.FTPClientFactory;
import com.newland.edc.cct.dgw.utils.ftputils.FTPClientUtil;
import com.newland.edc.cct.dgw.utils.rest.RestFulServiceUtils;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.ExportLog;
import com.newland.edc.cct.plsqltool.interactive.service.PLSqlToolQueryService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

@Component // 此注解必加
public class ExportTaskCleaner {

    private static BaseLogger log = (BaseLogger) BaseLogger.getLogger(ExportTaskCleaner.class);

    @Resource(name ="com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolQueryServiceImpl")
    private PLSqlToolQueryService plSqlToolQueryService;

    @Value("${ExportTaskCleaner}")
    private String quartzSwitch;

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

//    @Scheduled(cron = "${Export.Task.Cleaner}")
    public void start() {
        if("true".equalsIgnoreCase(quartzSwitch)){
            FTPClientUtil ftpClientUtil = null;
            try {
                String PLS_DOWNLOAD_FILE_KEEP = RestFulServiceUtils.getconfig("PLS_DOWNLOAD_FILE_KEEP");
                List<ExportLog> exportLogBeanList;
                if(StringUtils.isNotBlank(PLS_DOWNLOAD_FILE_KEEP)){
                    exportLogBeanList = this.plSqlToolQueryService.queryTimeoutExportLog(Integer.parseInt(PLS_DOWNLOAD_FILE_KEEP));
                }else{
                    exportLogBeanList = this.plSqlToolQueryService.queryTimeoutExportLog(7);
                }
                if(!exportLogBeanList.isEmpty()){
                    ftpClientUtil = FTPClientFactory.getFTPClient(ftpType);
                    ftpClientUtil.connectFtp(ftpHost,ftpPort,ftpUser,ftpPwd);
                    for (int i=0;i<exportLogBeanList.size();i++){
                        if(StringUtils.isNotBlank(exportLogBeanList.get(i).getFtp_filename())&&StringUtils.isNotBlank(exportLogBeanList.get(i).getFtp_filepath())){
                            if(ftpClientUtil.exists(exportLogBeanList.get(i).getFtp_filepath()+"/"+exportLogBeanList.get(i).getFtp_filename())){
                                ftpClientUtil.delete(exportLogBeanList.get(i).getFtp_filepath(),exportLogBeanList.get(i).getFtp_filename());
                            }
                        }
//                        this.plSqlToolQueryService.deleteExportLog(exportLogBeanList.get(i).getExport_id());
                        this.plSqlToolQueryService.updateExportLog(exportLogBeanList.get(i).getExport_id(),null,"","0");
                    }
                }
            }catch (Exception e){
                log.error(e.getMessage(),e);
            }finally {
                if(ftpClientUtil!=null){
                    try {
                        ftpClientUtil.closeFTPConnect();
                    }catch (Exception e){
                        log.error(e.getMessage(),e);
                    }
                }
            }
        }
    }
}
