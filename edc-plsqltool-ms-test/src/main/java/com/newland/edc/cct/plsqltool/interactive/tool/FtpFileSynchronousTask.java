package com.newland.edc.cct.plsqltool.interactive.tool;

import com.newland.bd.ms.core.utils.SpringContextUtils;
import com.newland.bi.util.logger.BaseLogger;
import com.newland.edc.cct.dgw.utils.ftputils.FTPClientFactory;
import com.newland.edc.cct.dgw.utils.ftputils.FTPClientUtil;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.FtpConfig;
import com.newland.edc.cct.plsqltool.interactive.service.PLSqlToolQueryService;

import java.io.InputStream;
import java.util.concurrent.Callable;

public class FtpFileSynchronousTask implements Callable<Object> {

    private static BaseLogger log = (BaseLogger) BaseLogger.getLogger(FtpFileSynchronousTask.class);

    private PLSqlToolQueryService plSqlToolQueryService =(PLSqlToolQueryService)SpringContextUtils.getBean("com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolQueryServiceImpl");

    private String    synTaskId;
    private FtpConfig localConfig;
    private FtpConfig remoteConfig;
    private String    localFileName;
    private String    remoteFileName;

    public FtpFileSynchronousTask(String synTaskId, FtpConfig localConfig, FtpConfig remoteConfig,String localFileName,String remoteFileName){
        this.synTaskId = synTaskId;
        this.localConfig = localConfig;
        this.remoteConfig = remoteConfig;
        this.localFileName = localFileName;
        this.remoteFileName = remoteFileName;
    }

    private FtpFileSynchronousTask(){
        super();
    }

    @Override
    public Object call() throws Exception{
        FTPClientUtil localFtpClient = null;
        FTPClientUtil remoteFtpClient = null;
        InputStream inputStream = null;
        try {
            localFtpClient = FTPClientFactory.getFTPClient(localConfig.getFtp_type());
            remoteFtpClient = FTPClientFactory.getFTPClient(remoteConfig.getFtp_type());
            localFtpClient.connectFtp(localConfig.getFtp_host(),localConfig.getFtp_port(),localConfig.getFtp_user(),localConfig.getFtp_pwd());
            inputStream = localFtpClient.downloadStream(localConfig.getDefault_path(),localFileName);
            remoteFtpClient.connectFtp(remoteConfig.getFtp_host(),remoteConfig.getFtp_port(),remoteConfig.getFtp_user(),remoteConfig.getFtp_pwd());
//            remoteFtpClient.upload(inputStream,remoteConfig.getDefault_path(),remoteFileName,remoteConfig.getFile_encoding());
            localFtpClient.closeFTPConnect();
            plSqlToolQueryService.updateFtpSynchronousLog(synTaskId,"1",null);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            plSqlToolQueryService.updateFtpSynchronousLog(synTaskId,"-1",e.getMessage());
        }finally {
            if(remoteFtpClient!=null){
                remoteFtpClient.closeFTPConnect();
            }
            if(localFtpClient!=null){
                localFtpClient.closeFTPConnect();
            }
        }
        return null;
    }

}
