package com.newland.edc.cct.plsqltool.interactive.service.impl;

import com.alibaba.fastjson.JSON;
import com.newland.bd.model.cfg.GeneralDBBean;
import com.newland.bd.model.var.ConnInfo;
import com.newland.bd.utils.commons.UUIDUtils;
import com.newland.bi.util.common.StringUtil;
import com.newland.bi.util.logger.BaseLogger;
import com.newland.bi.webservice.common.MessageHandler;
import com.newland.edc.cct.dataasset.entity.model.DataAssetServiceReqData;
import com.newland.edc.cct.dataasset.entity.model.DataAssetServiceRequestObject;
import com.newland.edc.cct.dataasset.entity.model.DataAssetServiceRespData;
import com.newland.edc.cct.dataasset.entity.model.DataAssetServiceResponseObject;
import com.newland.edc.cct.dataasset.entity.model.javabean.ImporterInfo;
import com.newland.edc.cct.dataasset.entity.model.javabean.LoadDataLog;
import com.newland.edc.cct.dgw.utils.ftputils.FTPClientFactory;
import com.newland.edc.cct.dgw.utils.ftputils.FTPClientUtil;
import com.newland.edc.cct.dgw.utils.rest.RestFulServiceUtils;
import com.newland.edc.cct.plsqltool.interactive.client.ImportEntityBizClient;
import com.newland.edc.cct.plsqltool.interactive.dao.ImportEntityDao;
import com.newland.edc.cct.plsqltool.interactive.dataimport.DataImportUtil;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.ImportRequestInfo;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.ImportResponseInfo;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.PartitionInfo;
import com.newland.edc.cct.plsqltool.interactive.service.ImportEntityBiz;
import com.newland.edc.cct.plsqltool.interactive.util.DataSourceAccess;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

;

@Service("com.newland.edc.cct.plsqltool.interactive.service.impl.ImportEntityBizImpl")
public class ImportEntityBizImpl implements ImportEntityBiz {
    BaseLogger logger = (BaseLogger) BaseLogger.getLogger(ImportEntityBizImpl.class);
    @Resource(name = "com.newland.edc.cct.plsqltool.interactive.dao.impl.ImportEntityDaoImpl")
    private ImportEntityDao dao;

    @Autowired
    private ImportEntityBizClient client;

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
    @Value("${ImporterEntityThread.default:false}")
    private boolean defaultExecute;
    @Value("${env.type}")
    private String envType;

    @Value("${Importer.hive.tmpHdfsDir:/tmp/dataFile}")
    private String tmpHdfsDir;

    public ImportEntityBizImpl() {
        // TODO Auto-generated constructor stub
    }

    /**
     * 查询导入日志
     * @param requestObject
     * @return
     * @throws Exception
     */
    public DataAssetServiceResponseObject getLoadDataLogList(DataAssetServiceRequestObject requestObject) throws Exception {
        DataAssetServiceReqData reqData=(DataAssetServiceReqData) MessageHandler.newInstance().getReqData(requestObject);
        int startPage = NumberUtils.toInt(requestObject.getBodyReq().getStart(), 1);
        int pageCount = NumberUtils.toInt(requestObject.getBodyReq().getPageCount(), 5);
        String user_id = requestObject.getHeaderReq().getUser().getClientId();
        LoadDataLog req_bean=reqData.getLoadDataLog();
        DataAssetServiceResponseObject responseObject=new DataAssetServiceResponseObject();
        DataAssetServiceRespData respData=new DataAssetServiceRespData();
        List<LoadDataLog> dataLogs=new ArrayList<LoadDataLog>();
        Map<String,Object> map=this.dao.getLoadDataLogList (req_bean,startPage,pageCount);
        if(map.get("data")!=null){
            dataLogs=(List<LoadDataLog>) map.get("data");
        }
        respData.setDataLogs(dataLogs);
        MessageHandler.newInstance().setRespData(responseObject, respData);
        // 如果有返回总记录数，设置总记录数
        MessageHandler.newInstance().setTotalCount(responseObject, String.valueOf(map.get("total_num")));
        // 如果返回DBresult，设置应答头
        MessageHandler.newInstance().setHeaderResp(responseObject, "0","0", "");

        return responseObject;
    }

    /**
     * 新CSV文件导入函数
     * @param importerInfo
     */
    public void dealCSVEntityByLoadNew(ImporterInfo importerInfo ){
        if(importerInfo==null)
        {
            return;
        }
        String load_seq=importerInfo.getSeq_id();
        File file = null;
        FTPClientUtil ftpClientUtil = null;
        final ImportRequestInfo requestInfo = new ImportRequestInfo();;
        try{

            //HIVE集群执行判断
            ConnInfo connInfo = DataSourceAccess.getConnInfoByConnId(importerInfo.getConn_id());
            GeneralDBBean dbCfg = DataSourceAccess.getDBCfgByConnInfo(connInfo);
            if (dbCfg.getType().isEmpty()){
                throw new Exception("无法识别连接的url或者url为空");
            }
            String dbType = dbCfg.getType().toLowerCase();
            //envType 为all 不分发全部执行
            if (!envType.equalsIgnoreCase("all")){
                if (dbType.equalsIgnoreCase("hive")){
                    String currEnvType = DataSourceAccess.getEnvTypeByResourceId(importerInfo.getResource_id());
                    if ((StringUtils.isBlank(currEnvType)&&defaultExecute) || currEnvType.equals(envType)){
                    }else {
                        logger.info("交互式查询-导入线程, load_seq="+importerInfo.getSeq_id()+"当前env_type="+envType+", 目标envType="+currEnvType+"不执行");
                        return;
                    }
                }else if (!defaultExecute){
                    logger.info("交互式查询-导入线程, load_seq="+importerInfo.getSeq_id()+"不执行  =====结束====");
                    return;
                }
            }

            String realFtpPath=importerInfo.getRealFtpPath();
            String ftpFileName = UUIDUtils.getUUID() + ".csv";
            dao.updateLoadDataLog("0", "0", dao.LOADING, load_seq);

            // 附件请求bean
            ftpClientUtil = FTPClientFactory.getFTPClient(ftpType);
            ftpClientUtil.connectFtp(ftpHost,ftpPort,ftpUser,ftpPwd);
            // 下载文件
            file=new File(ftpFileName) ;
            String fileName =realFtpPath. substring(realFtpPath.lastIndexOf("/")+1);
            String filePath =realFtpPath. substring(0,realFtpPath.lastIndexOf("/")+1);
            String localFilePath = file.getAbsolutePath().substring(0, file.getAbsolutePath().length()-file.getName().length());
            ftpClientUtil.download(localFilePath,filePath, ftpFileName, fileName);
            String partition= importerInfo.getPartitionData();
            List<String> partitionList = null;
            if(!StringUtil.isBlank(partition)){
                partition = partition.toLowerCase();
                partitionList = Arrays.asList(partition.split(","));
            }else{
                partition="";
                partitionList = new ArrayList<>();
            }
            logger.info("partition:::::::::"+partition);
            logger.info("importerInfo:::::::::"+JSON.toJSONString(importerInfo));

            String tableName=importerInfo.getPhy_tab_name().toLowerCase();
            String overwrite_type=importerInfo.getOverwrite_type();//0覆盖 1追加
            String outputMode="cover";   //输出方式 cover:覆盖, append:追加
            if("1".equals(overwrite_type))
            {
                outputMode="append";
            }

            //构建请求参数
            String url = RestFulServiceUtils.getServiceUrl("nl-edc-cct-sys-ms");
            if(!url.substring(url.length()-1).equals("/")){
                url = url + "/";
            }
            requestInfo.setEnvService(url);
            requestInfo.setDbConnId(importerInfo.getConn_id());
            requestInfo.setOutputMode(outputMode);
            PartitionInfo partitionInfo = new PartitionInfo();
            partitionInfo.setPartitionList(partitionList);
            requestInfo.setPartitionInfo(partitionInfo);
            requestInfo.setSourceDataFilePath(localFilePath);
            requestInfo.setSourceDataFileName(ftpFileName);
            requestInfo.setTargetTable(tableName);
            requestInfo.setTmpHdfsDir(tmpHdfsDir);

            ImportResponseInfo responseInfo = null;
            switch(dbType) {
            case "oracle":
            case "db2":
            case "mysql":
            case "greenplum":
                responseInfo = client.dealCSVByJDBC(requestInfo,importerInfo);
                break;
            case "hive":
                client.checkFileHive(requestInfo,importerInfo);
                responseInfo = DataImportUtil.execute(requestInfo);
                break;
            default:
                responseInfo.setResultCode(0);
                responseInfo.setDesc("不支持"+dbType+"数据库类型导入");
            }

            if (responseInfo.getResultCode() != 0){
                dao.updateLoadDataLog(responseInfo.getSuccessCount()+"", responseInfo.getFailureCount()+"", dao.LOADED, load_seq, "导入成功");
            }else{
                dao.updateLoadDataLog(responseInfo.getSuccessCount()+"", responseInfo.getFailureCount()+"", dao.ERROR_LOAD, load_seq, "导入失败\n"+responseInfo.getDesc());
            }
        }catch(Exception e)
        {
            logger.info( e.getMessage(),e);
            try {
                dao.updateLoadDataLog(0+"", 0+"", dao.ERROR_LOAD, load_seq,"导入失败\n"+e.getMessage());
            } catch (Exception e1) {
                logger.info( e1.getMessage(),e1);
            }
        }
        finally {
            if(file!=null&&file.exists())
            {
                logger.info("delete:::::::"+file.getAbsolutePath());
                file.delete();
            }
            if (requestInfo.getSourceDataFileName() != null && requestInfo.getSourceDataFilePath() != null){
                File fileout = new File(requestInfo.getSourceDataFilePath() + requestInfo.getSourceDataFileName());
                if(fileout!=null&&fileout.exists())
                {
                    logger.info("delete:::::::"+fileout.getAbsolutePath());
                    fileout.delete();
                }
            }
            if(ftpClientUtil!=null){
                try {
                    ftpClientUtil.closeFTPConnect();
                }catch (Exception e){
                    logger.error(e.getMessage(),e);
                }
            }
        }
    }

    public DataAssetServiceResponseObject resolveCSVEntity(ImporterInfo importerInfo,String user_id)throws Exception{
        DataAssetServiceResponseObject responseObject=new DataAssetServiceResponseObject();
        dao.insertLoadDataLog(importerInfo, user_id);
        return responseObject;
    }

    public DataAssetServiceResponseObject resolveCSVEntity(DataAssetServiceRequestObject requestObject) throws Exception {
        DataAssetServiceReqData reqData=(DataAssetServiceReqData) MessageHandler.newInstance().getReqData(requestObject);
        ImporterInfo importerInfo=reqData.getImporterInfo();
        String user_id= MessageHandler.newInstance().getClientId(requestObject);
        return resolveCSVEntity(importerInfo,user_id);
    }

}