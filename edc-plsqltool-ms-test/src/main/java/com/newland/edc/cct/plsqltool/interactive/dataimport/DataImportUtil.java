package com.newland.edc.cct.plsqltool.interactive.dataimport;

import com.newland.bd.model.cfg.HadoopCfgBean;
import com.newland.bd.model.var.ConnInfo;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.ImportRequestInfo;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.ImportResponseInfo;
import com.newland.edc.cct.plsqltool.interactive.util.DataSourceAccess;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataImportUtil {
    private static final Logger      logger = LoggerFactory.getLogger(DataImportUtil.class);
    private static       IDataImport dataImport;

    public DataImportUtil() {
    }

    private static void initImportParam(ImportRequestInfo requestInfo) throws Exception {
        logger.info("传入参数={}", requestInfo);
        if (requestInfo != null && !StringUtils.isEmpty(requestInfo.getDbConnId()) && !StringUtils.isEmpty(requestInfo.getEnvService())) {
            String connId = requestInfo.getDbConnId();
            logger.info("数据库连接id为{}", connId);
            ConnInfo connInfo = null;

            try {
                connInfo = DataSourceAccess.getConnInfoByConnId(connId);
            } catch (Exception var8) {
                throw new Exception("根据connId获取数据库配置信息失败！connId=" + connId);
            }

            HadoopCfgBean hadoopCfgBean = null;
            try {
                hadoopCfgBean = DataSourceAccess.getHadoopCfgByConnId(connInfo, connId);
                System.out.println("hadoopcfg = " + hadoopCfgBean);
                logger.info("hadoopcfg = " + hadoopCfgBean);
            } catch (Exception var7) {
                throw new Exception("根据connId获取hadoop配置信息失败！connId=" + connId);
            }
            dataImport = new DataImportToHive(requestInfo.getSourceDataFilePath(), requestInfo.getSourceDataFileName(),
                            requestInfo.getOutputMode(), requestInfo.getTargetTable(), requestInfo.getPartitionInfo(),
                            hadoopCfgBean, connId, requestInfo.getTmpHdfsDir());
        } else {
            throw new Exception("传入参数存在空值，请检查参数！requestInfo=" + requestInfo);
        }
    }

    public static ImportResponseInfo execute(ImportRequestInfo requestInfo) {
        ImportResponseInfo responseInfo = new ImportResponseInfo();

        try {
            initImportParam(requestInfo);
            responseInfo = dataImport.importDataToDb();
        } catch (Exception var3) {
            logger.error(var3.toString(), var3);
            responseInfo.setResultCode(0);
            responseInfo.setDesc(var3.getMessage());
        }

        return responseInfo;
    }
}