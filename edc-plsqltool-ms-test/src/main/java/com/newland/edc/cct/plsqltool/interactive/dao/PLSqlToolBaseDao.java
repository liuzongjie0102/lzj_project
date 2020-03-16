package com.newland.edc.cct.plsqltool.interactive.dao;

import com.newland.edc.cct.dataasset.entity.model.javabean.TabColBean;
import com.newland.edc.cct.dataasset.entity.model.javabean.TabInfoBean;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.*;

import java.util.List;

public interface PLSqlToolBaseDao {

    public void insertTemporaryInfo(TemporaryTabInfo temporaryTabInfo) throws Exception;

    public void deleteTemporaryTab(TemporaryTabInfo temporaryTabInfo) throws Exception;

    public void deleteTemporaryCol(String tab_id) throws Exception;

    public List<TemporaryTabInfo> selectTemporaryInfo(String tenant_id, String user_id);

    public List<TemporaryTabInfo> selectTemporaryInfo(TemporaryTabInfo temporaryTabInfo);

    public List<TemporaryTabInfo> selectTemporaryInfo(TemporaryReq temporaryReq);

    public void updateTemporaryTableName(String tenant_id, String user_id, String resource_id, String conn_id, String fromTable,
                    String toTable) throws Exception;

    public void insertTemporaryCol(TemporaryColInfo temporaryColInfo)throws Exception;

    public Integer getTemporaryCount(TemporaryReq temporaryReq);

    public List<PLSqlToolTable> getTabEntity(String tenant_id, String db_type, String resource_id) throws Exception;

    public List<ExecuteGroupLog> queryExecuteLogs(ExecuteLog executeLog) throws Exception;

    public List<ExecuteLog> selectExecuteLogs(ExecuteLog executeLog) throws Exception;

    public void insertExecuteLog(ExecuteLog executeLog) throws Exception;

    public void updateExecuteLog(ExecuteLog executeLog, String status) throws Exception;

    public void cleanExecuteLog(String time) throws Exception;

    public List<ExecuteErrorInfo> selectErrorInfos(ExecuteErrorInfo executeErrorInfo);

    public void insertErrorInfo(ExecuteErrorInfo executeErrorInfo) throws Exception;

    public List<ExecuteResult> selectExecuteResult(String task_id);

    public void insertExecuteResult(ExecuteResult executeResult) throws Exception;

    public void cleanExecuteResult(String time) throws Exception;

    public List<String> selectDataRange(String tenant_id);

    public List<TabColBean> selectTabEntityCols(String tab_id);

    public List<TableOpt> selectAuthority(String user_id);

    public void insertTabInfo(TabInfo tabInfo) throws Exception;

    public void updateTabInfo(TabInfo tabInfo) throws Exception;

    public TabInfo selectTabInfo(TabInfo tabInfo) throws Exception;

    public List<TabInfo> selectTabInfoListByPage(TabInfoReq tabInfoReq);

    public int getTabInfoCount(TabInfoReq tabInfoReq);

    public void deleteTabInfo(TabInfo tabInfo) throws Exception;

    public List<ExecutePoolHeartbeat> selectPoolHeartbeat(ExecutePoolHeartbeat executePoolHeartbeat);

    public void insertPoolHeartbeat(ExecutePoolHeartbeat executePoolHeartbeat) throws Exception;

    public int updatePoolHeartbeat(ExecutePoolHeartbeat executePoolHeartbeat, String heartbeat)throws Exception;

    public ExecutePoolHeartbeat queryExecutePool(String db_type);

    public TabInfoBean getTabEntityInfo(String tabId);

    public List<ConnectTnsConf> queryConnConf(
                    List<com.newland.edc.cct.dataasset.dispose.model.javabean.DataDispBean> dataDispBeans);

    public String queryExecuteIp(String group_id);

    public void insertExportLog(ExportLog exportLogBean) throws Exception;

    public List<ExportLog> queryExportLog(ExportLog exportLog);

    public ExportLogReq queryExportLog(ExportLogReq exportLogBean);

    public List<ExportLog> queryExportLogQuartz();

    public void updateExportLog(String export_id, String status, String export_result, String download_status) throws Exception;

    public void updateExportLog(ExportLog exportLog, String export_id) throws Exception;

    public void updateExportLogQuartz() throws Exception;

    public void deleteExportLog(String export_id) throws Exception;

    public List<ExportLog> queryTimeoutExportLog(int time);

    public void insertExportDownloadLog(ExportDownloadLog exportDownloadLog) throws Exception;

    public List<FtpConfig> queryFtpConfig(FtpConfig ftpConfig);

    public void insertFtpSynchronousLog(FtpSynchronousLog ftpSynchronousLog) throws Exception;

    public List<FtpSynchronousLog> queryFtpSynchronousLog(FtpSynchronousLog ftpSynchronousLog);

    public void updateFtpSynchronousLog(String log_id, String status, String error_info) throws Exception;

    public String queryConnIdByConnNameEn(String conn_name_en);
}
