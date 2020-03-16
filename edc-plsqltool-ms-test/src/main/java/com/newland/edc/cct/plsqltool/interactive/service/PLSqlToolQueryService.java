package com.newland.edc.cct.plsqltool.interactive.service;

import com.newland.bd.ms.core.model.RespInfo;
import com.newland.edc.cct.dataasset.dispose.model.javabean.DataDispBean;
import com.newland.edc.cct.dataasset.entity.model.javabean.TabColBean;
import com.newland.edc.cct.dataasset.entity.model.javabean.TabInfoBean;
import com.newland.edc.cct.dataasset.entity.model.javabean.TabTenantBean;
import com.newland.edc.cct.plsqltool.interactive.common.ResAddress;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.*;

import java.util.List;
import java.util.Map;

public interface PLSqlToolQueryService {

    /**
     * 根据用户 租户查询实体
     * @param user_id
     * @param tenant_id
     * @return
     */
    public DataAssetEntity queryEntityData(String user_id, String tenant_id);

    /**
     * 查询临时表信息
     * @param tenant_id
     * @param user_id
     * @return
     */
    public List<TemporaryTabInfo> getTemporaryInfo(String tenant_id, String user_id);

    /**
     * 查询临时表信息
     * @param temporaryTabInfo
     * @return
     */
    public List<TemporaryTabInfo> getTemporaryInfo(TemporaryTabInfo temporaryTabInfo);

    /**
     * 查询临时表信息
     * @param temporaryReq
     * @return
     */
    public List<TemporaryTabInfo> getTemporaryInfo(TemporaryReq temporaryReq);

    /**
     * rename临时表
     * @param tenant_id
     * @param user_id
     * @param resource_id
     * @param conn_id
     * @param fromTable
     * @param toTable
     * @throws Exception
     */
    public void updateTemporaryTableName(String tenant_id, String user_id, String resource_id, String conn_id, String fromTable,
                    String toTable) throws Exception;

    /**
     * 查询资源
     * @param tenant_id
     * @param user_id
     * @return
     */
    public List<DataDispBean> queryTenantResouce(String tenant_id, String user_id);

    /**
     * 查询连接
     * @param tenant_id
     * @param resource_id
     * @return
     */
    public List<com.newland.edc.cct.dataasset.entity.model.javabean.DataDispBean> queryTenantConn(String tenant_id,
                    String resource_id);

    /**
     * 租户 资源 连接
     * @param user_id
     * @param tokencode
     * @return
     */
    public List<TabTenantBean> getTenantsByUserId(String user_id, String tokencode);

    /**
     * 租户 资源 连接
     * @return
     */
    public List<TabTenantBean> getTenants();

    /**
     * 获取租户列表
     * @return
     */
    public List<TabTenantBean> getTenanetList();

    /**
     * 提取查询缓存
     * @param tenant_id
     * @param resource_id
     * @param conn_id
     * @return
     */
    public DataAssetEntity getCatchData(String tenant_id, String resource_id, String conn_id);

    /**
     * 临时表bean转换
     * @param temporaryTabInfos
     * @return
     */
    public List<TabInfoBean> trans(List<TemporaryTabInfo> temporaryTabInfos);

    /**
     * 临时表bean转换
     * @param temporaryTabInfos
     * @return
     */
    public List<PLSqlToolTable> transPLSqlToolTable(List<TemporaryTabInfo> temporaryTabInfos);

    /**
     * 删除临时表
     * @param temporaryTabInfo
     * @throws Exception
     */
    public void deleteTemporaryTab(TemporaryTabInfo temporaryTabInfo) throws Exception;

    /**
     * 删除临时表字段
     * @param tab_id
     * @throws Exception
     */
    public void deleteTemporaryCol(String tab_id) throws Exception;

    /**
     * 插入临时表
     * @param temporaryTabInfo
     * @throws Exception
     */
    public void insertTemporaryInfo(TemporaryTabInfo temporaryTabInfo) throws Exception;

    /**
     * 插入临时表字段
     * @param temporaryColInfo
     * @throws Exception
     */
    public void insertTemporaryCol(TemporaryColInfo temporaryColInfo)throws Exception;

    /**
     * 计数
     * @param temporaryReq
     * @return
     */
    public Integer getTemporaryCount(TemporaryReq temporaryReq);

    /**
     * 获取租户 资源 连接
     * @param tenant_id
     * @param resource_id
     * @param conn_id
     * @return
     * @throws Exception
     */
    public PLSqlToolDataAssetEntity getTabEntity(String tenant_id, String resource_id, String conn_id)throws Exception;

    /**
     * 获取租户 资源 连接
     * @param testGrammarBean
     * @return
     * @throws Exception
     */
    public PLSqlToolDataAssetEntity getTabEntity(TestGrammarBean testGrammarBean)throws Exception;

    /**
     * 插入执行日志
     * @param executeLog
     * @throws Exception
     */
    public void insertExecuteLog(ExecuteLog executeLog) throws Exception;

    /**
     * 查询执行日志 分组
     * @param executeLog
     * @return
     * @throws Exception
     */
    public List<ExecuteGroupLog> queryExecuteLogs(ExecuteLog executeLog) throws Exception;

    /**
     * 查询执行日志 详情
     * @param executeLog
     * @return
     * @throws Exception
     */
    public List<ExecuteLog> selectExecuteLog(ExecuteLog executeLog) throws Exception;

    /**
     * 更新执行日志
     * @param executeLog
     * @param status
     * @throws Exception
     */
    public void updateExecuteLog(ExecuteLog executeLog, String status) throws Exception;

    /**
     * 清除执行日志
     * @param time
     * @throws Exception
     */
    public void cleanExecuteLog(String time) throws Exception;

    /**
     * 查询异常信息
     * @param executeErrorInfo
     * @return
     */
    public List<ExecuteErrorInfo> selectErrorInfo(ExecuteErrorInfo executeErrorInfo);

    /**
     * 插入异常信息
     * @param executeErrorInfo
     * @throws Exception
     */
    public void insertErrorInfo(ExecuteErrorInfo executeErrorInfo) throws Exception;

    /**
     * 查询执行结果
     * @param task_id
     * @return
     */
    public List<ExecuteResult> selectExecuteResult(String task_id);

    /**
     * 插入执行结果
     * @param executeResult
     * @throws Exception
     */
    public void insertExecuteResult(ExecuteResult executeResult) throws Exception;

    /**
     * 获取本机集群名
     * @return
     * @throws Exception
     */
    public String getIp() throws Exception;

    /**
     * 清除执行结果
     * @param time
     * @throws Exception
     */
    public void cleanExecuteResult(String time) throws Exception;

    /**
     * 查询租户地市权限
     * @param tenant_id
     * @return
     */
    public String selectDataRange(String tenant_id);

    /**
     * 查询实体字段
     * @param tab_id
     * @return
     */
    public List<TabColBean> selectTabEentityCols(String tab_id);

    /**
     * 变量翻译
     * @param var
     * @return
     */
    public String transVariableAll(String var);

    /**
     * 表执行权限
     * @param user_id
     * @return
     * @throws Exception
     */
    public TableOptAuthority selectAuthority(String user_id) throws Exception;

    /**
     * 查询标签卡片
     * @param tabInfo
     * @return
     * @throws Exception
     */
    public TabInfo queryTabInfo(TabInfo tabInfo) throws Exception;

    /**
     * 查询标签卡片
     * @param tabInfoReq
     * @return
     */
    public RespInfo queryTabInfoListByPage(TabInfoReq tabInfoReq);

    /**
     * 删除标签卡片
     * @param tabInfo
     * @throws Exception
     */
    public void deleteTabInfo(TabInfo tabInfo) throws Exception;

    /**
     * 保存标签卡片
     * @param tabInfo
     * @throws Exception
     */
    public void saveTabInfo(TabInfo tabInfo) throws Exception;

    /**
     * 查询线程心跳
     * @param executePoolHeartbeat
     * @return
     */
    public List<ExecutePoolHeartbeat> selectPoolHeartbeat(ExecutePoolHeartbeat executePoolHeartbeat);

    /**
     * 更新线程心跳
     * @param executePoolHeartbeat
     * @param heartbeat
     * @return
     * @throws Exception
     */
    public int updatePoolHeartbeat(ExecutePoolHeartbeat executePoolHeartbeat, String heartbeat)throws Exception;

    /**
     * 插入线程心跳
     * @param executePoolHeartbeat
     * @throws Exception
     */
    public void insertPoolHeartbeat(ExecutePoolHeartbeat executePoolHeartbeat) throws Exception;

    /**
     * 插入执行日志
     * @param testSqlBeans
     * @param executeIP
     * @param status
     * @return
     * @throws Exception
     */
    public List<ExecuteLog> insertExecuteLogs(List<TestSqlBean> testSqlBeans, String executeIP, String status) throws Exception;

    /**
     * 查询集群闲置线程池
     * @param db_type
     * @return
     */
    public ExecutePoolHeartbeat queryExecutePool(String db_type);

    /**
     * 查询实体
     * @param tabId
     * @return
     */
    public TabInfoBean getTabEntityInfo(String tabId);

    /**
     * 查询连接tns
     * @param dataDispBeans
     * @return
     */
    public Map<String, ConnectTnsConf> queryTnsConfigue(List<DataDispBean> dataDispBeans);

    /**
     * 查询执行集群
     * @param group_id
     * @return
     */
    public String queryExecuteIp(String group_id);

    /**
     * 插入导出记录
     * @param exportLogBean
     * @throws Exception
     */
    public void insertExportLog(ExportLog exportLogBean) throws Exception;

    /**
     * 查询导出记录
     * @param exportLog
     * @return
     */
    public List<ExportLog> queryExportLog(ExportLog exportLog);

    /**
     * 查询导出记录
     * @param exportLogBean
     * @return
     */
    public ExportLogReq queryExportLog(ExportLogReq exportLogBean);

    /**
     * 查询导出记录
     * @return
     */
    public List<ExportLog> queryExportLogQuartz();

    /**
     * 更新导出记录
     * @param export_id
     * @param status
     * @param export_result
     * @param download_status
     * @throws Exception
     */
    public void updateExportLog(String export_id, String status, String export_result, String download_status) throws Exception;

    /**
     * 更新导出记录
     * @param exportLog
     * @param export_id
     * @throws Exception
     */
    public void updateExportLog(ExportLog exportLog, String export_id) throws Exception;

    /**
     * 更新导出记录
     * @throws Exception
     */
    public void updateExportLogQuartz() throws Exception;

    /**
     * 删除导出记录
     * @param export_id
     * @throws Exception
     */
    public void deleteExportLog(String export_id) throws Exception;

    /**
     * 查询导出超时的导出记录
     * @return
     */
    public List<ExportLog> queryTimeoutExportLog(int time);

    /**
     * 插入下载日志
     * @param exportDownloadLog
     * @throws Exception
     */
    public void insertExportDownloadLog(ExportDownloadLog exportDownloadLog) throws Exception;

    /**
     * 查询临时表
     * @param temporaryTabInfo
     * @return
     */
    public RespInfo getTemporaryInfoByTrans(TemporaryTabInfo temporaryTabInfo);

    /**
     * 查询ftp配置
     * @param ftpConfig
     * @return
     */
    public List<FtpConfig> queryFtpConfig(FtpConfig ftpConfig);

    /**
     * 插入同步任务日志
     * @param ftpSynchronousLog
     * @throws Exception
     */
    public void insertFtpSynchronousLog(FtpSynchronousLog ftpSynchronousLog) throws Exception;

    /**
     * 查询同步任务日志
     * @param ftpSynchronousLog
     * @return
     */
    public List<FtpSynchronousLog> queryFtpSynchronousLog(FtpSynchronousLog ftpSynchronousLog);

    /**
     * 更新同步任务日志
     * @param log_id
     * @param status
     * @param error_info
     * @throws Exception
     */
    public void updateFtpSynchronousLog(String log_id, String status, String error_info) throws Exception;

    /**
     * 获取项目部署地址的配置信息
     * @throws Exception
     */
    public ResAddress getCurrentResAddressVersion() throws Exception;

    /**
     * 根据连接英文名获取连接id
     * @param conn_name_en
     * @return
     */
    public String queryConnIdByConnNameEn(String conn_name_en);

    /**
     * 获取所有租户 资源 连接
     * @param db_type
     * @return
     * @throws Exception
     */
    public List<DBTenantBean> getTenants(String db_type) throws Exception;

    /**
     * 根据用户id获取租户 资源 连接
     * @param user_id
     * @param tokencode
     * @param db_type
     * @return
     * @throws Exception
     */
    public List<DBTenantBean> getTenantsByUserId(String user_id, String tokencode, String db_type) throws Exception;

    /**
     * 根据数据库类型获取资源 连接
     * @param db_type
     * @return
     * @throws Exception
     */
    public Map<String,List<DBResurceBean>> queryResource(String db_type) throws Exception;

    /**
     * 根据用户id查询租户列表（公共服务版）
     * @param userId
     * @return
     * @throws Exception
     */
    public List<DBTenantBean> queryTenantPub(String userId) throws Exception;

    /**
     * 根据租户和数据库类型查询资源连接
     * @param tenantId
     * @param dbType
     * @return
     * @throws Exception
     */
    public List<DBResurceBean> queryResourceAndConn(String tenantId, String dbType, String resource_id) throws Exception;

}
