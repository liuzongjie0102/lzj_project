package com.newland.edc.cct.plsqltool.interactive.service;

import com.newland.edc.cct.plsqltool.interactive.model.javabean.*;

import java.util.List;

public interface PLSqlToolExecuteService {
    /**
     * 请求网关执行接口
     * @param bean
     * @return
     * @throws Exception
     */
    public DgwSqlToolResult executeSql(ExecuteRequestBean bean) throws Exception;

    /**
     * 查看执行计划接口
     * @param testSqlBeans
     * @param conn_id
     * @return
     * @throws Exception
     */
    public List<ExplainInfo> executeExplain(List<TestSqlBean> testSqlBeans, String conn_id) throws Exception;

    /**
     * 执行连接断开
     * @param testSqlBean
     * @throws Exception
     */
    public void closeExecute(TestSqlBean testSqlBean) throws Exception;

    /**
     * 删除临时表
     * @param temporaryReq
     * @throws Exception
     */
    public void dropExecute(TemporaryReq temporaryReq) throws Exception;

    /**
     * 计数
     * @param sqlString
     * @param conn_id
     * @param db_type
     * @return
     * @throws Exception
     */
    public long sqlCountTask(String sqlString, String conn_id, String db_type) throws Exception;
}
