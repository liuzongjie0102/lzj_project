package com.newland.edc.cct.plsqltool.interactive.service;

import com.newland.edc.cct.plsqltool.interactive.model.javabean.TestSqlBean;

/**
 * 关闭连接接口
 */
public interface IPLSqlToolCloseConnService {

    /**
     * 发送关闭连接信息
     *
     * @param nodeId  节点ID
     * @param sqlBean 请求对象
     * @return
     * @throws Exception
     */
    public String sendCloseMessage(String nodeId, TestSqlBean sqlBean) throws Exception;

    /**
     * 获得当前节点ID
     *
     * @return
     * @throws Exception
     */
    public String getCurrentNodeId() throws Exception;

    /**
     * 接收关闭连接信息的服务启动
     *
     * @throws Exception
     */
    public void receiveCloseMessageStart() throws Exception;


    /**
     * 设置sql执行的空闲任务数量
     *
     * @param dbType   数据库类型
     * @param workType 任务类型
     * @param idleNum  空闲数量
     * @throws Exception
     */
    public void setSqlExecuteTaskNum(String dbType, String workType, int idleNum) throws Exception;


    /**
     * 获得SQL执行的空闲任务数据
     *
     * @param dbType   数据库类型
     * @param workType 任务类型
     * @return
     */
    public int getSqlExecuteTaskNum(String dbType, String workType) throws Exception;


    /**
     * 判断节点是否存活
     *
     * @param nodeId 节点ID
     * @return true:存活;false:消亡
     * @throws Exception
     */
    public boolean isNodeAlive(String nodeId) throws Exception;


}
