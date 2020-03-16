package com.newland.edc.cct.plsqltool.interactive.service.impl;

import com.alibaba.fastjson.JSON;
import com.newland.edc.cct.dgw.utils.cache.ignite.ignite.base.IgniteInit;
import com.newland.edc.cct.dgw.utils.cache.ignite.ignite.config.IgniteContent;
import com.newland.edc.cct.plsqltool.interactive.content.PlsqltoolContent;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.TestSqlBean;
import com.newland.edc.cct.plsqltool.interactive.service.IPLSqlToolCloseConnService;
import com.newland.edc.cct.plsqltool.interactive.service.PLSqlToolExecuteService;
import com.newland.edc.cct.plsqltool.interactive.util.SpringUtils;
import com.newland.edc.cct.plsqltool.interactive.util.TopicContent;
import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Repository("com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolCloseConnServiceImpl")
public class PLSqlToolCloseConnServiceImpl implements IPLSqlToolCloseConnService {

    private static Logger log = LoggerFactory.getLogger(PLSqlToolCloseConnServiceImpl.class);

    @Resource(name = "com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolExecuteServiceImpl")
    public PLSqlToolExecuteService excuteService;


    /**
     * 发送关闭连接信息
     *
     * @param sqlBean 关闭的信息报文
     * @return
     * @throws Exception
     */
    public String sendCloseMessage(String nodeId, TestSqlBean sqlBean) throws Exception {

        String errMsg = "";

        if (StringUtils.isEmpty(nodeId) || null == sqlBean) {
            throw new Exception("参数不能为空");
        }

        if (IgniteContent.IGNITE_CURRENT_NODE_ID.equals(nodeId)) {
            /**
             * 如果请求的ID就为当前ID，说明就在当前服务器，直接调用方法进行停止
             */
            log.info("在本机进行关闭");


            try {
                excuteService.closeExecute(sqlBean);
            } catch (Exception e) {
                log.error("不影响结果的异常打印：", e);
            }

        } else {
            /**
             * 集群中的节点的处理
             */
            log.info("关闭远程节点连接");


//            ClusterGroup clusterGroup = null;
//            try {
//                clusterGroup = IgniteInit.ignite.cluster().forNodeId(UUID.fromString(nodeId));
//            } catch (Exception e) {
//                log.warn("没有找到对应的节点:"+nodeId,e);
//                clusterGroup = null;
//            }

            ClusterNode node = IgniteInit.ignite.cluster().forRemotes().node(UUID.fromString(nodeId));


            if (null == node) {
                errMsg = "节点" + nodeId + "已经被停止";

            } else {
                ClusterGroup clusterGroup = IgniteInit.ignite.cluster().forNode(node);

                IgniteMessaging rmtMsg = IgniteInit.ignite.message(clusterGroup);

                String sendMsg = JSON.toJSONString(sqlBean);
                log.info("发送的报文为：" + sendMsg);

                rmtMsg.send(TopicContent.TOPIC_CLOSE_CONN, sqlBean);
            }
        }
        log.info(errMsg);
        return errMsg;
    }

    /**
     * 获得当前节点ID
     *
     * @return 节点ID
     * @throws Exception
     */
    public String getCurrentNodeId() throws Exception {
        return IgniteContent.IGNITE_CURRENT_NODE_ID;
    }

    /**
     * 接收关闭连接信息的服务启动
     *
     * @throws Exception
     */
    public void receiveCloseMessageStart() throws Exception {
        log.info("开始启动关闭信息服务");
//        IgniteMessaging rmtMsg = IgniteInit.ignite.message(IgniteInit.ignite.cluster().forRemotes());
        IgniteMessaging rmtMsg = IgniteInit.ignite.message();
        log.info("完成启动关闭信息服务");


        rmtMsg.localListen(TopicContent.TOPIC_CLOSE_CONN, (nodeId, msg) -> {
            log.info("接收到来自节点=> " + nodeId + " <=的信息,当前的节点为:" + IgniteContent.IGNITE_CURRENT_NODE_ID);

            if (null != msg) {
                log.info("接收到的请求报文=> " + JSON.toJSONString(msg));

                try {
                    PLSqlToolExecuteService service = (PLSqlToolExecuteService) SpringUtils.getBean("com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolExecuteServiceImpl");
                    service.closeExecute((TestSqlBean) msg);

                } catch (Exception e) {
                    log.error("不影响结果的异常打印：", e);
                }


            }

            return true;
        });
    }

    /**
     * 空闲线程记录
     */
    private static Map<String, Integer> idleNumMap = new ConcurrentHashMap<>();

    /**
     * 设置sql执行的空闲任务数量
     *
     * @param dbType   数据库类型
     * @param workType 任务类型
     * @param idleNum  空闲数量
     * @throws Exception
     */
    @Override
    public void setSqlExecuteTaskNum(String dbType, String workType, int idleNum) throws Exception {
        String key = dbType + PlsqltoolContent.IDLE_THREAD_SPLIT_FLAG + workType;

        log.info("设置的key：" + key + ";设置的idleNum：" + idleNum);

        idleNumMap.put(key, idleNum);
    }

    /**
     * 获得SQL执行的空闲任务数据
     *
     * @param dbType   数据库类型
     * @param workType 任务类型
     * @return
     */
    @Override
    public int getSqlExecuteTaskNum(String dbType, String workType) throws Exception {

        String key = dbType + PlsqltoolContent.IDLE_THREAD_SPLIT_FLAG + workType;
        Integer idleNum = idleNumMap.get(key);
        idleNum = (null == idleNum) ? 0 : idleNum;

        log.info("设置的key：" + key + ";返回的idleNum：" + idleNum);

        return idleNum;
    }

    /**
     * 判断节点是否存活
     *
     * @param nodeId 节点ID
     * @return true:存活;false:消亡
     * @throws Exception
     */
    @Override
    public boolean isNodeAlive(String nodeId) throws Exception {

        boolean isAlive = true;

        //如果节点为当前节点，直接当做存活
        if (IgniteContent.IGNITE_CURRENT_NODE_ID.equals(nodeId)) {
            return isAlive;
        }

        log.info("关闭远程节点连接");
//        ClusterGroup clusterGroup = IgniteInit.ignite.cluster().forNodeId(UUID.fromString(nodeId));

//        ClusterGroup clusterGroup = null;
//        try {
//            clusterGroup = IgniteInit.ignite.cluster().forNodeId(UUID.fromString(nodeId));
//        } catch (Exception e) {
//            log.warn("没有找到对应的节点:"+nodeId,e);
//            clusterGroup = null;
//        }

        ClusterNode node = IgniteInit.ignite.cluster().forRemotes().node(UUID.fromString(nodeId));

        if (null == node) {
            isAlive = false;
        }

        return isAlive;
    }

}
