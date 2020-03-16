package com.newland.edc.cct.plsqltool.interactive.listener;

import com.newland.edc.cct.dgw.utils.cache.ignite.ignite.base.IgniteInit;
import com.newland.edc.cct.dgw.utils.cache.ignite.ignite.config.IgniteContent;
import com.newland.edc.cct.plsqltool.interactive.service.IPLSqlToolCloseConnService;
import com.newland.edc.cct.plsqltool.interactive.util.SpringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.Ignite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * Ignite启动
 */
@Component
@Order(1)//如果多个自定义ApplicationRunner，用来标明执行顺序
public class IgniteApplicationRunner implements ApplicationRunner {

    private static Logger log = LoggerFactory.getLogger(IgniteApplicationRunner.class);

    @Resource(name = "com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolCloseConnServiceImpl")
    public IPLSqlToolCloseConnService closeConnService;

    @Override
    public void run(ApplicationArguments applicationArguments) throws Exception {

        String path = SpringUtils.getProjectRootPath() + "/" + IgniteContent.IGNITE_CONFIG_FILE_PATH_NAME;
        String isRun = SpringUtils.getConfigByKey(IgniteContent.IGNITE_CONFIG_IS_RUN_IGNITE);

        log.info("is need start ignite : " + isRun);
        Ignite ignite = null;

        if (StringUtils.isNotEmpty(isRun) && "1".equals(isRun)) {
            ignite = IgniteInit.init(path);
//            IgniteInit.init(path);

            //启动接收线程
            closeConnService.receiveCloseMessageStart();
        }




//        ClusterNode cnode = ignite.cluster().node(UUID.fromString(IgniteContent.IGNITE_CURRENT_NODE_ID));
//        System.out.println("KKKKKKKKKKK"+cnode);
//
//        ClusterNode n = ignite.cluster().forRemotes().node(UUID.fromString(IgniteContent.IGNITE_CURRENT_NODE_ID));
//        System.out.println("KKKKKKKKKKK2"+n);
////
////        Collection<ClusterNode> nodes =  ignite.cluster().forRemotes().nodes();
////        for (ClusterNode node : nodes) {
////            System.out.println(node.id());
////        }
////
////        ClusterNode n = ignite.cluster().forRemotes().node(UUID.fromString("39da851c-fd04-4470-99ed-d24632453e25"));
//
//
////        IgniteCluster cluster = ignite.cluster();
////        cluster.forRemotes().nodes();
//
//        IgniteMessaging rmtMsg = ignite.message();
//       IgniteMessaging rmtMsg = ignite.message(ignite.cluster().forRemotes());
//        IgniteMessaging rmtMsg = ignite.message(ignite.cluster().forNodeId());

//        ClusterGroup clusterGroup = IgniteInit.ignite.cluster().forNodeId(UUID.fromString(09F3041D-ABB3-4709-8F8A-7E9ECC55B6AB));
//        IgniteMessaging rmtMsg = IgniteInit.ignite.message(clusterGroup);



//
//        // Add listener for unordered messages on all remote nodes.
//        rmtMsg.remoteListen("TOPIC_CLOSE_CONN", (nodeId, msg) -> {
//            System.out.println("Received unordered message [msg=" + msg + ", from=" + nodeId + ']');
//
//            return true; // Return true to continue listening.
//        });
//
        // Send unordered messages to remote nodes.



//        for (int i = 0; i < 10; i++) {
//            Thread.sleep(10000L);
//
//            TestSqlBean t = new TestSqlBean();
//            t.setConn_id("100");
//            System.out.println("发送"+1);
//            rmtMsg.send("TOPIC_CLOSE_CONN", t);
//
//
//            //rmtMsg.send("TOPIC_CLOSE_CONN", Integer.toString(i));
//            System.out.println("发送"+1);
//        }


    }


}