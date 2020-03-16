package com.newland.edc.cct.plsqltool.interactive.thread;

import com.newland.bi.util.logger.BaseLogger;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.ExecutePoolHeartbeat;
import com.newland.edc.cct.plsqltool.interactive.service.PLSqlToolQueryService;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Component // 此注解必加
public class InitDataExecutePoolHeartbeat {
    private static BaseLogger log = (BaseLogger) BaseLogger.getLogger(InitDataExecutePoolHeartbeat.class);

    @Resource(name ="com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolQueryServiceImpl")
    private PLSqlToolQueryService plSqlToolQueryService;

    ExecutePoolHeartbeat executePoolHeartbeat1 = new ExecutePoolHeartbeat();
    ExecutePoolHeartbeat executePoolHeartbeat2 = new ExecutePoolHeartbeat();
    ExecutePoolHeartbeat executePoolHeartbeat3 = new ExecutePoolHeartbeat();
    /**
     * 用于记录本台服务任务执行情况
     */
//    @Scheduled(cron = "${DataAsset.ExecutePoolHeartbeat.Task:0 0 0 * * ?}")
    public void start() throws Exception{
//        String heart_switch = RestFulServiceUtils.getconfig("PLS_EXECUTEPOOL_HEARTBEAT_SWITCH");
//        if(heart_switch.equals("1")){
//            String edc_plsqltool_ip = RestFulServiceUtils.getconfig("EDC_PLSQLTOOL_IP");
//            if(edc_plsqltool_ip!=null&&!edc_plsqltool_ip.equals("")){
//                //hive异步心跳
//                executePoolHeartbeat1.setExecute_id(edc_plsqltool_ip);
//                executePoolHeartbeat1.setDb_type("hive");
//                int aliveTask_1 = DataCatch.getHiveAsyExecutePools().getAliveTask();
//                int allPools_1 = DataCatch.getHiveAsyExecutePools().getTaskPool();
//                if(this.plSqlToolQueryService.updatePoolHeartbeat(executePoolHeartbeat1,allPools_1-aliveTask_1+"")<=0){
//                    executePoolHeartbeat1.setP_working(allPools_1-aliveTask_1+"");
//                    this.plSqlToolQueryService.insertPoolHeartbeat(executePoolHeartbeat1);
//                }
//                //oracle异步心跳
//                executePoolHeartbeat2.setExecute_id(edc_plsqltool_ip);
//                executePoolHeartbeat2.setDb_type("oracle");
//                int aliveTask_2 = DataCatch.getOracleAsyExecutePools().getAliveTask();
//                int allPools_2 = DataCatch.getOracleAsyExecutePools().getTaskPool();
//                if(this.plSqlToolQueryService.updatePoolHeartbeat(executePoolHeartbeat2,allPools_2-aliveTask_2+"")<=0){
//                    executePoolHeartbeat2.setP_working(allPools_2-aliveTask_2+"");
//                    this.plSqlToolQueryService.insertPoolHeartbeat(executePoolHeartbeat2);
//                }
//                //db2异步心跳
//                executePoolHeartbeat3.setExecute_id(edc_plsqltool_ip);
//                executePoolHeartbeat3.setDb_type("db2");
//                int aliveTask_3 = DataCatch.getDb2AsyExecutePools().getAliveTask();
//                int allPools_3 = DataCatch.getDb2AsyExecutePools().getTaskPool();
//                if(this.plSqlToolQueryService.updatePoolHeartbeat(executePoolHeartbeat3,allPools_3-aliveTask_3+"")<=0){
//                    executePoolHeartbeat3.setP_working(allPools_3-aliveTask_3+"");
//                    this.plSqlToolQueryService.insertPoolHeartbeat(executePoolHeartbeat3);
//                }
//            }
//        }
    }

}
