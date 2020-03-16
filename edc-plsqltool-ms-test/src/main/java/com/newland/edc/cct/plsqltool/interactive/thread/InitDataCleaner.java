package com.newland.edc.cct.plsqltool.interactive.thread;

import com.newland.bi.util.logger.BaseLogger;
import com.newland.edc.cct.dgw.utils.rest.RestFulServiceUtils;
import com.newland.edc.cct.plsqltool.interactive.service.PLSqlToolQueryService;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

@Component // 此注解必加
public class InitDataCleaner {
    private static BaseLogger log = (BaseLogger) BaseLogger.getLogger(InitDataCleaner.class);

    @Resource(name ="com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolQueryServiceImpl")
    private PLSqlToolQueryService plSqlToolQueryService;

//    @Scheduled(cron = "${DataAsset.DataCleaner.Task}")
    public void start() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        //每天0点启动定时清理过期数据
        //1.清理执行结果
        try {
            Calendar clean_result =Calendar.getInstance();
            clean_result.setTime(new Date());
            String PLS_EXECUTE_RESULT = RestFulServiceUtils.getconfig("PLS_EXECUTE_RESULT");
            if(PLS_EXECUTE_RESULT!=null&&!PLS_EXECUTE_RESULT.equals("")){
                clean_result.add(Calendar.DATE,-Integer.parseInt(PLS_EXECUTE_RESULT));
            }else{
                clean_result.add(Calendar.DATE,-5);
            }
            plSqlToolQueryService.cleanExecuteResult(sdf.format(clean_result.getTime()));
        }catch (Exception e1){
            log.error(e1.getMessage(),e1);
        }
        //2.清理执行记录
        try {
            Calendar clean_log =Calendar.getInstance();
            clean_log.setTime(new Date());
            String PLS_EXECUTE_LOG = RestFulServiceUtils.getconfig("PLS_EXECUTE_LOG");
            if(PLS_EXECUTE_LOG!=null&&!PLS_EXECUTE_LOG.equals("")){
                clean_log.add(Calendar.DATE,-Integer.parseInt(PLS_EXECUTE_LOG));
            }else{
                clean_log.add(Calendar.DATE,-10);
            }
            plSqlToolQueryService.cleanExecuteLog(sdf.format(clean_log.getTime()));
        }catch (Exception e2){
            log.error(e2.getMessage(),e2);
        }
    }

}
