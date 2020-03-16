package com.newland.edc.cct.plsqltool.interactive.thread;

import com.newland.edc.cct.dataasset.entity.model.javabean.ImporterInfo;
import com.newland.edc.cct.plsqltool.interactive.dao.ImportEntityDao;
import com.newland.edc.cct.plsqltool.interactive.service.ImportEntityBiz;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@EnableAsync
@Component
public class ImporterEntityThread {
    private static Logger logger = LoggerFactory.getLogger(ImporterEntityThread.class);

    @Resource(name = "com.newland.edc.cct.plsqltool.interactive.dao.impl.ImportEntityDaoImpl")
    ImportEntityDao                                                                                                dao;
    @Resource(name = "com.newland.edc.cct.plsqltool.interactive.service.impl.ImportEntityBizImpl") ImportEntityBiz biz;

    @Value("${Load.Thread.MaxNum:3}")
    private int max_load_thread_num;

    @Value("${ImporterEntityThread:false}")
    private boolean quartzSwitch;

    @Async
//    @Scheduled(cron = "${Importer.Task.Scheduled:* * * * * ?}")
    public void executeTask() {
        try {
            if(quartzSwitch){
                logger.info("交互式查询-导入线程 开始 ");
                int load_thread_num=dao.getUnFinishLoadLogNum();
                if(load_thread_num>=max_load_thread_num)
                {
                    logger.info("交互式查询-导入 正在执行的导入线程超出最大数量"+max_load_thread_num+"跑数停止");
                    return;
                }
                ImporterInfo importerInfo=dao.getUnLoadLog();
                biz.dealCSVEntityByLoadNew(importerInfo);
                logger.info("交互式查询-导入线程 结束 ");
            }
        } catch (Exception ex) {
            logger.info("交互式查询-导入线程 异常", ex);
        }
    }
}