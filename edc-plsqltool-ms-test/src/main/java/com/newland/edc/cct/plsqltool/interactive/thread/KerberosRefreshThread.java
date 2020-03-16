package com.newland.edc.cct.plsqltool.interactive.thread;

import com.newland.edc.cct.plsqltool.interactive.content.PlsqltoolContent;
import com.newland.edc.cct.plsqltool.interactive.util.KerberosTool;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;

/**
 * 票据刷新线程
 */
@Component
public class KerberosRefreshThread {

    private static Logger logger = LoggerFactory.getLogger(KerberosRefreshThread.class);


    /**
     * 刷新
     */
    @Scheduled(cron = "${dgw.quartz.kbRefresh:0 0 */2 * * *}")
    public void kbRefresh() {
        logger.info("refresh kerberos start");
        KerberosTool.getUgiMap().clear();
        KerberosTool.getCheckMap().clear();
        logger.info("refresh kerberos end");
    }


}
