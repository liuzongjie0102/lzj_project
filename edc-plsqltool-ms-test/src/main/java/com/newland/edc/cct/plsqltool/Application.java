/**
 * edc-cct-system-ms : Application.java
 *
 * Copyright (c) 2017 福建新大陆软件工程有限公司 版权所有
 * Newland Co. Ltd. All rights reserved.
 */

package com.newland.edc.cct.plsqltool;

import com.newland.bd.ms.core.utils.SpringContextUtils;
import com.newland.bi.util.logger.BaseLogger;
import com.newland.edc.cct.dgw.utils.rest.RestFulServiceUtils;
import com.newland.edc.cct.plsqltool.interactive.service.IPLSqlToolCloseConnService;
import com.newland.edc.cct.plsqltool.interactive.util.DataCatch;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

/**
 * @Description:统一系统管理，微服务入口
 * @author zjh 大数据事业部-研发1部
 * @version 1.0 -------------------------------------------
 * @History: 修订日期 修订人 版本 描述
 *
 */
@SpringBootApplication
//@ComponentScan(basePackages = { "com.newland.edc.cct.dgw" ,"com.newland.edc.cct.plsqltool" })
//@ComponentScan(basePackages = { "com.newland","org.springframework" })
public class Application {

    private static BaseLogger log = (BaseLogger) BaseLogger.getLogger(Application.class);

    /**
     * 统一系统管理，微服务入口
     *
     * @param args
     */
    public static void main(String[] args) throws Exception{
        ApplicationContext ac = SpringApplication.run(Application.class, args);
        SpringContextUtils.setApplicationContext(ac);

        IPLSqlToolCloseConnService iplSqlToolCloseConnService = (IPLSqlToolCloseConnService)SpringContextUtils.getBean("com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolCloseConnServiceImpl");

        int FTP_FILE_SYNCHRONOUS_POOL = Integer.parseInt(RestFulServiceUtils.getconfig("FTP_FILE_SYNCHRONOUS_POOL"));
        DataCatch.createFtpFileSynchronousPools(FTP_FILE_SYNCHRONOUS_POOL);

        int PLS_EXECUTE_HIVE_ASY_POOL = Integer.parseInt(RestFulServiceUtils.getconfig("PLS_EXECUTE_HIVE_ASY_POOL"));
        int PLS_EXECUTE_HIVE_SYN_POOL = Integer.parseInt(RestFulServiceUtils.getconfig("PLS_EXECUTE_HIVE_SYN_POOL"));

        DataCatch.createHiveAsyExecutePools(PLS_EXECUTE_HIVE_ASY_POOL);
        DataCatch.createHiveSynExecutePools(PLS_EXECUTE_HIVE_SYN_POOL);
        iplSqlToolCloseConnService.setSqlExecuteTaskNum("hive","asy",PLS_EXECUTE_HIVE_ASY_POOL);
        iplSqlToolCloseConnService.setSqlExecuteTaskNum("hive","syn",PLS_EXECUTE_HIVE_SYN_POOL);

        int PLS_EXECUTE_ORACLE_ASY_POOL = Integer.parseInt(RestFulServiceUtils.getconfig("PLS_EXECUTE_ORACLE_ASY_POOL"));
        int PLS_EXECUTE_ORACLE_SYN_POOL = Integer.parseInt(RestFulServiceUtils.getconfig("PLS_EXECUTE_ORACLE_SYN_POOL"));

        DataCatch.createOracleAsyExecutePools(PLS_EXECUTE_ORACLE_ASY_POOL);
        DataCatch.createOracleSynExecutePools(PLS_EXECUTE_ORACLE_SYN_POOL);
        iplSqlToolCloseConnService.setSqlExecuteTaskNum("oracle","asy",PLS_EXECUTE_ORACLE_ASY_POOL);
        iplSqlToolCloseConnService.setSqlExecuteTaskNum("oracle","syn",PLS_EXECUTE_ORACLE_SYN_POOL);

        int PLS_EXECUTE_DB2_ASY_POOL = Integer.parseInt(RestFulServiceUtils.getconfig("PLS_EXECUTE_DB2_ASY_POOL"));
        int PLS_EXECUTE_DB2_SYN_POOL = Integer.parseInt(RestFulServiceUtils.getconfig("PLS_EXECUTE_DB2_SYN_POOL"));

        DataCatch.createDB2AsyExecutePools(PLS_EXECUTE_DB2_ASY_POOL);
        DataCatch.createDB2SynExecutePools(PLS_EXECUTE_DB2_SYN_POOL);
        iplSqlToolCloseConnService.setSqlExecuteTaskNum("db2","asy",PLS_EXECUTE_DB2_ASY_POOL);
        iplSqlToolCloseConnService.setSqlExecuteTaskNum("db2","syn",PLS_EXECUTE_DB2_SYN_POOL);

        int PLS_EXECUTE_GREENPLUM_ASY_POOL = Integer.parseInt(RestFulServiceUtils.getconfig("PLS_EXECUTE_GREENPLUM_ASY_POOL"));
        int PLS_EXECUTE_GREENPLUM_SYN_POOL = Integer.parseInt(RestFulServiceUtils.getconfig("PLS_EXECUTE_GREENPLUM_SYN_POOL"));

        DataCatch.createGreenplumAsyExecutePools(PLS_EXECUTE_GREENPLUM_ASY_POOL);
        DataCatch.createGreenplumSynExecutePools(PLS_EXECUTE_GREENPLUM_SYN_POOL);
        iplSqlToolCloseConnService.setSqlExecuteTaskNum("greenplum","asy",PLS_EXECUTE_GREENPLUM_ASY_POOL);
        iplSqlToolCloseConnService.setSqlExecuteTaskNum("greenplum","syn",PLS_EXECUTE_GREENPLUM_SYN_POOL);

        int PLS_EXECUTE_MYSQL_ASY_POOL = Integer.parseInt(RestFulServiceUtils.getconfig("PLS_EXECUTE_MYSQL_ASY_POOL"));
        int PLS_EXECUTE_MYSQL_SYN_POOL = Integer.parseInt(RestFulServiceUtils.getconfig("PLS_EXECUTE_MYSQL_SYN_POOL"));

        DataCatch.createMysqlAsyExecutePools(PLS_EXECUTE_MYSQL_ASY_POOL);
        DataCatch.createMysqlSynExecutePools(PLS_EXECUTE_MYSQL_SYN_POOL);
        iplSqlToolCloseConnService.setSqlExecuteTaskNum("mysql","asy",PLS_EXECUTE_MYSQL_ASY_POOL);
        iplSqlToolCloseConnService.setSqlExecuteTaskNum("mysql","syn",PLS_EXECUTE_MYSQL_SYN_POOL);

        int PLS_EXECUTE_GBASE_ASY_POOL = Integer.parseInt(RestFulServiceUtils.getconfig("PLS_EXECUTE_GBASE_ASY_POOL"));
        int PLS_EXECUTE_GBASE_SYN_POOL = Integer.parseInt(RestFulServiceUtils.getconfig("PLS_EXECUTE_GBASE_SYN_POOL"));

        DataCatch.createGbaseAsyExecutePools(PLS_EXECUTE_GBASE_ASY_POOL);
        DataCatch.createGbaseSynExecutePools(PLS_EXECUTE_GBASE_SYN_POOL);
        iplSqlToolCloseConnService.setSqlExecuteTaskNum("gbase","asy",PLS_EXECUTE_GBASE_ASY_POOL);
        iplSqlToolCloseConnService.setSqlExecuteTaskNum("gbase","syn",PLS_EXECUTE_GBASE_SYN_POOL);

    }


}
