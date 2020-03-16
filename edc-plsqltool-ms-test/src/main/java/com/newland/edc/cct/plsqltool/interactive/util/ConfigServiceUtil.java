package com.newland.edc.cct.plsqltool.interactive.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.newland.bd.model.cfg.GeneralDBBean;
import com.newland.bd.model.cfg.HadoopCfgBean;
import com.newland.bd.model.var.ConnInfo;
import com.newland.bd.model.var.VarInfo;
import com.newland.bd.ms.core.model.RespInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

public class ConfigServiceUtil {
    private static final Logger logger          = LoggerFactory.getLogger(ConfigServiceUtil.class);
    private static final String CONF_PATH       = "conf_path";
    private static final String AUTHENTICATION  = "authentication";
    private static final String KB_PRINCIPAL    = "kb_principal";
    private static final String KB_CONF         = "kb_conf";
    private static final String KB_KEYTAB       = "kb_keytab";
    private static final String JAAS_CONF       = "jaas_conf";
    private static final String BDOC_ACCESS_ID  = "bdoc_access_id";
    private static final String BDOC_ACCESS_KEY = "bdoc_access_key";
    private static final String QUEUENAME       = "queuename";
    private static final String FS_DEFAULTFS    = "fs_defaultFS";
    private static final String KEYTAB          = "keytab";
    private static final String KERBEROS        = "kerberos";
    private static final String PRINCIPAL       = "principal";

    public ConfigServiceUtil() {
    }

    public static Configuration getConfiguration(HadoopCfgBean hdfs, String connId) throws Exception {
        Configuration conf = new Configuration();
        if (hdfs.getCore_site_cfg() != null && !"".endsWith(hdfs.getCore_site_cfg())) {
            conf.addResource(new Path(hdfs.getCore_site_cfg()));
            logger.info("加载集群core-site配置");
        }

        if (hdfs.getHdfs_site_cfg() != null && !"".endsWith(hdfs.getHdfs_site_cfg())) {
            conf.addResource(new Path(hdfs.getHdfs_site_cfg()));
            logger.info("加载集群hdfs-site配置");
        }

        if (hdfs.getMr_site_cfg() != null && !"".endsWith(hdfs.getMr_site_cfg())) {
            conf.addResource(new Path(hdfs.getMr_site_cfg()));
            logger.info("加载集群mr-site配置");
        }

        if (hdfs.getYarn_site_cfg() != null && !"".endsWith(hdfs.getYarn_site_cfg())) {
            conf.addResource(new Path(hdfs.getYarn_site_cfg()));
            logger.info("加载集群yarn-site配置");
        }

//        String run_queue;
//        String job_priority;
//        if (hdfs.getKeytab() != null && hdfs.getKerberos() != null) {
//            run_queue = "xyup_T@HADOOP.COM";
//            if (hdfs.getPrincipal() != null && !"".endsWith(hdfs.getPrincipal())) {
//                run_queue = hdfs.getPrincipal();
//            }
//
//            if ("kerberos".equalsIgnoreCase(conf.get("hadoop.security.authentication"))) {
//                conf.set("principal", run_queue);
//                conf.set("keytab", hdfs.getKeytab());
//                job_priority = hdfs.getKerberos();
//                System.setProperty("java.security.krb5.conf", job_priority);
//                UserGroupInformation.setConfiguration(conf);
//
//                try {
//                    UserGroupInformation.loginUserFromKeytab(conf.get("principal"), conf.get("keytab"));
//                } catch (IOException var9) {
//                    logger.error(var9.getMessage(), var9);
//                }
//            }
//        }

        if (hdfs.getBdoc_access_id() != null && !"".equals(hdfs.getBdoc_access_id().trim())) {
            conf.set("hadoop.security.bdoc.access.id", hdfs.getBdoc_access_id());
            logger.info("配置的BDOC ID = " + hdfs.getBdoc_access_id());
        }

        if (hdfs.getBdoc_access_key() != null && !"".equals(hdfs.getBdoc_access_key().trim())) {
            conf.set("hadoop.security.bdoc.access.key", hdfs.getBdoc_access_key());
            logger.info("配置的BDOC KEY = " + hdfs.getBdoc_access_key());
        }

        String run_queue = hdfs.getRun_queue();
        String job_priority = hdfs.getJob_priority();
        long map_mamory = 0L;
        long reduce_mamory = 0L;
        if (run_queue != null && !"".endsWith(run_queue.trim())) {
            conf.set("mapreduce.job.queuename", run_queue);
        }

        if (job_priority != null && !"".endsWith(job_priority.trim())) {
            conf.set("mapreduce.job.priority", job_priority);
        }

        if (hdfs.getMap_mamory_str() != null && !"".endsWith(hdfs.getMap_mamory_str().trim())) {
            map_mamory = Long.parseLong(hdfs.getMap_mamory_str());
        }

        if (hdfs.getReduce_mamory_str() != null && !"".endsWith(hdfs.getReduce_mamory_str().trim())) {
            reduce_mamory = Long.parseLong(hdfs.getReduce_mamory_str());
        }

        Long redopts;
        if (map_mamory > 0L) {
            conf.set("mapreduce.map.memory.mb", String.valueOf(map_mamory));
            redopts = map_mamory * 8L / 10L;
            if (conf.get("mapreduce.map.java.opts") != null) {
                conf.set("mapreduce.map.java.opts", conf.get("mapreduce.map.java.opts") + " -Xmx" + redopts + "m");
            } else {
                conf.set("mapreduce.map.java.opts", "-Xmx" + redopts + "m");
            }
        }

        if (reduce_mamory > 0L) {
            conf.set("mapreduce.reduce.memory.mb", String.valueOf(reduce_mamory));
            redopts = reduce_mamory * 8L / 10L;
            if (conf.get("mapreduce. reduce.java.opts") != null) {
                conf.set("mapreduce. reduce.java.opts", conf.get("mapreduce. reduce.java.opts") + " -Xmx" + redopts + "m");
            } else {
                conf.set("mapreduce. reduce.java.opts", "-Xmx" + redopts + "m");
            }
        }

        logger.info("运行资源配置：queue=" + conf.get("mapred.job.queue.name") + "   #priority=" + conf.get("mapreduce.job.priority") + "   #map memory=" + conf.get("mapreduce.map.memory.mb") + "   #map opt=" + conf.get("mapreduce.map.java.opts") + "   #reduce mamory=" + conf.get("mapreduce.reduce.memory.mb") + "   #reduce opt=" + conf.get("mapreduce. reduce.java.opts"));
        logger.info("默认文件系统名fs.defaultFS: " + hdfs.getFs_defaultFS());
        if (hdfs.getFs_defaultFS() != null && !"".equals(hdfs.getFs_defaultFS().trim())) {
            conf.set("fs.defaultFS", hdfs.getFs_defaultFS());
        }
        logger.info("文件系统名fs.defaultFS: " + conf.get("fs.defaultFS"));
        KerberosTool.settingKerberosForHDFS(connId,hdfs.getKeytab(), hdfs.getKerberos(), hdfs.getPrincipal(), conf);

        return conf;
    }

    public static ConnInfo getConnById(String envServiceUrl, String connId) throws Exception {
        logger.info("环境变量微服务地址={}", envServiceUrl);
        String url = envServiceUrl + "conn/group/id?connId=" + connId;
        System.out.println("url=" + url);
        logger.info("根据connId获取数据库连接信息，url={}", url);
        ConnInfo connInfo = null;
        HttpClient httpClient = new DefaultHttpClient();
        HttpGet request = new HttpGet(url);
        HttpResponse response = httpClient.execute(request);
        int statusCode = response.getStatusLine().getStatusCode();
        System.out.println("statusCode=" + statusCode);
        if (statusCode != 200) {
            throw new RuntimeException("根据connId获取连接信息，微服务调用失败。connId = " + connId + "     客户端连接异常，状态码：" + statusCode);
        } else {
            String json = EntityUtils.toString(response.getEntity());
            System.out.println("json=" + json);
            RespInfo respInfo = (RespInfo) JSONObject.parseObject(json, RespInfo.class);
            logger.info("getConnInfoByConnId   respInfo:" + respInfo);
            String code = respInfo.getRespErrorCode();
            if (code == null || "null".equals(code)) {
                String data = respInfo.getRespData().toString();
                connInfo = (ConnInfo)JSONObject.parseObject(data, ConnInfo.class);
            }

            return connInfo;
        }
    }

    public static GeneralDBBean getDBCfgByConnInfo(ConnInfo connInfo) throws Exception {
        logger.info("连接信息：" + JSON.toJSONString(connInfo));
        GeneralDBBean dbCfg = new GeneralDBBean();

        try {
            String user_name = "";
            String password = "";
            String url = "";
            String driverClass = "";
            String dbType = "";
            String keytab = "";
            String kerberos = "";
            String principal = "";
            String jaas = "";
            List<VarInfo> connVars = connInfo.getConnVar();
            List<VarInfo> resourceVars = connInfo.getResourceVar();
            Iterator var13;
            VarInfo varInfo;
            if (connVars != null) {
                var13 = connVars.iterator();

                while(var13.hasNext()) {
                    varInfo = (VarInfo)var13.next();
                    if ("user_name".equalsIgnoreCase(varInfo.getVarName())) {
                        user_name = varInfo.getVarValue();
                        dbCfg.setUsername(user_name);
                    } else if ("password".equalsIgnoreCase(varInfo.getVarName())) {
                        password = varInfo.getVarValue();
                        dbCfg.setPassword(password);
                    } else if ("tns".equalsIgnoreCase(varInfo.getVarName())) {
                        url = varInfo.getVarValue();
                        dbCfg.setUrl(url);
                    } else if ("kb_keytab".equalsIgnoreCase(varInfo.getVarName())) {
                        keytab = varInfo.getVarValue();
                        dbCfg.setKeytab(keytab);
                    } else if ("kb_conf".equalsIgnoreCase(varInfo.getVarName())) {
                        kerberos = varInfo.getVarValue();
                        dbCfg.setKerberos(kerberos);
                    } else if ("kb_principal".equalsIgnoreCase(varInfo.getVarName())) {
                        principal = varInfo.getVarValue();
                        dbCfg.setPrincipal(principal);
                    } else if ("jaas_conf".equalsIgnoreCase(varInfo.getVarName())) {
                        jaas = varInfo.getVarValue();
                        dbCfg.setJaas(jaas);
                    }
                }
            }

            if (resourceVars != null) {
                var13 = resourceVars.iterator();

                while(var13.hasNext()) {
                    varInfo = (VarInfo)var13.next();
                    if ("tns".equalsIgnoreCase(varInfo.getVarName()) && StringUtils.isEmpty(url)) {
                        url = varInfo.getVarValue();
                    }
                }
            }

            if (StringUtils.isNotEmpty(url)) {
                if (url.indexOf("oracle") > 0) {
                    dbType = "oracle";
                    driverClass = "oracle.jdbc.OracleDriver";
                } else if (url.indexOf("mysql") > 0) {
                    dbType = "mysql";
                    driverClass = "com.mysql.jdbc.Driver";
                } else if (url.indexOf("db2") > 0) {
                    dbType = "db2";
                    driverClass = "com.ibm.db2.jcc.DB2Driver";
                } else if (url.indexOf("gbase") > 0) {
                    dbType = "gbase";
                    driverClass = "com.gbase.jdbc.Driver";
                } else if (url.indexOf("hive") > 0) {
                    dbType = "hive";
                    driverClass = "org.apache.hive.jdbc.HiveDriver";
                } else if (url.indexOf("impala") > 0) {
                    dbType = "impala";
                    driverClass = "com.cloudera.impala.jdbc41.Driver";
                } else if (url.indexOf("timesten") > 0) {
                    dbType = "timesten";
                    driverClass = "com.timesten.jdbc.TimesTenDriver";
                } else {
                    if (url.indexOf("greenplum") <= 0) {
                        throw new Exception("无法获取数据库类型及驱动程序。");
                    }

                    dbType = "greenplum";
                    driverClass = "com.pivotal.jdbc.GreenplumDriver";
                }

                dbCfg.setUrl(url);
                dbCfg.setType(dbType);
                dbCfg.setDriver(driverClass);
            }

            return dbCfg;
        } catch (Exception var15) {
            logger.error(var15.toString());
            throw new Exception("从cct获取连接信息失败:" + var15.getMessage());
        }
    }

    public static HadoopCfgBean getHadoopCfgByConnId(ConnInfo connInfo, String connId) throws Exception {
        HadoopCfgBean hdfsCfg = new HadoopCfgBean();

        try {
            List<VarInfo> varInfos = connInfo.getConnVar();
            List<VarInfo> resourceInfos = connInfo.getResourceVar();
            varInfos.addAll(resourceInfos);
            hdfsCfg.setHadoop_name(connId);
            Iterator var5 = varInfos.iterator();

            while(true) {
                while(var5.hasNext()) {
                    VarInfo varInfo = (VarInfo)var5.next();
                    String varName = varInfo.getVarName();
                    if ("conf_path".equals(varName)) {
                        String confs = varInfo.getVarValue();
                        String[] conflist = confs.split(",");
                        String[] var10 = conflist;
                        int var11 = conflist.length;

                        for(int var12 = 0; var12 < var11; ++var12) {
                            String conf = var10[var12];
                            if (conf.indexOf("core-site.xml") >= 0) {
                                hdfsCfg.setCore_site_cfg(conf);
                            } else if (conf.indexOf("hdfs-site.xml") >= 0) {
                                hdfsCfg.setHdfs_site_cfg(conf);
                            } else if (conf.indexOf("mapred-site.xml") >= 0) {
                                hdfsCfg.setMr_site_cfg(conf);
                            } else if (conf.indexOf("yarn-site.xml") >= 0) {
                                hdfsCfg.setYarn_site_cfg(conf);
                            } else if (conf.indexOf("hbase-site.xml") >= 0) {
                                hdfsCfg.setHbase_site_cfg(conf);
                            }
                        }
                    } else if (varName.equals("kb_conf")) {
                        hdfsCfg.setKerberos_cfg(varInfo.getVarValue());
                        hdfsCfg.setKerberos(varInfo.getVarValue());
                    } else if (varName.equals("kb_keytab")) {
                        hdfsCfg.setKeytab_cfg(varInfo.getVarValue());
                        hdfsCfg.setKeytab(varInfo.getVarValue());
                    } else if (varName.equals("kb_principal")) {
                        hdfsCfg.setPrincipal(varInfo.getVarValue());
                    } else if (varName.equals("bdoc_access_id")) {
                        hdfsCfg.setBdoc_access_id(varInfo.getVarValue());
                        logger.info("bdoc_access_id===========:" + varInfo.getVarValue());
                    } else if (varName.equals("bdoc_access_key")) {
                        hdfsCfg.setBdoc_access_key(varInfo.getVarValue());
                        logger.info("bdoc_access_key===========::" + varInfo.getVarValue());
                    } else if (varName.equals("queuename")) {
                        hdfsCfg.setRun_queue(varInfo.getVarValue());
                    } else if (varName.equals("fs_defaultFS")) {
                        hdfsCfg.setFs_defaultFS(varInfo.getVarValue());
                    } else {
                        logger.warn("配置参数:" + connId + "的二级参数<" + varName + ">无法识别.");
                    }
                }

                return hdfsCfg;
            }
        } catch (Exception var14) {
            logger.error(var14.getMessage(), var14);
            throw new Exception(connId + "获取hadoop配置错误，" + var14.getMessage());
        }
    }
}
