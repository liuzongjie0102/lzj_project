package com.newland.edc.cct.plsqltool.interactive.util;

import com.alibaba.fastjson.JSON;
import com.newland.bd.workflow.sql.bean.consanguinity.TableBean;
import com.newland.bi.util.logger.BaseLogger;
import com.newland.edc.cct.dgw.common.DgwConstant;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import sun.security.krb5.Config;
import sun.security.krb5.KrbException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 票据工具类
 */
public class KerberosTool {

    private static BaseLogger logger = (BaseLogger) BaseLogger.getLogger(KerberosTool.class);

    //校验MAP
    private static Map<String, String> checkMap = new ConcurrentHashMap<>();
    //UGI集合
    private static Map<String, UserGroupInformation> ugiMap = new ConcurrentHashMap<>();


    public static synchronized void settingKerberosForHive(String connId, String keytab, String kerberos, String principal, String jaas) throws Exception {
        settingKerberos(connId, DgwConstant.DB_TYPE_HIVE, keytab, kerberos, principal, null, jaas);
    }


    /**
     * 设置UGI信息
     *
     * @param connId
     * @param keytab
     * @param kerberos
     * @param principal
     * @throws IOException
     */
    public static synchronized void settingKerberosForHbase(String connId, String keytab, String kerberos, String principal, Configuration conf) throws Exception {
        settingKerberos(connId, DgwConstant.DB_TYPE_HBASE, keytab, kerberos, principal, conf, null);
    }

    /**
     * 设置UGI信息
     *
     * @param connId
     * @param keytab
     * @param kerberos
     * @param principal
     * @throws IOException
     */
    public static synchronized void settingKerberosForHDFS(String connId, String keytab, String kerberos, String principal, Configuration conf) throws Exception {
        settingKerberos(connId, DgwConstant.DB_TYPE_HDFS, keytab, kerberos, principal, conf, null);

    }


    /**
     * 设置UGI信息
     *
     * @param connId
     * @param keytab
     * @param kerberos
     * @param principal
     * @throws IOException
     */
    public static synchronized void settingKerberos(String connId, String dbType, String keytab, String kerberos, String principal, Configuration conf, String jaas) throws Exception {

        if (StringUtils.isEmpty(connId)) {
            logger.error("没有设置连接ID");
            return;
        }

        if(StringUtils.isEmpty(dbType)){
            logger.error("没有设置数据库类型");
            return;
        }

        // 连接+数据库类型 key值
        String connidAndTypeKey = connId + "-" + dbType;

        if (StringUtils.isNotEmpty(keytab) && StringUtils.isNotEmpty(kerberos) && StringUtils.isNotEmpty(principal)) {

            boolean isHaveKB = true;
            //校验是否有票据
            if (!checkMap.containsKey(connidAndTypeKey)) {
                //如果没有对应的key,则算没有票据
                isHaveKB = false;
            } else {
                //如果有票据的情况，检查分区信息是否有改变
                String checkVal = checkMap.get(connidAndTypeKey);
                if (!(principal + "-" + keytab).equals(checkVal)) {
                    //如果值发生变化,则没有票据，需要重新刷
                    isHaveKB = false;
                }
            }

            //没有票据的情况下的处理
            if (false == isHaveKB) {
                logger.info("没有票据的情况，进入处理");

                String PRINCIPAL = "username.client.kerberos.principal";
                String KEYTAB = "username.client.keytab.file";

                if (null == conf) {
                    logger.info("传入的配置文件为null");
                    conf = new Configuration();
                }

                conf.set("hadoop.security.authentication", "kerberos");
                conf.set("hadoop.security.authorization", "true");
                conf.set(PRINCIPAL, principal);
                conf.set(KEYTAB, keytab);

                System.setProperty("java.security.krb5.conf", kerberos);
                Config.refresh();
                if(StringUtils.isNotEmpty(jaas)){
                    System.setProperty("java.security.auth.login.config", jaas);
                }
//                System.setProperty("sun.security.krb5.debug", "true");

                logger.info(" UserGroupInformation.isSecurityEnabled() =>" + UserGroupInformation.isSecurityEnabled());
                logger.info(" conf的内容：" + JSON.toJSONString(conf));
                UserGroupInformation.setConfiguration(conf);
                UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(conf.get(PRINCIPAL), conf.get(KEYTAB));

                ugiMap.put(connidAndTypeKey, ugi);
                checkMap.put(connidAndTypeKey, (principal + "-" + keytab));
                logger.info("☆☆☆ put ugi => ugiMap: " + ugiMap.keySet());
                ugiMap.keySet().stream().forEach(p -> logger.info("key:" + p + ", value:" + ugiMap.get(p)));
                logger.info("☆☆☆ put ugi => ugiMap");
                logger.info("没有票据的情况，处理完成");
            }
        } else {
            //如果没有票据信息，则置空对应的连接
            if (checkMap.containsKey(connidAndTypeKey)) {
                logger.warn("置空对应的连接信息，对应的连接信息为connId");

                ugiMap.put(connidAndTypeKey, null);
                checkMap.put(connidAndTypeKey, null);
            }

        }
    }

    /**
     * 通过连接获取UGI
     *
     * @param connId
     * @return
     */
    public static UserGroupInformation get(String connId,String dbType) {
        logger.info("传入的参数:connid = " + connId);
        logger.info("传入的参数:dbType = " + dbType);
        logger.info("☆☆☆ get ugi <= ugiMap: " + ugiMap.keySet());
        ugiMap.keySet().stream().forEach(p -> logger.info("key:" + p + ", value:" + ugiMap.get(p)));
        logger.info("☆☆☆ get ugi <= ugiMap");
        // 连接+数据库类型 key值
        String connidAndTypeKey = connId + "-" + dbType.toUpperCase();
        logger.info("☆☆☆ get connidAndTypeKey: " + connidAndTypeKey);
        UserGroupInformation ugi = ugiMap.get(connidAndTypeKey);
        logger.info("☆☆☆ get ugi: " + ugi);
        return ugi;
    }

    /**
     * 获得GUI MAP
     *
     * @return
     */
    public static Map<String, UserGroupInformation> getUgiMap() {
        return ugiMap;
    }

    public static Map<String, String> getCheckMap() {
        return checkMap;
    }
}
