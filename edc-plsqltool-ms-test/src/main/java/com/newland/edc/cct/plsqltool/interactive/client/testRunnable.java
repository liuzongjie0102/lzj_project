package com.newland.edc.cct.plsqltool.interactive.client;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.newland.bd.model.var.ConnInfo;
import com.newland.bd.ms.core.model.RespInfo;
import com.newland.bd.ms.core.utils.RespJsonUtils;
import com.newland.edc.cct.dgw.common.DgwConstant;
import com.newland.edc.cct.plsqltool.interactive.util.DataSourceAccess;
import com.newland.edc.cct.plsqltool.interactive.util.KerberosTool;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class testRunnable implements Runnable{

    private static final Logger logger = LoggerFactory.getLogger(testRunnable.class);

    private String name;
    private String connId;
    private String confUrl;
    private String sql;

    public testRunnable(String name, String connId, String sql, String confUrl) {
        this.name = name;
        this.connId = connId;
        this.sql = sql;
        this.confUrl = confUrl;
    }

    @Override public void run() {
        try {
            if (org.apache.commons.lang3.StringUtils.isEmpty(connId)) {
                throw new Exception("连接ID不能为空");
            }

            ConnInfo connInfo = null; // 连接信息
            Client client = ClientBuilder.newClient().register(JacksonJsonProvider.class);
            logger.info("☆☆☆ connId = " + connId);
            // logger.info("☆☆☆ RestFul Service Url = " +
            // RestFulServiceUtils.getServiceUrl("nl-edc-cct-sys-ms") + "conn/group/id");

            StringBuilder url = new StringBuilder(confUrl + "conn/group/id?connId=" + connId);

            logger.info("☆☆☆ RestFul Service Url  = " + url.toString());

            WebTarget target = client.target(url.toString());
            Response response = target.request().get();

            if (response.getStatus() == 200) {
                if (response.getLength() != 0) {
                    RespInfo respInfo = response.readEntity(RespInfo.class);
                    connInfo = (ConnInfo) RespJsonUtils.parseObject(respInfo.getRespData(), ConnInfo.class);
                    if (connInfo == null) {
                        throw new Exception("无法根据connId获取连接信息，请确认资源与连接关系是否已配置。connId = " + connId);
                    }
                    logger.info("☆☆☆ connInfo  = " + JSON.toJSONString(connInfo));
                }
            } else {
                logger.error("★★★ response error  = " + response.getStatusInfo());
                throw new Exception("根据connId获取连接信息，微服务调用失败。RESOURCE_ID = " + connId);
            }
            Connection connection = DataSourceAccess.getConnByResourceId(connId, connInfo,"");

            while (true){
                UserGroupInformation ugiHive = KerberosTool.get(connId, DgwConstant.DB_TYPE_HIVE);
                if (ugiHive != null) {
                    logger.info("-------------------------test kb------------------------");
                    ugiHive.doAs(new PrivilegedExceptionAction<Void>() {
                        @Override public Void run() throws Exception {
                            try (Statement stm = connection.createStatement();
                                            ResultSet rs = stm.executeQuery(sql)
                            ){
                                logger.info("--------------------------------kb ResultSet------------------------");
                                if(rs.next()){
                                    String line = rs.getString(1);
                                    logger.info(name + " select success------------------------"+line);
                                }
                                return null;
                            }
                        }
                    });
                }else {
                    try (Statement stm = connection.createStatement();
                                    ResultSet rs = stm.executeQuery(sql)
                    ){
                        logger.info("--------------------------------ResultSet------------------------");
                        while(rs.next()){
                            String line = rs.getString(1);
                            logger.info("select success------------------------"+line);
                        }
                    }
                }
                Thread.sleep(10000);
            }
        }catch (Exception e){
            logger.error(e.getMessage(),e);
            System.exit(-2);
        }
    }
}
