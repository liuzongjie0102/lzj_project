package com.newland.edc.cct.plsqltool.interactive.util;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.newland.bd.model.cfg.GeneralDBBean;
import com.newland.bd.model.cfg.HadoopCfgBean;
import com.newland.bd.model.var.ConnInfo;
import com.newland.bd.model.var.VarInfo;
import com.newland.bd.ms.core.model.RespInfo;
import com.newland.bd.ms.core.utils.RespJsonUtils;
import com.newland.bi.util.common.Function;
import com.newland.bi.util.config.ParameterConfig;
import com.newland.edc.cct.dgw.common.DgwConstant;
import com.newland.edc.cct.dgw.utils.rest.RestFulServiceUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 数据源访问
 *
 * @author zhengliexin Nov 7, 2017
 * @version $Revision: 1.0
 */
public class DataSourceAccess {
	private static Logger logger = LoggerFactory.getLogger(DataSourceAccess.class);
	// //数据源和数据源bean id映射
	// private static Map<String, String> resAndDataSourceMap = null;
	// 数据源对应的库类型
	// private static Map<String, String> resAndDbtypeMap = null;

	// 是否需要跳转环境
	// public static String IS_REDIRECT_ENV =
	// ParameterConfig.getValue("IS_REDIRECT_ENV");
	// 跳转环境map : <实体类型 ： <环境类型 ： 地址参数 > >
	private static Map<String, Map<String, String>> redirect_env_map = null;

	/**
	 * 根据资源ID获取跳转的url
	 *
	 * @return
	 * @throws Exception
	 * @create by zhengliexin 2018年10月3日
	 * @update by zhengliexin 2018年10月3日
	 * @version $Revision: 1.0
	 */
	public static Map<String, Map<String, String>> getRedirectUrlByResourceId() throws Exception {

		// 缓存配置文件中的内容
		if (redirect_env_map == null) {
			redirect_env_map = new HashMap<>();

			String redirect_env_conf = ParameterConfig.getValue("REDIRECT_ENV_CONF");

			if (StringUtils.isNotEmpty(redirect_env_conf)) {
				String[] arr_redirect_env_conf = redirect_env_conf.split(";");

				for (int i = 0; i < arr_redirect_env_conf.length; i++) {
					String tmp_str = arr_redirect_env_conf[i];

					if (StringUtils.isNotEmpty(tmp_str)) {
						String[] arr_values = tmp_str.split("\\|");

						String entity_type = arr_values[0];
						String env_type = arr_values[1];
						String url_param = arr_values[2];

						if (redirect_env_map.containsKey(entity_type)) {
							redirect_env_map.get(env_type).put(env_type, url_param);
						} else {
							Map<String, String> tmp_map = new HashMap<>();
							tmp_map.put(env_type, url_param);
							redirect_env_map.put(entity_type, tmp_map);
						}
					}
				}
			}
			logger.info("缓存的跳转信息为：" + JSON.toJSONString(redirect_env_map));
		}

		return redirect_env_map;
	}

	/**
	 * 获取对应的地址
	 *
	 * @param db_type    库类型
	 * @param cluster_id 集群ID
	 * @return
	 * @throws Exception
	 * @create by zhengliexin 2018年10月8日
	 * @update by zhengliexin 2018年10月8日
	 * @version $Revision: 1.0
	 */
	public static String getRedirectUrl(String db_type, String cluster_id) throws Exception {
		String url = "";
		String IS_REDIRECT_ENV = ParameterConfig.getValue("IS_REDIRECT_ENV");
		// 如果全局设置不需要跳转，则不需要跳转
		if (StringUtils.isEmpty(IS_REDIRECT_ENV) || "0".equals(IS_REDIRECT_ENV)) {
			return url;
		}

		Map<String, Map<String, String>> map = getRedirectUrlByResourceId();

		// 没有找到对应的资源
		if (!map.containsKey(db_type)) {
			return url;
		}

		if (!map.get(db_type).containsKey(cluster_id)) {
			return url;
		}

		url = map.get(db_type).get(cluster_id);

		return url;
	}

	/**
	 * 根据库类型判断是否需要跳转
	 *
	 * @param db_type
	 * @return
	 * @throws Exception
	 * @create by zhengliexin 2018年10月10日
	 * @update by zhengliexin 2018年10月10日
	 * @version $Revision: 1.0
	 */
	public static boolean isRedirectByDbType(String db_type) throws Exception {
		boolean flag = false;
		String IS_REDIRECT_ENV = ParameterConfig.getValue("IS_REDIRECT_ENV");
		// 如果全局设置不需要跳转，则不需要跳转
		if (StringUtils.isEmpty(IS_REDIRECT_ENV) || "0".equals(IS_REDIRECT_ENV)) {
			return false;
		}

		Map<String, Map<String, String>> map = getRedirectUrlByResourceId();

		// 没有找到对应的资源
		if (map.containsKey(db_type)) {
			return true;
		}

		return flag;
	}

	/**
	 * 根据连接信息获取数据库连接
	 *
	 * @param connId   : 一般为connID 或者 资源ID
	 * @param connInfo
	 * @param err_msg
	 * @return
	 * @throws Exception
	 */
	public static Connection getConnByResourceId(String connId, ConnInfo connInfo, String err_msg) throws Exception {
		Connection conn = null;
		try {
			// String data_source_id = accessDataSourceIdForSingle(resource_id);
			// Connection conn = new DBUtil(data_source_id).getConnection();
			String user_name = ""; // 用户名
			String password = ""; // 密码
			String url = ""; // 连接串
			String driverClass = ""; // 驱动
			String dbType = ""; // 数据库类型

			String keytab = "";// kb_keytab KB认证keytab xxxxx.keytab
			String kerberos = "";// kb_conf kerberos认证配置 名为krb5.conf的配置文件
			String principal = "";// kb_principal KB认证域名例hadoop/edc03@NEWLAND.COMd
			String jaas = "";// jaas_conf JAAS配置路径

			List<VarInfo> connVars = connInfo.getConnVar();
			List<VarInfo> resourceVars = connInfo.getResourceVar();

			// 连接参数
			if (connVars != null) {
				for (VarInfo varInfo : connVars) {
					if ("user_name".equalsIgnoreCase(varInfo.getVarName())) {
						user_name = varInfo.getVarValue();
					} else if ("password".equalsIgnoreCase(varInfo.getVarName())) {
						password = varInfo.getVarValue();
					} else if ("tns".equalsIgnoreCase(varInfo.getVarName())) {
						url = varInfo.getVarValue();
					} else if ("kb_keytab".equalsIgnoreCase(varInfo.getVarName())) {
						keytab = varInfo.getVarValue();
					} else if ("kb_conf".equalsIgnoreCase(varInfo.getVarName())) {
						kerberos = varInfo.getVarValue();
					} else if ("kb_principal".equalsIgnoreCase(varInfo.getVarName())) {
						principal = varInfo.getVarValue();
					} else if ("jaas_conf".equalsIgnoreCase(varInfo.getVarName())) {
						jaas = varInfo.getVarValue();
					}
				}
			}

			// 引用参数
			if (resourceVars != null) {
				for (VarInfo varInfo : resourceVars) {
					if ("tns".equalsIgnoreCase(varInfo.getVarName())) {

						if (StringUtils.isEmpty(url)) {
							// 当连接参数没有值的情况下，才能将引用参数赋值给url
							url = varInfo.getVarValue();
						}

					}
				}
			}

			// 设置URL的值
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
				} else if (url.indexOf("greenplum") > 0) {
					dbType = "greenplum";
					driverClass = "com.pivotal.jdbc.GreenplumDriver";
				} else if (url.indexOf("xcloud") > 0) {
					dbType = "x-cloud";
					driverClass = "com.bonc.xcloud.jdbc.XCloudDriver";
				} else {
					throw new Exception("无法获取数据库类型及驱动程序。" + err_msg);
				}
			}

			if (!Function.equalsNull(driverClass)) {

				if ("hive".equals(dbType)) {

					//                    if (StringUtils.isNotEmpty(keytab) && StringUtils.isNotEmpty(kerberos) && StringUtils.isNotEmpty(principal)) {
					//                        System.setProperty("java.security.krb5.conf", kerberos);
					////						System.setProperty("java.security.auth.login.config", jaas);
					//
					//                        Configuration conf = new Configuration();
					//                        conf.set("hadoop.security.authentication", "kerberos");
					//                        conf.set("hadoop.security.authorization", "true");
					//
					//                        String PRINCIPAL = "username.client.kerberos.principal";
					//                        String KEYTAB = "username.client.keytab.file";
					//                        conf.set(PRINCIPAL, principal);
					//                        conf.set(KEYTAB, keytab);
					//
					//
					//                        UserGroupInformation.setConfiguration(conf);
					//
					////                        //根据情况刷新KB认证信息
					//                        DataSourceAccess.refreshKB();
					////                        //登录
					//                        SecurityUtil.login(conf, KEYTAB, PRINCIPAL);
					//
					//                    }

					KerberosTool.settingKerberosForHive(connId, keytab, kerberos, principal, jaas);
				}

				// 设置驱动类
				Class.forName(driverClass);

				if ("oracle".equals(dbType) || "mysql".equals(dbType) || "db2".equals(dbType) || "gbase".equals(dbType) || "greenplum".equals(dbType) || "x-cloud".equals(dbType)) {
					conn = DriverManager.getConnection(url, user_name, password);
				} else if ("hive".equals(dbType)) {
					if (StringUtils.isEmpty(user_name)) {
						user_name = "";
					}
					if (StringUtils.isEmpty(password)) {
						password = "";
					}


					UserGroupInformation ugi = KerberosTool.get(connId,DgwConstant.DB_TYPE_HIVE);

					if( null  != ugi ){
						String finalUser_name = user_name;
						String finalUrl = url;
						String finalPassword = password;
						conn = ugi.doAs(new PrivilegedExceptionAction<Connection>() {

							@Override
							public Connection run() throws Exception {
								return DriverManager.getConnection(finalUrl, finalUser_name, finalPassword);
							}
						});
					}else{
						conn = DriverManager.getConnection(url, user_name, password);
					}




				} else if ("impala".equals(dbType) || "timesten".equals(dbType)) {
					conn = DriverManager.getConnection(url);
				}
			} else {
				throw new Exception("无法获取数据库驱动，建立连接失败。RESOURCE_ID = " + err_msg);
			}

		} catch (Exception e) {
			logger.error("",e);
			throw new Exception("从cct获取连接信息失败:" + e.getMessage());
		}

		return conn;
	}

	/**
	 * 根据资源取连接
	 *
	 * @param resource_id
	 * @return
	 * @throws Exception
	 * @create by zhengliexin Nov 24, 2017
	 * @update by zhengliexin Nov 24, 2017
	 * @version $Revision: 1.0
	 */
	public static Connection getConnByResourceId(String resource_id, String tenant_id) throws Exception {
		ConnInfo connInfo = getConnInfo(resource_id, tenant_id);
		Connection conn = getConnByResourceId(resource_id, connInfo, "RESOURCE_ID = " + resource_id + " ; tenant_id = " + tenant_id);
		return conn;
	}

	/**
	 * 根据资源取连接
	 *
	 * @param connId
	 * @return
	 * @throws Exception
	 * @create by zhengliexin Nov 24, 2017
	 * @update by zhengliexin Nov 24, 2017
	 * @version $Revision: 1.0
	 */
	public static Connection getConnByConnId(String connId) throws Exception {
		ConnInfo connInfo = getConnInfoByConnId(connId);
		Connection conn = getConnByResourceId(connId, connInfo, "connId = " + connId);
		return conn;
	}

	/**
	 * 通过接口获取连接信息
	 *
	 * @param resource_id
	 * @return
	 * @throws Exception
	 * @create by zhengliexin 2018年8月9日
	 * @update by zhengliexin 2018年8月9日
	 * @version $Revision: 1.0
	 */
	public static ConnInfo getConnInfo(String resource_id, String tenant_id) throws Exception {
		ConnInfo connInfo = null; // 连接信息
		Client client = ClientBuilder.newClient().register(JacksonJsonProvider.class);
		logger.info("☆☆☆ RESOURCE_ID = " + resource_id);
		logger.info("☆☆☆ TENANT_ID = " + tenant_id);
		// logger.info("☆☆☆ RestFul Service Url = " +
		// RestFulServiceUtils.getServiceUrl("nl-edc-cct-sys-ms") + "conn/group/id");

		StringBuilder url = new StringBuilder(RestFulServiceUtils.getServiceUrl("nl-edc-cct-sys-ms") + "conn/group/id?resourceId=" + resource_id);
		if (StringUtils.isNotEmpty(tenant_id)) {
			url.append("&tenantId=" + tenant_id);
		}

		logger.info("☆☆☆ RestFul Service Url  = " + url.toString());

		WebTarget target = client.target(url.toString());
		// Response response = target.queryParam("resourceId",
		// resource_id).queryParam("tenantId", tenant_id).request().get();
		Response response = target.request().get();

		if (response.getStatus() == 200) {
			if (response.getLength() != 0) {
				RespInfo respInfo = response.readEntity(RespInfo.class);
				connInfo = (ConnInfo) RespJsonUtils.parseObject(respInfo.getRespData(), ConnInfo.class);
				if (connInfo == null) {
					throw new Exception("无法根据RESOURCE_ID获取连接信息，请确认资源与连接关系是否已配置。RESOURCE_ID = " + resource_id);
				}
				logger.info("☆☆☆ connInfo  = " + JSON.toJSONString(connInfo));
			}
		} else {
			logger.error("★★★ response error  = " + response.getStatusInfo());
			throw new Exception("根据RESOURCE_ID获取连接信息，微服务调用失败。RESOURCE_ID = " + resource_id);
		}
		return connInfo;
	}

	/**
	 * 根据资源ID获取环境类型 //
	 * http://10.1.8.6:8291/nl-edc-cct-sys-ms/v1/conn/qryFitCluster?resourceId=C48DEF913AD22A9A029704C0ABFC11E8
	 *
	 * @param resource_id
	 * @return
	 * @throws Exception
	 * @create by zhengliexin 2018年10月3日
	 * @update by zhengliexin 2018年10月3日
	 * @version $Revision: 1.0
	 */
	@SuppressWarnings("unchecked")
	public static String getEnvTypeByResourceId(String resource_id) throws Exception {

		String env_msg = "";
		if (StringUtils.isEmpty(resource_id)) {
			return env_msg;
		}

		Client client = ClientBuilder.newClient().register(JacksonJsonProvider.class);

		logger.info("☆☆☆ RESOURCE_ID = " + resource_id);
		StringBuilder url = new StringBuilder(RestFulServiceUtils.getServiceUrl("nl-edc-cct-sys-ms") + "conn/qryFitCluster?resourceId=" + resource_id);
		logger.info("☆☆☆ RestFul Service Url  = " + url.toString());

		WebTarget target = client.target(url.toString());
		// WebTarget target =
		// client.target("http://10.1.8.6:8291/nl-edc-cct-sys-ms/v1/conn/qryFitCluster?resourceId=C48DEF913AD22A9A029704C0ABFC11E8");
		Response response = target.request().get();

		if (response.getStatus() == 200) {
			if (response.getLength() != 0) {
				RespInfo respInfo = response.readEntity(RespInfo.class);
				logger.info("☆☆☆ respInfo  = " + JSON.toJSONString(respInfo));
				if (null != respInfo && null != respInfo.getRespData()) {
					Map<String, Object> resMap = (Map<String, Object>) respInfo.getRespData();
					Object varValue = resMap.get("varValue");

					if (null != varValue) {
						env_msg = String.valueOf(varValue);
					}
				}

			}
		} else {
			logger.error("★★★ response error  = " + response.getStatusInfo());
			throw new Exception("根据RESOURCE_ID获取环境类型信息，微服务调用失败。RESOURCE_ID = " + resource_id);
		}
		return env_msg;
	}

	/**
	 * 根据连接ID获得对应的连接信息
	 *
	 * @param connId
	 * @return
	 * @throws Exception
	 * @create by zhengliexin 2018年10月11日
	 * @update by zhengliexin 2018年10月11日
	 * @version $Revision: 1.0
	 */
	public static ConnInfo getConnInfoByConnId(String connId) throws Exception {

		if (StringUtils.isEmpty(connId)) {
			throw new Exception("连接ID不能为空");
		}

		ConnInfo connInfo = null; // 连接信息
		Client client = ClientBuilder.newClient().register(JacksonJsonProvider.class);
		logger.info("☆☆☆ connId = " + connId);
		// logger.info("☆☆☆ RestFul Service Url = " +
		// RestFulServiceUtils.getServiceUrl("nl-edc-cct-sys-ms") + "conn/group/id");

		StringBuilder url = new StringBuilder(RestFulServiceUtils.getServiceUrl("nl-edc-cct-sys-ms") + "conn/group/id?connId=" + connId);

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
		return connInfo;

	}

	/**
	 * @param resource_id
	 * @param tenant_id
	 * @param conn_id
	 * @return
	 * @throws Exception
	 * @target 获取hbase的连接配置信息
	 */

//	public static List<Object> getCfgForHbase(String resource_id, String tenant_id, String conn_id) throws Exception {
//		ConnInfo connInfo = null;
//
//		if (StringUtils.isNotEmpty(conn_id)) {
//			connInfo = DataSourceAccess.getConnInfoByConnId(conn_id);
//			//            connInfo = DataSourceAccess.getConnInfo(resource_id, tenant_id);
//			//            if (connInfo == null && StringUtils.isNotEmpty(conn_id)) {
//			//
//			//                connInfo = DataSourceAccess.getConnInfoByConnId(conn_id);
//			//
//			//            }
//		} else if (StringUtils.isNotEmpty(resource_id)) {
//
//			try {
//				connInfo = DataSourceAccess.getConnInfo(resource_id, tenant_id);
//			} catch (Exception e) {
//				logger.info(e.getMessage());
//			}
//			if (connInfo == null && StringUtils.isNotEmpty(conn_id)) {
//
//				connInfo = DataSourceAccess.getConnInfoByConnId(conn_id);
//
//			}
//
//		}
//
//		if(null == conn_id){
//			conn_id = resource_id;
//		}
//
//		return DataSourceAccess.getCfgForHbase(conn_id,connInfo);
//
//	}

	/**
	 * 获取hbase的连接配置信息
	 *
	 * @param connInfo
	 * @return
	 * @throws Exception
	 */
//	public static List<Object> getCfgForHbase(String connId,ConnInfo connInfo) throws Exception {
//		List<Object> list = new ArrayList<Object>();
//		String ip = null;
//		String port = null;
//		String hbase_master = null;
//
//		String conf_path = "";
//		String keytab = "";// kb_keytab KB认证keytab xxxxx.keytab
//		String kerberos = "";// kb_conf kerberos认证配置 名为krb5.conf的配置文件
//		String principal = "";// kb_principal KB认证域名例hadoop/edc03@NEWLAND.COMd
//		String jaas = "";// jaas_conf JAAS配置路径
//		String nameSpace = "";
//		List<VarInfo> connVars = connInfo.getConnVar();
//		List<VarInfo> resourceVar = connInfo.getResourceVar();
//
//		if (resourceVar != null) {
//			for (VarInfo varInfo : resourceVar) {
//				if ("zk_ip".equalsIgnoreCase(varInfo.getVarName())) {
//					ip = varInfo.getVarValue();
//				} else if ("zk_port".equalsIgnoreCase(varInfo.getVarName())) {
//					port = varInfo.getVarValue();
//				} else if ("hbase_master".equalsIgnoreCase(varInfo.getVarName())) {
//					hbase_master = varInfo.getVarValue();
//				} else if ("conf_path".equalsIgnoreCase(varInfo.getVarName())) {
//					conf_path = varInfo.getVarValue();
//				}
//			}
//		}
//
//		// 连接参数
//		if (connVars != null) {
//			for (VarInfo varInfo : connVars) {
//
//				if ("kb_keytab".equalsIgnoreCase(varInfo.getVarName())) {
//					keytab = varInfo.getVarValue();
//				} else if ("kb_conf".equalsIgnoreCase(varInfo.getVarName())) {
//					kerberos = varInfo.getVarValue();
//				} else if ("kb_principal".equalsIgnoreCase(varInfo.getVarName())) {
//					principal = varInfo.getVarValue();
//				} else if ("jaas_conf".equalsIgnoreCase(varInfo.getVarName())) {
//					jaas = varInfo.getVarValue();
//				} else if("namespace".equalsIgnoreCase(varInfo.getVarName())) {
//					nameSpace = varInfo.getVarValue();
//				}
//			}
//		}
//
//		Configuration conf = HBaseConfiguration.create();
//		// 客户端文件地址
//		if (StringUtils.isNotEmpty(conf_path)) {
//			String[] paths = conf_path.split(",");
//			for (int i = 0; i < paths.length; i++) {
//				logger.info("加入的文件：" + paths[i]);
//				conf.addResource(new Path(paths[i]));
//			}
//		}
//
//
//		logger.info("hbase.zookeeper.property.clientPort:" + port);
//		logger.info("hbase.zookeeper.quorum:" + ip);
//
//		conf.set("hbase.zookeeper.property.clientPort", port);
//		conf.set("hbase.zookeeper.quorum", ip);
//		logger.info("认证内容：keytab=" + keytab + " kerberos=" + kerberos + " principal=" + principal);
//		if(StringUtils.isNotEmpty(keytab) && StringUtils.isNotEmpty(kerberos) && StringUtils.isNotEmpty(principal)) {
//			conf.set("hbase.security.authentication","kerberos");
//			logger.info("开始添加认证信息");
//			KerberosTool.settingKerberos(connId,keytab,kerberos,principal,conf);
//		}
//		//        if (StringUtils.isNotEmpty(keytab) && StringUtils.isNotEmpty(kerberos) && StringUtils.isNotEmpty(principal)) {
//		//
//		//            logger.info("设置认证内容开始");
//		//
//		//            System.setProperty("java.security.krb5.conf", kerberos);
//		//            // System.setProperty("java.security.auth.login.config", jaas);
//		//
//		//            // conf.set("hadoop.security.authentication", "kerberos");
//		//            // conf.set("hadoop.security.authorization", "true");
//		//            // conf.set("hbase.security.authentication","kerberos");
//		//
//		//            String PRINCIPAL = "username.client.kerberos.principal";
//		//            String KEYTAB = "username.client.keytab.file";
//		//            conf.set(KEYTAB, keytab);
//		//            conf.set(PRINCIPAL, principal);
//		//
//		//            // conf.set("keytab.file",keytab);
//		//            // conf.set("kerberos.principal",principal);
//		//            // conf.set("hbase.master.kerberos.principal",principal);
//		//            // conf.set("hbase.regionserver.kerberos.principal",principal);
//		//            // conf.set("zookeeper.znode.parent", "/hbase-secure");
//		//
//		//
//		//            UserGroupInformation.setConfiguration(conf);
//		//
//		//            //根据情况刷新KB认证信息
//		//            DataSourceAccess.refreshKB();
//		//
//		//            // SecurityUtil.login(conf, KEYTAB, PRINCIPAL);
//		//            UserGroupInformation.loginUserFromKeytab(principal, keytab);
//		//
//		//            logger.info("设置认证内容完成");
//		//        }
//
//		list.add(conf);
//		list.add(nameSpace);
//		return list;
//
//	}


	/**
	 * KB认证文件刷新时间
	 */
	private static Long kbRefreshTime = 0L;


	/**
	 * 刷新KB票据信息
	 *
	 * @return
	 * @throws Exception
	 * @create by zhengliexin 2019年4月12日
	 * @update by zhengliexin 2019年4月12日
	 * @version $Revision: 1.0
	 */
	public static void refreshKB() throws Exception {
		UserGroupInformation info = UserGroupInformation.getLoginUser();
		if (null != info) {
			info.checkTGTAndReloginFromKeytab();
		}

		//        boolean is_refresh = false;
		//        long curr_time = System.currentTimeMillis();
		//        long timeout_time = 1000 * 60 * 60 * 24L;
		//
		//                if (0L == kbRefreshTime) {
		//                    //如果刷新时间为0，说明第一次访问，需要刷新
		//                    is_refresh = true;
		//                } else if (curr_time - DataSourceAccess.kbRefreshTime > timeout_time) {
		//                    //如果票据刷新时间超过24个小时，需要刷新
		//                    is_refresh = true;
		//                }
		//
		//                if (is_refresh) {
		//                    //如果需要刷新的操作
		//                    UserGroupInformation info = UserGroupInformation.getLoginUser();
		//                    if (null != info) {
		//                info.checkTGTAndReloginFromKeytab();
		//
		//                //刷新后记录刷新时间，进入下个轮回
		//                DataSourceAccess.kbRefreshTime = curr_time;
		//            }
		//        }
	}


	// public static void main(String[] args) {
	//
	// try {
	// String s =
	// DataSourceAccess.getEnvTypeByResourceId("C48DEF913AD22A9A029704C0ABFC11E8");
	// System.out.println(s);
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	// }

	// public static void main(String[] args) {
	// try {
	// DataSourceAccess.getConnByResourceId("EF8EB942C13177FB789F3CF0786211E8",
	// "34234234");
	//// DataSourceAccess.getConnByResourceId("EF8EB942C13177FB789F3CF0786211E8");
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	// }

	//	public static void main(String[] args) {
	//
	//		try {
	//			// String s =
	//			// DataSourceAccess.getRedirectUrlByResourceId("C48DEF913AD22A9A029704C0ABFC11E8");
	//			// System.out.println(s);
	//
	//			DataSourceAccess.getRedirectUrlByResourceId();
	//
	//		} catch (Exception e) {
	//			e.printStackTrace();
	//		}
	//	}

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
        } catch (Exception var14) {
            logger.error(var14.getMessage(), var14);
            throw new Exception(connId + "获取hadoop配置错误，" + var14.getMessage());
        }
    }
}
