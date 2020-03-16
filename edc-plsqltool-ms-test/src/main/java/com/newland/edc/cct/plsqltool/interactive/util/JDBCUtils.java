package com.newland.edc.cct.plsqltool.interactive.util;

import com.newland.bd.model.cfg.GeneralDBBean;
import com.newland.edc.cct.dgw.common.DgwConstant;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.security.PrivilegedExceptionAction;
import java.sql.*;
import java.util.Iterator;
import java.util.List;

public class JDBCUtils {
    protected static final Logger logger   = LoggerFactory.getLogger(JDBCUtils.class);
    protected              String URL      = null;
    protected              String USER     = null;
    protected              String PASSWORD = null;
    protected              String DRIVER   = null;
    protected              String TYPE     = null;
    protected              String keytab;
    protected              String kerberos;
    protected              String principal;
    protected              String jaas;
    String            connId;
    Connection        conn  = null;
    Statement         stm   = null;
    PreparedStatement pstmt = null;

    public Statement getStatement() {
        return this.stm;
    }

    public Connection getConnection() {
        return this.conn;
    }

    public JDBCUtils(String connId) throws Exception {
        this.connId = connId;
        conn =DataSourceAccess.getConnByConnId(connId);
        UserGroupInformation ugiHive = KerberosTool.get(connId, DgwConstant.DB_TYPE_HIVE);
        if(ugiHive!=null){
            this.stm = ugiHive.doAs( new PrivilegedExceptionAction<Statement>(){
                @Override
                public Statement run() throws Exception{
                    return conn.createStatement();
                }
            });
        }else {
            this.stm = conn.createStatement();
        }

    }

    private void initConn() throws Exception {
    }

    public JDBCUtils() {
    }

    private Connection getConnAdapter(GeneralDBBean gdb, String connId) throws Exception {
        Connection conn = null;
        logger.info("开始建立数据库连接,连接属性:{}", gdb);
        String TYPE = gdb.getType();
        String URL = gdb.getUrl();
        if (TYPE == null || TYPE.isEmpty()) {
            if (URL.indexOf("oracle") > 0) {
                TYPE = "oracle";
            } else if (URL.indexOf("mysql") > 0) {
                TYPE = "mysql";
            } else if (URL.indexOf("db2") > 0) {
                TYPE = "db2";
            } else if (URL.indexOf("gbase") > 0) {
                TYPE = "gbase";
            } else if (URL.indexOf("greenplum") > 0) {
                TYPE = "greenplum";
            } else if (URL.indexOf("hive") > 0) {
                TYPE = "hive";
            } else if (URL.indexOf("timesten") > 0) {
                TYPE = "timesten";
            } else {
                logger.error("无法获取到数据库类型,URL{}", URL);
            }

            gdb.setType(TYPE);
            logger.error("无法获取到数据库类型:,使用自动解析URL获得类型{}.", TYPE);
        }

        String USER = gdb.getUsername();
        String PASSWORD = gdb.getPassword();
        String DRIVER = gdb.getDriver();
        if ("hive".equals(TYPE)) {
            this.keytab = gdb.getKeytab();
            this.kerberos = gdb.getKerberos();
            this.principal = gdb.getPrincipal();
            this.jaas = gdb.getJaas();
        }

        Class.forName(DRIVER);
        if (!"oracle".equals(TYPE) && !"mysql".equals(TYPE) && !"db2".equals(TYPE) && !"gbase".equals(TYPE) && !"greenplum".equals(TYPE)) {
            Configuration conf;
            String PRINCIPAL;
            String KEYTAB;
            if ("hive".equals(TYPE)) {
//                if (StringUtils.isNotBlank(keytab) && StringUtils.isNotBlank(kerberos) && StringUtils.isNotBlank(principal)) {
//                    logger.info("Hive进行KB认证" + this.kerberos + " JAAS=" + this.jaas);
//                    System.setProperty("java.security.krb5.conf", this.kerberos);
//                    System.setProperty("java.security.auth.login.config", this.jaas);
//                    conf = new Configuration();
//                    conf.set("hadoop.security.authentication", "kerberos");
//                    conf.set("hadoop.security.authorization", "true");
//                    PRINCIPAL = "username.client.kerberos.principal";
//                    KEYTAB = "username.client.keytab.file";
//                    conf.set(KEYTAB, this.keytab);
//                    conf.set(PRINCIPAL, this.principal);
//                    UserGroupInformation.setConfiguration(conf);
//                    SecurityUtil.login(conf, KEYTAB, PRINCIPAL);
//                }
                KerberosTool.settingKerberosForHive(connId, keytab, kerberos, principal, jaas);
                conn = DriverManager.getConnection(URL, USER, PASSWORD);
            } else if ("hive-kerberos".equals(TYPE)) {
//                System.setProperty("java.security.krb5.conf", this.kerberos);
//                System.setProperty("java.security.auth.login.config", this.jaas);
//                conf = new Configuration();
//                conf.set("hadoop.security.authentication", "kerberos");
//                conf.set("hadoop.security.authorization", "true");
//                PRINCIPAL = "username.client.kerberos.principal";
//                KEYTAB = "username.client.keytab.file";
//                conf.set(KEYTAB, this.keytab);
//                conf.set(PRINCIPAL, this.principal);
//                UserGroupInformation.setConfiguration(conf);
//                SecurityUtil.login(conf, KEYTAB, PRINCIPAL);
                KerberosTool.settingKerberosForHive(connId, keytab, kerberos, principal, jaas);
                conn = DriverManager.getConnection(URL, "", "");
            } else if ("impala".equalsIgnoreCase(TYPE)) {
                conn = DriverManager.getConnection(URL);
            } else {
                if (!"timesten".equalsIgnoreCase(TYPE)) {
                    throw new Exception("无法自动判断连接串对应的数据库类型" + TYPE + ".");
                }

                setLibraryPath((String)System.getenv().get("LD_LIBRARY_PATH"));
                logger.info("获取系统变量LD_LIBRARY_PATH = {}" + (String)System.getenv().get("LD_LIBRARY_PATH"));
                logger.info("更新后的java.library.path = {}" + System.getProperty("java.library.path"));
                String TimesTenURL = "jdbc:timesten:client:dsn=" + URL + ";uid=" + USER + ";pwd=" + PASSWORD;
                logger.info("TimesTen连接串 = {}", TimesTenURL);
                conn = DriverManager.getConnection(TimesTenURL);
            }
        } else {
            conn = DriverManager.getConnection(URL, USER, PASSWORD);
        }

        return conn;
    }

    private static void setLibraryPath(String path) throws Exception {
        System.setProperty("java.library.path", path);
        Field sysPathsField = ClassLoader.class.getDeclaredField("sys_paths");
        sysPathsField.setAccessible(true);
        sysPathsField.set((Object)null, (Object)null);
    }

    public static Connection createConn(GeneralDBBean gdb, String connId) throws Exception {
        Connection conn = null;
        conn = (new JDBCUtils()).getConnAdapter(gdb, connId);
        return conn;
    }

    public boolean executeSql(String sql) throws Exception {
        boolean var2 = true;

        try {
            if (this.stm == null) {
                this.stm = this.conn.createStatement();
            }

            String[] var3 = sql.split(";");
            int var4 = var3.length;

            for(int var5 = 0; var5 < var4; ++var5) {
                String tsql = var3[var5];
                this.stm.execute(tsql);
            }

            return true;
        } catch (SQLException var7) {
            this.closeAll();
            throw new Exception("执行SQL语句" + sql + "时报错：" + var7.getMessage());
        }
    }

    public ResultSet executeQuerySql(String sql) {
        ResultSet rs = null;

        try {
            if (this.stm == null) {
                this.stm = this.conn.createStatement();
            }

            rs = this.stm.executeQuery(sql);
            return rs;
        } catch (SQLException var4) {
            this.closeAll();
            throw new RuntimeException("SQL异常，sql语句：" + sql, var4);
        }
    }

    public long insertBatch(String sql, List<List<String>> params) throws SQLException {
        long count = 0L;

        try {
            this.conn.setAutoCommit(false);
            if (this.pstmt == null) {
                this.pstmt = this.conn.prepareStatement(sql);
            }

            label49:
            for(Iterator var5 = params.iterator(); var5.hasNext(); this.pstmt.addBatch()) {
                List<String> param = (List)var5.next();
                int index = 1;
                Iterator var8 = param.iterator();

                while(true) {
                    while(true) {
                        if (!var8.hasNext()) {
                            continue label49;
                        }

                        String p = (String)var8.next();
                        if (p != null && !"".equals(p)) {
                            this.pstmt.setString(index++, p);
                        } else {
                            this.pstmt.setObject(index++, (Object)null);
                        }
                    }
                }
            }

            int[] cnt = this.pstmt.executeBatch();
            this.conn.commit();
            count = this.arraySum(cnt);
            return count;
        } catch (SQLException var11) {
            try {
                if (this.pstmt != null) {
                    this.pstmt.close();
                }
            } catch (SQLException var10) {
                logger.error(var10.getMessage(), var10);
            }

            throw var11;
        }
    }

    public long insertBatch(String sql, List<List<String>> params, boolean update) throws SQLException {
        long count = 0L;

        try {
            this.conn.setAutoCommit(false);
            if (this.pstmt == null) {
                this.pstmt = this.conn.prepareStatement(sql);
            }

            label53:
            for(Iterator var6 = params.iterator(); var6.hasNext(); this.pstmt.addBatch()) {
                List<String> param = (List)var6.next();
                int index = 1;
                Iterator var9 = param.iterator();

                while(true) {
                    while(true) {
                        if (!var9.hasNext()) {
                            continue label53;
                        }

                        String p = (String)var9.next();
                        if (p != null && !"".equals(p)) {
                            this.pstmt.setString(index++, p);
                        } else {
                            this.pstmt.setObject(index++, (Object)null);
                        }
                    }
                }
            }

            int[] cnt = this.pstmt.executeBatch();
            this.conn.commit();
            if (update) {
                count = (long)this.pstmt.getUpdateCount();
            } else {
                count = this.arraySum(cnt);
            }

            return count;
        } catch (SQLException var12) {
            try {
                if (this.pstmt != null) {
                    this.pstmt.close();
                }
            } catch (SQLException var11) {
                logger.error(var11.getMessage(), var11);
            }

            throw var12;
        }
    }

    public void insertBatchWait(String sql, List<List<String>> params) throws SQLException {
        try {
            this.conn.setAutoCommit(false);
            if (this.pstmt == null || this.pstmt.isClosed()) {
                this.pstmt = this.conn.prepareStatement(sql);
            }

            label56:
            for(Iterator var3 = params.iterator(); var3.hasNext(); this.pstmt.addBatch()) {
                List<String> param = (List)var3.next();
                int index = 1;
                Iterator var12 = param.iterator();

                while(true) {
                    while(true) {
                        if (!var12.hasNext()) {
                            continue label56;
                        }

                        String p = (String)var12.next();
                        if (p != null && !"".equals(p)) {
                            this.pstmt.setString(index++, p);
                        } else {
                            this.pstmt.setObject(index++, (Object)null);
                        }
                    }
                }
            }

            this.pstmt.executeBatch();
        } catch (SQLException var9) {
            try {
                if (this.pstmt != null) {
                    this.pstmt.close();
                }
            } catch (SQLException var8) {
                logger.error(var8.getMessage(), var8);
            }

            String logstr = "";

            String tmp;
            for(Iterator var5 = ((List)params.get(0)).iterator(); var5.hasNext(); logstr = logstr + "#######" + tmp) {
                tmp = (String)var5.next();
            }

            throw new SQLException(var9.getMessage() + "       记录举例:" + logstr, var9);
        }
    }

    public void doCommit() throws SQLException {
        this.conn.commit();
    }

    public void callProcedure(String procedureName, List<String> params) throws SQLException {
        String strProc = "CALL " + procedureName + " (?, ?";
        if (params != null) {
            for(Iterator var4 = params.iterator(); var4.hasNext(); strProc = strProc + ",?") {
                Object ignored = var4.next();
            }
        }

        strProc = strProc + ")";
        CallableStatement cstmt = this.conn.prepareCall(strProc);

        try {
            cstmt.registerOutParameter(1, 4);
            cstmt.registerOutParameter(2, 12);
            if (params != null) {
                for(int i = 0; i < params.size(); ++i) {
                    cstmt.setString(3 + i, ((String)params.get(i)).toString());
                }
            }

            cstmt.executeUpdate();
        } finally {
            cstmt.close();
        }

    }

    public void closeAll() {
        this.closeStm();
        this.closeStmt();
        this.closeConn();
    }

    private void closeConn() {
        if (this.conn != null) {
            try {
                UserGroupInformation ugiHive = KerberosTool.get(connId, DgwConstant.DB_TYPE_HIVE);
                if(ugiHive!=null){
                    ugiHive.doAs( new PrivilegedExceptionAction<Void>(){
                        @Override
                        public Void run() throws Exception{
                            conn.close();
                            return null;
                        }
                    });
                }else {
                    conn.close();
                }
            } catch (Exception var2) {
                logger.error(var2.getMessage(), var2);
            }
        }

    }

    private void closeStm() {
        if (this.stm != null) {
            try {
                UserGroupInformation ugiHive = KerberosTool.get(connId, DgwConstant.DB_TYPE_HIVE);
                if(ugiHive!=null){
                    ugiHive.doAs( new PrivilegedExceptionAction<Void>(){
                        @Override
                        public Void run() throws Exception{
                            stm.close();
                            return null;
                        }
                    });
                }else {
                    stm.close();
                }
            } catch (Exception var2) {
                logger.error(var2.getMessage(), var2);
            }
        }

    }

    private void closeStmt() {
        try {
            if (this.pstmt != null) {
                UserGroupInformation ugiHive = KerberosTool.get(connId, DgwConstant.DB_TYPE_HIVE);
                if(ugiHive!=null){
                    ugiHive.doAs( new PrivilegedExceptionAction<Void>(){
                        @Override
                        public Void run() throws Exception{
                            pstmt.close();
                            return null;
                        }
                    });
                }else {
                    pstmt.close();
                }
            }
        } catch (Exception var2) {
            logger.error(var2.getMessage(), var2);
        }

    }

    private long arraySum(int[] arr) {
        if (arr == null) {
            return 0L;
        } else {
            long res = 0L;
            int[] var4 = arr;
            int var5 = arr.length;

            for(int var6 = 0; var6 < var5; ++var6) {
                int i = var4[var6];
                if (i > 0) {
                    res += (long)i;
                }
            }

            return res;
        }
    }

    /**
     * 构造分页查询SQL语句
     *
     * @param db_type    ORCLE,MYSQL
     * @param sql
     * @param start_page
     * @param page_count
     * @return
     */
    public static String buildPageQuerySQL(String db_type, String sql, int start_page, int page_count) {
        StringBuffer execSql = new StringBuffer(500);
        if (start_page <= 0) {
            start_page = 1;
        }
        if (db_type.equalsIgnoreCase("ORACLE")) {
            int startRow = (start_page - 1) * page_count + 1;
            int endRow = startRow + page_count - 1;

            execSql.append("select * from (select b.*, rownum as r_n from (");
            execSql.append(sql);
            execSql.append(") b where rownum <= " + endRow + ") c where c.r_n >= " + startRow);
            execSql.trimToSize();

        } else if (db_type.equalsIgnoreCase("MYSQL")) {
            int startRow = (start_page - 1) * page_count;

            execSql.append(sql);
            execSql.append(" limit " + startRow + "," + page_count);
            execSql.trimToSize();
        }

        logger.info("☆☆☆构造分页查询SQL语句:" + execSql.toString());
        return execSql.toString();
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}