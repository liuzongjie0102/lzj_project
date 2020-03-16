package com.newland.edc.cct.plsqltool.interactive.tool;

import com.newland.bi.util.logger.BaseLogger;
import com.newland.edc.cct.plsqltool.interactive.util.DataSourceAccess;
import com.newland.edc.cct.plsqltool.interactive.util.KerberosTool;
import org.apache.hadoop.security.UserGroupInformation;

import java.security.PrivilegedExceptionAction;
import java.sql.*;

public class SqlCountTask {

    private static BaseLogger log = (BaseLogger) BaseLogger.getLogger(SqlCountTask.class);

    protected Connection conn = null;
    protected PreparedStatement ps = null;
    protected ResultSet rs = null;
    protected String conn_id = null;
    protected String sqlString = null;
    protected String db_type = null;

    public SqlCountTask(String sqlString,String conn_id,String db_type){
        this.sqlString = sqlString;
        this.conn_id = conn_id;
        this.db_type = db_type;
    }

    public long execute() throws Exception{
        long count = 0;
        sqlString = "select count(1) as record_count from ("+sqlString+") t ";
        try {
            conn = DataSourceAccess.getConnByConnId(conn_id);
            UserGroupInformation ugi = KerberosTool.get(conn_id,db_type);
            if("hive".equalsIgnoreCase(db_type)){
                if(ugi!=null){
                    count = ugi.doAs( new PrivilegedExceptionAction<Long>() {
                        @Override public Long run() throws Exception {
                            long record_count = 0;
                            ps = conn.prepareStatement(sqlString);
                            rs = ps.executeQuery();
                            while (rs.next()) {
                                record_count = rs.getLong("record_count");
                            }
                            return record_count;
                        }
                    });
                }else{
                    ps = conn.prepareStatement(sqlString);
                    rs = ps.executeQuery();
                    while (rs.next()) {
                        count = rs.getLong("record_count");
                    }
                }
            }else{
                ps = conn.prepareStatement(sqlString);
                rs = ps.executeQuery();
                while (rs.next()) {
                    count = rs.getLong("record_count");
                }
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }finally {
            closeAll(conn,ps,rs,null);
        }
        return count;
    }

    protected void closeAll(Connection conn, PreparedStatement ps, ResultSet rs, ResultSetMetaData rsmd) throws SQLException {
        if (rsmd != null) {
            rsmd = null;
        }

        if (rs != null) {
            rs.close();
            rs = null;
        }

        if (ps != null) {
            ps.close();
            ps = null;
        }

        if (conn != null) {
            conn.close();
            conn = null;
        }
    }
}
