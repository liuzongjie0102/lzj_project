package com.newland.edc.cct.plsqltool.interactive.tool;

import com.newland.bi.util.logger.BaseLogger;
import com.newland.edc.cct.plsqltool.interactive.util.DataSourceAccess;
import com.newland.edc.cct.plsqltool.interactive.util.KerberosTool;
import org.apache.hadoop.security.UserGroupInformation;

import java.security.PrivilegedExceptionAction;
import java.sql.*;

public class DropTableTask {

    private static BaseLogger log = (BaseLogger) BaseLogger.getLogger(DropTableTask.class);

    private String conn_id;
    private String db_type;
    private String tab_name;

    protected Connection conn = null;
    protected PreparedStatement ps = null;
    protected ResultSet rs = null;
    protected DatabaseMetaData dbmd = null;
    protected String sqlString = null;
    protected String schema = null;

    public DropTableTask(String conn_id,String db_type,String tab_name){
        this.conn_id = conn_id;
        this.db_type = db_type;
        this.tab_name = tab_name;
    }

    public void executeDrop() throws Exception{
        try {
            conn = DataSourceAccess.getConnByConnId(conn_id);
            UserGroupInformation ugi = KerberosTool.get(conn_id,db_type);
            if(db_type.equalsIgnoreCase("hive")||db_type.equalsIgnoreCase("mysql")||db_type.equalsIgnoreCase("gbase")){
                sqlString = "drop table if exists "+tab_name;
                if(ugi!=null){
                    ugi.doAs( new PrivilegedExceptionAction<Void>(){
                        @Override
                        public Void run() throws Exception{
                            schema = conn.getSchema();
                            return null;
                        }
                    });
                }else if(db_type.equalsIgnoreCase("mysql")){
                    String[] dbNames = conn.getMetaData().getUserName().split("@");
                    schema = dbNames[0];
                }else{
                    schema = conn.getSchema();
                }
            }else if(db_type.equalsIgnoreCase("oracle")||db_type.equalsIgnoreCase("db2")||db_type.equalsIgnoreCase("greenplum")){
                sqlString = "drop table "+tab_name;
                schema = conn.getMetaData().getUserName();
            }
            if(ugi!=null){
                ugi.doAs( new PrivilegedExceptionAction<Void>(){
                    @Override
                    public Void run() throws Exception{
                        dbmd = conn.getMetaData();
                        rs = dbmd.getTables(null, schema.toUpperCase(), tab_name.toUpperCase(), new String[] { "TABLE" });
                        if(rs.next()){
                            ps = conn.prepareStatement(sqlString);
                            ps.executeUpdate();
                        }
                        return null;
                    }
                });
            }else{
                dbmd = conn.getMetaData();
                if(db_type.equalsIgnoreCase("greenplum")){
                    rs = dbmd.getTables(null, null, tab_name, new String[] { "TABLE" });
                }else if(db_type.equalsIgnoreCase("mysql")){
                    rs = dbmd.getTables(schema.toUpperCase(), null, tab_name, new String[] { "TABLE" });
                }else{
                    rs = dbmd.getTables(null, schema.toUpperCase(), tab_name.toUpperCase(), new String[] { "TABLE" });
                }
                if(rs.next()){
                    ps = conn.prepareStatement(sqlString);
                    ps.executeUpdate();
                }
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }finally {
            try {
                closeAll(conn,ps,rs,dbmd);
            }catch (Exception e1){
                log.error(e1.getMessage(),e1);
            }
        }
    }

    protected void closeAll(Connection conn, PreparedStatement ps, ResultSet rs, DatabaseMetaData dbmd) throws SQLException {
        if (dbmd != null) {
            dbmd = null;
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
