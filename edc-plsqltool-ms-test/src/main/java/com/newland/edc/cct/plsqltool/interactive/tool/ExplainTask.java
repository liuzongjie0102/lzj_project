package com.newland.edc.cct.plsqltool.interactive.tool;

import com.newland.bi.util.logger.BaseLogger;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.ExplainInfo;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.TestSqlBean;
import com.newland.edc.cct.plsqltool.interactive.util.DataSourceAccess;
import com.newland.edc.cct.plsqltool.interactive.util.KerberosTool;
import org.apache.hadoop.security.UserGroupInformation;

import java.security.PrivilegedExceptionAction;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ExplainTask {

    private static BaseLogger log = (BaseLogger) BaseLogger.getLogger(ExplainTask.class);

    protected Connection conn = null;
    protected PreparedStatement ps = null;
    protected ResultSet rs = null;
    protected String conn_id = null;
    protected String sqlString = null;

    protected List<TestSqlBean> testSqlBeans;

    public ExplainTask(List<TestSqlBean> testSqlBeans,String conn_id){
        this.testSqlBeans = testSqlBeans;
        this.conn_id = conn_id;
    }

    public List<ExplainInfo> executeExplain() throws Exception{
        List<ExplainInfo> explainInfos = new ArrayList<>();
        try {
            conn = DataSourceAccess.getConnByConnId(conn_id);
            UserGroupInformation ugi = KerberosTool.get(conn_id,testSqlBeans.get(0).getDbType());
            for (int i=0;i<testSqlBeans.size();i++){
                ExplainInfo explainInfo = new ExplainInfo();
                sqlString = "";
                if(testSqlBeans.get(i).getDbType().equalsIgnoreCase("hive")){
                    sqlString = "explain extended "+testSqlBeans.get(i).getReal_sql();
                    if(ugi!=null){
                        explainInfo = ugi.doAs( new PrivilegedExceptionAction<ExplainInfo>(){
                            @Override
                            public ExplainInfo run() throws Exception{
                                ExplainInfo explainInfo = new ExplainInfo();
                                ps = conn.prepareStatement(sqlString);
                                rs = ps.executeQuery();
                                StringBuffer explainContent = new StringBuffer();
                                while (rs.next()){
                                    explainContent.append(rs.getString(1));
                                    explainContent.append("\n");
                                }
                                explainInfo.setExplainInfo(explainContent.toString());
                                return explainInfo;
                            }
                        });
                    }else{
                        ps = conn.prepareStatement(sqlString);
                        rs = ps.executeQuery();
                        StringBuffer explainContent = new StringBuffer();
                        while (rs.next()){
                            explainContent.append(rs.getString(1));
                            explainContent.append("\n");
                        }
                        explainInfo.setExplainInfo(explainContent.toString());
                    }
                }else if(testSqlBeans.get(i).getDbType().equalsIgnoreCase("oracle")){
                    sqlString = "explain plan for "+testSqlBeans.get(i).getReal_sql();
                    try (PreparedStatement ps2 = conn.prepareStatement("select * from table(dbms_xplan.display())")){
                        ps = conn.prepareStatement(sqlString);
                        ps.execute();
                        rs = ps2.executeQuery();
                        StringBuffer explainContent = new StringBuffer();
                        while (rs.next()){
                            explainContent.append(rs.getString(1));
                            explainContent.append("\n");
                        }
                        explainInfo.setExplainInfo(explainContent.toString());
                    }catch (Exception e){
                        log.error(e.getMessage(),e);
                        throw e;
                    }
                }else if(testSqlBeans.get(i).getDbType().equalsIgnoreCase("db2")){

                }else if(testSqlBeans.get(i).getDbType().equalsIgnoreCase("greenplum")){
                    sqlString = "explain " + testSqlBeans.get(i).getReal_sql();
                    try {
                        ps = conn.prepareStatement(sqlString);
                        rs = ps.executeQuery();
                        StringBuffer explainContent = new StringBuffer();
                        while (rs.next()){
                            explainContent.append(rs.getString(1));
                            explainContent.append("\n");
                        }
                        explainInfo.setExplainInfo(explainContent.toString());
                    }catch (Exception e){
                        log.error(e.getMessage(),e);
                        throw e;
                    }
                }else if(testSqlBeans.get(i).getDbType().equalsIgnoreCase("mysql")||testSqlBeans.get(i).getDbType().equalsIgnoreCase("gbase")){
                    sqlString = "explain " + testSqlBeans.get(i).getReal_sql();
                    try {
                        ps = conn.prepareStatement(sqlString);
                        rs = ps.executeQuery();
                        ResultSetMetaData rsmd = rs.getMetaData();
                        int colnum = rsmd.getColumnCount();
                        List<List<String>> explainDetail = new ArrayList<>();
                        List<String> titleColumn = new ArrayList<>();
                        for (int m=1;m<=colnum;m++){
                            titleColumn.add(rsmd.getColumnLabel(m));
                        }
                        explainDetail.add(titleColumn);
                        while (rs.next()){
                            List<String> contentColumn = new ArrayList<>();
                            for (int n=0;n<colnum;n++){
                                Object obj = rs.getObject(titleColumn.get(n));
                                if(obj!=null){
                                    contentColumn.add(String.valueOf(rs.getObject(titleColumn.get(n))));
                                }else{
                                    contentColumn.add("");
                                }
                            }
                            explainDetail.add(contentColumn);
                        }
                        explainInfo.setExplainDetail(explainDetail);
                    }catch (Exception e){
                        log.error(e.getMessage(),e);
                        throw e;
                    }
                }
                explainInfo.setSqlString(testSqlBeans.get(i).getSql());
                explainInfo.setReal_sqlString(testSqlBeans.get(i).getReal_sql());
                explainInfo.setEnplain_sqlString(sqlString);
                explainInfos.add(explainInfo);
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }finally {
            closeAll(conn,ps,rs,null);
        }
        return explainInfos;
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
