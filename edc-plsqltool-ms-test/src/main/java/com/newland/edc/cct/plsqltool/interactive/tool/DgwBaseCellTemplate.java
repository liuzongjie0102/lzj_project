package com.newland.edc.cct.plsqltool.interactive.tool;


import com.newland.edc.cct.plsqltool.interactive.model.javabean.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public abstract class DgwBaseCellTemplate extends DgwBaseTemplate {

    protected ResultSet rs = null;
    protected ResultSetMetaData rsmd = null;
    protected PreparedStatement ps = null;
    protected Connection conn = null;

    private static final Logger log = LoggerFactory.getLogger(DgwBaseCellTemplate.class);
    protected String sql;
    protected String resource_id;
    protected String conn_id;
    protected String execute_type;
    protected String count_sql;
    protected String db_type;
    protected String start_page;
    protected String page_num;
    protected String timeout;
    protected String table_name;

    @Override
    public DgwSqlToolResult run(ExecuteRequestBean bean) throws Exception{
        this.conn_id = bean.getConnId();
        this.resource_id = bean.getResourceId();
        this.start_page = bean.getStartPage();
        this.page_num = bean.getPageNum();
        this.timeout = bean.getConnTimeout();
        this.sql = bean.getSql();
        this.execute_type = bean.getExecuteType();
        this.db_type = bean.getDbType();
        this.table_name = bean.getTableName();
        if(execute_type==null||execute_type.equals("")){
            if(sql.toUpperCase().indexOf("INSERT")!=-1){
                execute_type = "INSERT";
            }else if(sql.toUpperCase().indexOf("CREATE")!=-1){
                execute_type = "CREATE";
            }else if(sql.toUpperCase().indexOf("DROP")!=-1){
                execute_type = "DROP";
            }else if(sql.toUpperCase().indexOf("DELETE")!=-1){
                execute_type = "DELETE";
            }else if(sql.toUpperCase().indexOf("UPDATE")>=-1){
                execute_type = "UPDATE";
            }else if(sql.toUpperCase().indexOf("SELECT")>=-1){
                execute_type = "SELECT";
            }else if(sql.toUpperCase().indexOf("TRUNCATE")>=-1){
                execute_type = "TRUNCATE";
            }else{
                execute_type = "QUERY";
            }
        }
        DgwSqlToolResult result = execute();
        if(result==null){
            result = new DgwSqlToolResult();
        }
        result.setSql(sql);
        return result;
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

    protected String getExecuteTime(Long begin,Long end){
        Long duration = (end-begin)/1000;//s
        StringBuffer sb = new StringBuffer();
        if((duration/3600)>0){
            sb.append((duration/3600)+"小时");
            duration = duration%3600;
        }else if((duration/60)>0){
            sb.append((duration/60)+"分钟");
            duration = duration%60;
        }else if(duration>0){
            sb.append(duration+"秒");
        }else if(sb.toString().equals("")){
            sb.append(end-begin+"毫秒");
        }
        return sb.toString();
    }


    @Override
    public DgwSqlToolResult commonDeal() throws Exception {
        DgwSqlToolResult result = new DgwSqlToolResult();
        long startTime = System.currentTimeMillis();    //获取开始时间
        if(execute_type.equals("CREATE")||execute_type.equals("DROP")||execute_type.equals("ALTER")||execute_type.equals("TRUNCATE")){
            ps = conn.prepareStatement(sql);
            String schema = conn.getSchema();
            ps.execute();
            if(execute_type.equals("CREATE")||execute_type.equals("ALTER")){
                result.setResult_col_list(new ArrayList<>());
                DatabaseMetaData dbmd = conn.getMetaData();
                rs = dbmd.getColumns(null, schema,table_name.toUpperCase(), "%");
                while(rs.next()){
                    DgwResultCol dgwResultCol = new DgwResultCol();
                    dgwResultCol.setCol_id(rs.getString("COLUMN_NAME"));//字段名
                    dgwResultCol.setCol_type(rs.getString("TYPE_NAME"));//字段类型
                    dgwResultCol.setCol_length(rs.getString("COLUMN_SIZE"));//字段长度
                    dgwResultCol.setCol_precise(rs.getString("DECIMAL_DIGITS"));//字段精度
                    result.getResult_col_list().add(dgwResultCol);
                }
            }
        }else if(execute_type.equals("UPDATE")||execute_type.equals("INSERT")||execute_type.equals("DELETE")){
            ps = conn.prepareStatement(sql);
            int num = ps.executeUpdate();
            result.setUpdateCount(num+"");
        }else if(execute_type.equals("SELECT")){
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            rsmd = rs.getMetaData();
            List<DgwResultCol> result_col_list = new ArrayList<>();
            List<DgwResultRowData> result_data_list = new ArrayList<>();
            int colnum = rsmd.getColumnCount();
            for(int i=1;i<=colnum;i++){
                String col_label = rsmd.getColumnLabel(i);
                DgwResultCol result_col = new DgwResultCol();
                result_col.setCol_id(col_label.replaceFirst("v_alias_12.",""));
                result_col_list.add(result_col);
            }
            while(rs.next()){
                List<DgwResultData> _data_list = new ArrayList<>();
                for (DgwResultCol result_col_bean : result_col_list) {
                    DgwResultData data = new DgwResultData();
                    Object obj = rs.getObject(result_col_bean.getCol_id());
                    if (obj != null) {
                        data.setData_val(String.valueOf(rs.getObject(result_col_bean.getCol_id())));
                    } else {
                        data.setData_val("");
                    }
                    _data_list.add(data);
                }
                DgwResultRowData row_data_bean = new DgwResultRowData();
                row_data_bean.setData_list(_data_list);
                result_data_list.add(row_data_bean);
            }
            result.setResult_col_list(result_col_list);
            result.setResult_data_list(result_data_list);
        }
        long endTime = System.currentTimeMillis();    //获取结束时间
        result.setTimeConsume(getExecuteTime(startTime,endTime));
        return result;
    }
}
