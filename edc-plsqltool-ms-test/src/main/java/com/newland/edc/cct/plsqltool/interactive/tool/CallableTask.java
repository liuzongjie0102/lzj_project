package com.newland.edc.cct.plsqltool.interactive.tool;

import com.alibaba.fastjson.JSON;
import com.newland.bd.ms.core.model.RespInfo;
import com.newland.bd.ms.core.utils.SpringContextUtils;
import com.newland.bd.utils.commons.UUIDUtils;
import com.newland.bi.util.logger.BaseLogger;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.*;
import com.newland.edc.cct.plsqltool.interactive.service.PLSqlToolQueryService;
import com.newland.edc.cct.plsqltool.interactive.service.PLSqlToolTestService;
import com.newland.edc.cct.plsqltool.interactive.util.DataCatch;
import com.newland.edc.cct.plsqltool.interactive.util.DataSourceAccess;
import com.newland.edc.cct.plsqltool.interactive.util.KerberosTool;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.PrivilegedExceptionAction;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CallableTask implements Callable<Object> {
    private static BaseLogger log = (BaseLogger) BaseLogger.getLogger(CallableTask.class);

    private PLSqlToolQueryService plSqlToolQueryService =(PLSqlToolQueryService)SpringContextUtils.getBean("com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolQueryServiceImpl");
    private PLSqlToolTestService  plSqlToolTestService  =(PLSqlToolTestService)SpringContextUtils.getBean("com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolTestServiceImpl");

    private String              execute_group ="";
    protected ResultSet         rs = null;
    protected ResultSetMetaData rsmd = null;
    protected PreparedStatement ps = null;
    protected Connection        conn = null;
    protected List<TestSqlBean> testSqlBeans;

    private String                 conn_id;
    private String                 sql = "";
    private String                 schema = null;
    private String                 table_name = null;
    private List<DgwResultCol>     DQL_result_col_list;
    private List<DgwResultRowData> DQL_result_data_list;
    private String                 execute_type;
    private String                 key_col;

    public String getExecute_group() {
        return execute_group;
    }

    public void setExecute_group(String execute_group) {
        this.execute_group = execute_group;
    }

    public CallableTask(){
        super();
    }

    public CallableTask(List<TestSqlBean> testSqlBeans){
        this.execute_group = testSqlBeans.get(0).getGroup_id();
        this.conn_id = testSqlBeans.get(0).getConn_id();
        this.testSqlBeans = testSqlBeans;
        String jsonString = JSON.toJSONString(testSqlBeans);
        log.info("任务组："+this.execute_group+"连接id："+this.conn_id+"请求报文："+jsonString);
    }

    @Override
    public Object call() throws Exception{
        List<DgwSqlToolResult> results = new ArrayList<>();
        ExecuteLog executeLog = null;
        DgwSqlToolResult result = null;
        String log_id = "";
        try {
            conn = DataSourceAccess.getConnByConnId(conn_id);
            DataCatch.getConMap().put(execute_group,conn);//在缓存中注册任务连接
            log.info("conn_id="+conn_id);
            log.info("dbType=" + this.testSqlBeans.get(0).getDbType());

            UserGroupInformation ugi = KerberosTool.get(conn_id,this.testSqlBeans.get(0).getDbType());
            //多条语句执行
            for (int m= 0;m<this.testSqlBeans.size();m++){
                try {
                    log_id = this.testSqlBeans.get(m).getTask_id();
                    execute_type = "DQL";
                    key_col = "SELECT";
                    table_name = "";
                    for (int n=0;n<this.testSqlBeans.get(m).getTestSqlDetailBeans().size();n++){
                        if(!this.testSqlBeans.get(m).getTestSqlDetailBeans().get(n).getExecuteType().equalsIgnoreCase("DQL")&&execute_type.equalsIgnoreCase("DQL")){
                            execute_type = this.testSqlBeans.get(m).getTestSqlDetailBeans().get(n).getExecuteType();
                            key_col = this.testSqlBeans.get(m).getTestSqlDetailBeans().get(n).getKeyCol();
                            table_name = this.testSqlBeans.get(m).getTestSqlDetailBeans().get(n).getTableName();
                        }
                    }
                    result = new DgwSqlToolResult();
                    executeLog = new ExecuteLog();
                    executeLog.setId(this.testSqlBeans.get(m).getTask_id());
                    executeLog.setGroup_id(this.testSqlBeans.get(m).getGroup_id());
                    executeLog.setUser_id(this.testSqlBeans.get(m).getUser_id());
                    executeLog.setTenant_id(this.testSqlBeans.get(m).getTenant_id());
                    executeLog.setDb_type(this.testSqlBeans.get(m).getDbType());
                    executeLog.setExecute_type(execute_type);
                    executeLog.setKeycol(key_col);
                    executeLog.setRun_mode(this.testSqlBeans.get(m).getRun_mode());
                    executeLog.setSqlstring(this.testSqlBeans.get(m).getSql());
                    executeLog.setReal_sqlstring(this.testSqlBeans.get(m).getReal_sql());
                    executeLog.setResource_id(this.testSqlBeans.get(m).getResource_id());
                    executeLog.setConn_id(this.testSqlBeans.get(m).getConn_id());
                    executeLog.setIp(this.plSqlToolQueryService.getIp());
                    //根据数据库类型 添加分页查询
                    if(execute_type.equals("DQL")){
                        if(this.testSqlBeans.get(m).getDbType().equals("hive")){
                            //hive提取去数量限制1000条  没有字段信息无法用函数分页
                            sql = "SELECT 1 as row_num,* FROM ("+ this.testSqlBeans.get(m).getReal_sql() +") v_alias_12 limit 1000";
                        }else if(this.testSqlBeans.get(m).getDbType().equals("oracle")){
                            sql = "SELECT * FROM (SELECT rownum row_num,table_alias_1.* FROM ("+this.testSqlBeans.get(m).getReal_sql()+" ) table_alias_1 )WHERE row_num>=1 AND row_num<=1000";
                        }else if(this.testSqlBeans.get(m).getDbType().equals("db2")){
                            sql = "SELECT * FROM (SELECT rownumber() over() row_num,t1.* FROM ("+this.testSqlBeans.get(m).getReal_sql()+") t1 ) t2 WHERE t2.row_num <=1000";
                        }else if(this.testSqlBeans.get(m).getDbType().equals("greenplum")){
                            sql = "SELECT 1 as gp_rownum,t1.* FROM ("+this.testSqlBeans.get(m).getReal_sql()+") t1 limit 1000 offset 0";
                        }else if(this.testSqlBeans.get(m).getDbType().equals("mysql")){
                            sql = "SELECT 1 as mysql_rownum,t1.* FROM ("+this.testSqlBeans.get(m).getReal_sql()+") t1 limit 0,1000";
                        }else if(this.testSqlBeans.get(m).getDbType().equals("gbase")){
                            sql = "SELECT 1 as gbase_rownum,t1.* FROM ("+this.testSqlBeans.get(m).getReal_sql()+") t1 limit 0,1000";
                        }
                    }else{
                        sql = this.testSqlBeans.get(m).getReal_sql();
                    }
                    long startTime = System.currentTimeMillis();    //获取开始时间
                    //实例化ps
                    if(ugi!=null){
                        ugi.doAs( new PrivilegedExceptionAction<Void>(){
                            @Override
                            public Void run() throws Exception{
                                ps = conn.prepareStatement(sql);
                                return null;
                            }
                        });
                    }else{
                        ps = conn.prepareStatement(sql);
                    }
                    DataCatch.getPsMap().put(this.testSqlBeans.get(m).getTask_id(),ps);
                    //提取用户模式
                    if(this.testSqlBeans.get(m).getDbType().equalsIgnoreCase("hive")){
                        if(ugi!=null){
                            ugi.doAs( new PrivilegedExceptionAction<Void>(){
                                @Override
                                public Void run() throws Exception{
                                    schema = conn.getSchema();
                                    return null;
                                }
                            });
                        }else{
                            schema = conn.getSchema();
                        }
                    }else if(this.testSqlBeans.get(m).getDbType().equalsIgnoreCase("oracle")){
                        schema = conn.getMetaData().getUserName();
                        if(key_col.equalsIgnoreCase("DROP_INDEX")){
                            //根据索引名换取表名
                            table_name = queryTableByIndexNameOracle(table_name);
                            if(table_name.equals("")){
                                throw new Exception("执行sql语句："+sql+"，因无法获取索引对应的表，无法执行");
                            }
                        }
                    }else if(this.testSqlBeans.get(m).getDbType().equalsIgnoreCase("db2")){
                        schema = conn.getMetaData().getUserName();
                        if(key_col.equalsIgnoreCase("DROP_INDEX")){
                            //根据索引名换取表名
                            table_name = queryTableByIndexNameDB2(table_name,schema);
                            if(table_name.equals("")){
                                throw new Exception("执行sql语句："+sql+"，因无法获取索引对应的表，无法执行");
                            }
                        }
                    }else if(this.testSqlBeans.get(m).getDbType().equalsIgnoreCase("greenplum")){
                        schema = conn.getMetaData().getUserName();
                        if(key_col.equalsIgnoreCase("DROP_INDEX")){
                            //根据索引名换取表名
                            table_name = queryTableByIndexNameGreenplum(table_name,schema);
                            if(table_name.equals("")){
                                throw new Exception("执行sql语句："+sql+"，因无法获取索引对应的表，无法执行");
                            }
                        }
                    }else if(this.testSqlBeans.get(m).getDbType().equalsIgnoreCase("mysql")||this.testSqlBeans.get(m).getDbType().equalsIgnoreCase("gbase")){
                        String[] dbNames = conn.getMetaData().getUserName().split("@");
                        schema = dbNames[0];
                        //msql删除索引时无需跟换tablename
//                        if(key_col.equalsIgnoreCase("DROP_INDEX")){
//                            //根据索引名换取表名
//                            table_name = queryTableByIndexNameMysql(table_name,schema);
//                            if(table_name.equals("")){
//                                throw new Exception("执行sql语句："+sql+"，因无法获取索引对应的表，无法执行");
//                            }
//                        }
                    }
                    List<DgwResultCol> dgwResultCols;
                    DatabaseMetaData dbmd = null;
                    if(execute_type.equals("DDL")){
                        if(ugi!=null) {
                            dgwResultCols = ugi.doAs(new PrivilegedExceptionAction<List<DgwResultCol>>() {
                                @Override
                                public List<DgwResultCol> run() throws Exception {
                                    ps.execute();
                                    List<DgwResultCol> dgwResultCols = new ArrayList<>();
                                    DatabaseMetaData dbmd = conn.getMetaData();
                                    rs = dbmd.getColumns(null, schema.toUpperCase(), table_name.toUpperCase(), "%");
                                    log.info("=====================执行查询字段详情");
                                    while (rs.next()) {
                                        log.info("=====================字段名："+rs.getString("COLUMN_NAME")+" 字段类型："+rs.getString("TYPE_NAME")+" 字段长度："+rs.getString("COLUMN_SIZE")+" 字段精度："+rs.getString("DECIMAL_DIGITS"));
                                        DgwResultCol dgwResultCol = new DgwResultCol();
                                        dgwResultCol.setCol_id(rs.getString("COLUMN_NAME"));//字段名
                                        dgwResultCol.setCol_type(rs.getString("TYPE_NAME"));//字段类型
                                        dgwResultCol.setCol_length(rs.getString("COLUMN_SIZE"));//字段长度
                                        dgwResultCol.setCol_precise(rs.getString("DECIMAL_DIGITS"));//字段精度
                                        dgwResultCol.setCol_desc(rs.getString("REMARKS"));//字段描述
                                        dgwResultCols.add(dgwResultCol);
                                    }
                                    return dgwResultCols;
                                }
                            });
                        }else{
                            ps.execute();
                            dgwResultCols = new ArrayList<>();
                            dbmd = conn.getMetaData();
                            if(this.testSqlBeans.get(m).getDbType().equalsIgnoreCase("greenplum")){
                                Map<String,String> colTrans = new HashMap<>();
                                colTrans.put("CHARACTER VARYING","VARCHAR");
                                colTrans.put("INT2","SMALLINT");
                                colTrans.put("INT4","INTEGER");
                                colTrans.put("INT8","BIGINT");
                                colTrans.put("TIMESTAMP WITHOUT TIME ZONE","TIMESTAMP");
                                colTrans.put("TIME WITHOUT TIME ZONE","TIME");
                                rs = dbmd.getColumns(null, null, table_name.toLowerCase(), "%");
                                while (rs.next()) {
                                    DgwResultCol dgwResultCol = new DgwResultCol();
                                    dgwResultCol.setCol_id(rs.getString("COLUMN_NAME"));//字段名
                                    if(StringUtils.isNotBlank(colTrans.get(rs.getString("TYPE_NAME")))){
                                        dgwResultCol.setCol_type(colTrans.get(rs.getString("TYPE_NAME")));
                                    }else{
                                        dgwResultCol.setCol_type(rs.getString("TYPE_NAME"));//字段类型
                                    }
                                    if(dgwResultCol.getCol_type().indexOf("INT")!=-1){
                                        dgwResultCol.setCol_length("");//字段长度
                                    }else{
                                        dgwResultCol.setCol_length(rs.getString("COLUMN_SIZE"));//字段长度
                                    }
                                    dgwResultCol.setCol_precise(rs.getString("DECIMAL_DIGITS"));//字段精度
                                    dgwResultCol.setCol_desc(rs.getString("REMARKS"));//字段描述
                                    dgwResultCols.add(dgwResultCol);
                                }
                            }else if(this.testSqlBeans.get(m).getDbType().equalsIgnoreCase("db2")){
                                String sql = "select colname as COLUMN_NAME,typename as TYPE_NAME,length as COLUMN_SIZE,scale as DECIMAL_DIGITS,remarks as REMARKS from syscat.columns where tabschema=upper('"+schema+"') and tabname=upper('"+table_name+"') ";
                                PreparedStatement ps1 = conn.prepareStatement(sql);
                                rs = ps1.executeQuery();
                                while(rs.next()){
                                    DgwResultCol dgwResultCol = new DgwResultCol();
                                    dgwResultCol.setCol_id(rs.getString("COLUMN_NAME"));//字段名
                                    dgwResultCol.setCol_type(rs.getString("TYPE_NAME"));//字段类型
                                    dgwResultCol.setCol_length(rs.getString("COLUMN_SIZE"));//字段长度
                                    dgwResultCol.setCol_precise(rs.getString("DECIMAL_DIGITS"));//字段精度
                                    dgwResultCol.setCol_desc(rs.getString("REMARKS"));//字段描述
                                    dgwResultCols.add(dgwResultCol);
                                }
                                if(ps1!=null){
                                    ps1.close();
                                }
                            }else if(this.testSqlBeans.get(m).getDbType().equalsIgnoreCase("mysql")||this.testSqlBeans.get(m).getDbType().equalsIgnoreCase("gbase")){
                                rs = dbmd.getColumns(schema.toUpperCase(), null, table_name.toUpperCase(), "%");
                                while (rs.next()) {
                                    DgwResultCol dgwResultCol = new DgwResultCol();
                                    dgwResultCol.setCol_id(rs.getString("COLUMN_NAME"));//字段名
                                    dgwResultCol.setCol_type(rs.getString("TYPE_NAME"));//字段类型
                                    dgwResultCol.setCol_length(rs.getString("COLUMN_SIZE"));//字段长度
                                    dgwResultCol.setCol_precise(rs.getString("DECIMAL_DIGITS"));//字段精度
                                    dgwResultCol.setCol_desc(rs.getString("REMARKS"));//字段描述
                                    dgwResultCols.add(dgwResultCol);
                                }
                            }else{//oracle hive
                                rs = dbmd.getColumns(null, schema.toUpperCase(), table_name.toUpperCase(), "%");
                                while (rs.next()) {
                                    DgwResultCol dgwResultCol = new DgwResultCol();
                                    dgwResultCol.setCol_id(rs.getString("COLUMN_NAME"));//字段名
                                    dgwResultCol.setCol_type(rs.getString("TYPE_NAME"));//字段类型
                                    dgwResultCol.setCol_length(rs.getString("COLUMN_SIZE"));//字段长度
                                    dgwResultCol.setCol_precise(rs.getString("DECIMAL_DIGITS"));//字段精度
                                    dgwResultCol.setCol_desc(rs.getString("REMARKS"));//字段描述
                                    dgwResultCols.add(dgwResultCol);
                                }
                            }
                        }
                        result.setResult_col_list(dgwResultCols);
                    }else if(execute_type.equals("DML")){
                        int num = 0;
                        if(ugi!=null){
                            num = ugi.doAs( new PrivilegedExceptionAction<Integer>(){
                                @Override
                                public Integer run() throws Exception{
                                    int num = ps.executeUpdate();
                                    return num;
                                }
                            });
                        }else{
                            num = ps.executeUpdate();
                        }
                        result.setUpdateCount(num + "");
                    }else if(execute_type.equals("DQL")){
                        DQL_result_col_list = new ArrayList<>();
                        DQL_result_data_list = new ArrayList<>();
                        if(ugi!=null){
                            ugi.doAs( new PrivilegedExceptionAction<Void>(){
                                @Override
                                public Void run() throws Exception{
                                    rs = ps.executeQuery();
                                    rsmd = rs.getMetaData();
                                    int colnum = rsmd.getColumnCount();
                                    for(int i=2;i<=colnum;i++){
                                        String col_label = rsmd.getColumnLabel(i);
                                        DgwResultCol result_col = new DgwResultCol();
                                        result_col.setCol_id(col_label.replaceFirst("v_alias_12.",""));
                                        DQL_result_col_list.add(result_col);
                                    }
                                    while(rs.next()){
                                        List<DgwResultData> _data_list = new ArrayList<>();
                                        for (DgwResultCol result_col_bean : DQL_result_col_list) {
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
                                        DQL_result_data_list.add(row_data_bean);
                                    }
                                    return null;
                                }
                            });
                        }else{
                            rs = ps.executeQuery();
                            rsmd = rs.getMetaData();
                            int colnum = rsmd.getColumnCount();
                            for(int i=2;i<=colnum;i++){
                                String col_label = rsmd.getColumnLabel(i);
                                DgwResultCol result_col = new DgwResultCol();
                                if(this.testSqlBeans.get(m).getDbType().equals("hive")){
                                    result_col.setCol_id(col_label.replaceFirst("v_alias_12.",""));
                                }else{
                                    result_col.setCol_id(col_label);
                                }
                                DQL_result_col_list.add(result_col);
                            }
                            while(rs.next()){
                                List<DgwResultData> _data_list = new ArrayList<>();
                                for (DgwResultCol result_col_bean : DQL_result_col_list) {
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
                                DQL_result_data_list.add(row_data_bean);
                            }
                        }
                        result.setResult_col_list(DQL_result_col_list);
                        result.setResult_data_list(DQL_result_data_list);
                    }
                    //维护临时表信息
                    if(key_col.equals("ALTER_RENAME_FROMTABLE")||key_col.equals("ALTER_RENAME_TOTABLE")){
                        String fromTable = "";
                        String toTable = "";
                        for (int n=0;n<this.testSqlBeans.get(m).getTestSqlDetailBeans().size();n++){
                            if(this.testSqlBeans.get(m).getTestSqlDetailBeans().get(n).getKeyCol().equals("ALTER_RENAME_FROMTABLE")){
                                fromTable = this.testSqlBeans.get(m).getTestSqlDetailBeans().get(n).getTableName();
                            }
                            if(this.testSqlBeans.get(m).getTestSqlDetailBeans().get(n).getKeyCol().equals("ALTER_RENAME_TOTABLE")){
                                toTable = this.testSqlBeans.get(m).getTestSqlDetailBeans().get(n).getTableName();
                            }
                        }
                        if((fromTable!=null&&!fromTable.equals(""))&&(toTable!=null&&!toTable.equals(""))){
                            this.plSqlToolQueryService.updateTemporaryTableName(this.testSqlBeans.get(m).getTenant_id(),this.testSqlBeans.get(m).getUser_id(),this.testSqlBeans.get(m).getResource_id(),this.testSqlBeans.get(m).getConn_id(),fromTable,toTable);
                        }
                    }else if (key_col.equals("CREATE_TABLE") || key_col.equals("CREATE_VIEW")
                                    ||key_col.equals("ALTER")||key_col.equals("ALTER_ADD_COLUMN")||key_col.equalsIgnoreCase("ALTER_DROP_COLUMN")
                                    ||key_col.equalsIgnoreCase("ALTER_MODIFY_COLUMN")||key_col.equalsIgnoreCase("ALTER_MODIFY_COLUMNTYPE")
                                    ||key_col.equalsIgnoreCase("CREATE_INDEX")||key_col.equalsIgnoreCase("DROP_INDEX")) {

                        if(key_col.equalsIgnoreCase("CREATE_TABLE")||key_col.equalsIgnoreCase("CREATE_VIEW")){
                            TemporaryTabInfo temporaryTabInfo = new TemporaryTabInfo();
                            String tab_id = UUIDUtils.getUUID();
                            temporaryTabInfo.setTab_id(tab_id);
                            temporaryTabInfo.setUser_id(this.testSqlBeans.get(m).getUser_id());
                            temporaryTabInfo.setTab_name(table_name);
                            //表中文名

                            temporaryTabInfo.setDb_type(this.testSqlBeans.get(m).getDbType());
                            temporaryTabInfo.setConn_id(this.testSqlBeans.get(m).getConn_id());
                            temporaryTabInfo.setTenant_id(this.testSqlBeans.get(m).getTenant_id());
                            temporaryTabInfo.setResource_id(this.testSqlBeans.get(m).getResource_id());
                            if(!result.getResult_col_list().isEmpty()){
                                temporaryTabInfo.setColInfos(new ArrayList<>());
                                for (int i=0;i<result.getResult_col_list().size();i++){
                                    TemporaryColInfo temporaryColInfo = new TemporaryColInfo();
                                    temporaryColInfo.setTab_id(tab_id);
                                    temporaryColInfo.setCol_id(result.getResult_col_list().get(i).getCol_id());
                                    temporaryColInfo.setCol_name(result.getResult_col_list().get(i).getCol_id());
                                    temporaryColInfo.setCol_chs_name(result.getResult_col_list().get(i).getCol_desc());
                                    temporaryColInfo.setCol_type(result.getResult_col_list().get(i).getCol_type());
                                    if(this.testSqlBeans.get(m).getDbType().equals("hive")){
                                        if(!result.getResult_col_list().get(i).getCol_type().equalsIgnoreCase("STRING")&&result.getResult_col_list().get(i).getCol_type().toUpperCase().indexOf("INT")==-1){
                                            temporaryColInfo.setCol_length(result.getResult_col_list().get(i).getCol_length());
                                        }else{
                                            temporaryColInfo.setCol_length("");
                                        }
                                    }else{
                                        if(StringUtils.isBlank(result.getResult_col_list().get(i).getCol_length())||result.getResult_col_list().get(i).getCol_length().equals("0")){
                                            temporaryColInfo.setCol_length("");
                                        }else{
                                            temporaryColInfo.setCol_length(result.getResult_col_list().get(i).getCol_length());
                                        }
                                    }
                                    if(StringUtils.isNotBlank(result.getResult_col_list().get(i).getCol_precise())&&Integer.parseInt(result.getResult_col_list().get(i).getCol_precise())>0){
                                        temporaryColInfo.setCol_precise(result.getResult_col_list().get(i).getCol_precise());
                                    }
                                    temporaryTabInfo.getColInfos().add(temporaryColInfo);
                                }
                                //hive分区为虚拟字段  需要单独检索
                                if(this.testSqlBeans.get(m).getDbType().equalsIgnoreCase("HIVE")){
                                    this.plSqlToolTestService.getPartitionCol(sql,temporaryTabInfo);
                                }
                                //oracle添加主键 分区 索引
                                if(this.testSqlBeans.get(m).getDbType().equalsIgnoreCase("ORACLE")){
                                    try {
                                        dealOracleColumnInfo(temporaryTabInfo,table_name);
                                    }catch (Exception e11){
                                        log.error(e11.getMessage(),e11);
                                    }
                                }
                                //db2添加主键 分区 索引
                                if(this.testSqlBeans.get(m).getDbType().equalsIgnoreCase("DB2")){
                                    try {
                                        dealDB2ColumnInfo(temporaryTabInfo,table_name,schema);
                                    }catch (Exception e11){
                                        log.error(e11.getMessage(),e11);
                                    }
                                }
                                //greenplum添加主键 分区 索引
                                if(this.testSqlBeans.get(m).getDbType().equalsIgnoreCase("GREENPLUM")){
                                    try {
                                        dealGreenplumColumnInfo(temporaryTabInfo,table_name,schema);
                                    }catch (Exception e11){
                                        log.error(e11.getMessage(),e11);
                                    }
                                }
                                //mysql gbase 添加主键 分区 索引
                                if(this.testSqlBeans.get(m).getDbType().equalsIgnoreCase("MYSQL")||this.testSqlBeans.get(m).getDbType().equalsIgnoreCase("GBASE")){
                                    try {
                                        dealMysqlColumnInfo(temporaryTabInfo,table_name,schema);
                                    }catch (Exception e11){
                                        log.error(e11.getMessage(),e11);
                                    }
                                }
                            }
                            plSqlToolQueryService.insertTemporaryInfo(temporaryTabInfo);
                        }else if(key_col.equalsIgnoreCase("ALTER_ADD_COLUMN")||key_col.equalsIgnoreCase("ALTER_DROP_COLUMN")
                                        ||key_col.equalsIgnoreCase("ALTER_MODIFY_COLUMN")||key_col.equalsIgnoreCase("ALTER_MODIFY_COLUMNTYPE")
                                        ||key_col.equalsIgnoreCase("CREATE_INDEX")||key_col.equalsIgnoreCase("DROP_INDEX")||key_col.equalsIgnoreCase("ALTER")){
                            TemporaryTabInfo temporaryTabInfo = new TemporaryTabInfo();
                            temporaryTabInfo.setTab_name(table_name);
                            temporaryTabInfo.setDb_type(this.testSqlBeans.get(m).getDbType());
                            temporaryTabInfo.setConn_id(this.testSqlBeans.get(m).getConn_id());
                            List<TemporaryTabInfo> temporaryTabInfos = plSqlToolQueryService.getTemporaryInfo(temporaryTabInfo);
                            if(!temporaryTabInfos.isEmpty()){
                                for (int i=0;i<temporaryTabInfos.size();i++){
                                    if(!result.getResult_col_list().isEmpty()){
                                        plSqlToolQueryService.deleteTemporaryCol(temporaryTabInfos.get(i).getTab_id());
                                        List<TemporaryColInfo> colInfos = new ArrayList<>();
                                        String update_table_name = temporaryTabInfos.get(i).getTab_name();
                                        for (int j=0;j<result.getResult_col_list().size();j++){
                                            TemporaryColInfo temporaryColInfo = new TemporaryColInfo();
                                            temporaryColInfo.setTab_id(temporaryTabInfos.get(i).getTab_id());
                                            temporaryColInfo.setCol_id(result.getResult_col_list().get(j).getCol_id());
                                            temporaryColInfo.setCol_name(result.getResult_col_list().get(j).getCol_id());
                                            temporaryColInfo.setCol_chs_name(result.getResult_col_list().get(j).getCol_desc());
                                            temporaryColInfo.setCol_type(result.getResult_col_list().get(j).getCol_type());
                                            if(this.testSqlBeans.get(m).getDbType().equals("hive")){
                                                if(!result.getResult_col_list().get(j).getCol_type().equalsIgnoreCase("STRING")&&result.getResult_col_list().get(i).getCol_type().toUpperCase().indexOf("INT")==-1){
                                                    temporaryColInfo.setCol_length(result.getResult_col_list().get(j).getCol_length());
                                                }else{
                                                    temporaryColInfo.setCol_length("");
                                                }
                                            }else{
                                                if(StringUtils.isBlank(result.getResult_col_list().get(i).getCol_length())||result.getResult_col_list().get(i).getCol_length().equals("0")){
                                                    temporaryColInfo.setCol_length("");
                                                }else{
                                                    temporaryColInfo.setCol_length(result.getResult_col_list().get(j).getCol_length());
                                                }
                                            }
                                            if(result.getResult_col_list().get(j).getCol_precise()!=null&&!result.getResult_col_list().get(j).getCol_precise().equals("")&&Integer.parseInt(result.getResult_col_list().get(j).getCol_precise())>0){
                                                temporaryColInfo.setCol_precise(result.getResult_col_list().get(j).getCol_precise());
                                            }
                                            if(this.testSqlBeans.get(m).getDbType().equals("hive")){
                                                if(temporaryTabInfos.get(i).getColInfos().get(j)!=null){
                                                    if(temporaryTabInfos.get(i).getColInfos().get(j).getIs_partition()!=null&&!temporaryTabInfos.get(i).getColInfos().get(j).getIs_partition().equals("")){
                                                        temporaryColInfo.setIs_partition(temporaryTabInfos.get(i).getColInfos().get(j).getIs_partition());
                                                    }
                                                    if(temporaryTabInfos.get(i).getColInfos().get(j).getIs_index()!=null&&!temporaryTabInfos.get(i).getColInfos().get(j).getIs_index().equals("")){
                                                        temporaryColInfo.setIs_index(temporaryTabInfos.get(i).getColInfos().get(j).getIs_index());
                                                    }
                                                    if(temporaryTabInfos.get(i).getColInfos().get(j).getIs_key()!=null&&!temporaryTabInfos.get(i).getColInfos().get(j).getIs_key().equals("")){
                                                        temporaryColInfo.setIs_key(temporaryTabInfos.get(i).getColInfos().get(j).getIs_key());
                                                    }
                                                }
                                            }
                                            colInfos.add(temporaryColInfo);
                                        }
                                        //oracle添加主键 分区 索引
                                        if(this.testSqlBeans.get(m).getDbType().equals("oracle")){
                                            temporaryTabInfo.setColInfos(colInfos);
                                            dealOracleColumnInfo(temporaryTabInfo,update_table_name);
                                        }
                                        //db2添加主键 分区 索引
                                        if(this.testSqlBeans.get(m).getDbType().equals("db2")){
                                            temporaryTabInfo.setColInfos(colInfos);
                                            dealDB2ColumnInfo(temporaryTabInfo,update_table_name,schema);
                                        }
                                        //greenplum添加主键 分区 索引
                                        if(this.testSqlBeans.get(m).getDbType().equals("greenplum")){
                                            temporaryTabInfo.setColInfos(colInfos);
                                            dealGreenplumColumnInfo(temporaryTabInfo,update_table_name,schema);
                                        }
                                        //mysql gbase 添加主键 分区 索引
                                        if(this.testSqlBeans.get(m).getDbType().equals("mysql")||this.testSqlBeans.get(m).getDbType().equals("gbase")){
                                            temporaryTabInfo.setColInfos(colInfos);
                                            dealMysqlColumnInfo(temporaryTabInfo,update_table_name,schema);
                                        }
                                        for (int g=0;g<colInfos.size();g++){
                                            colInfos.get(g).setOrder_id(g+1);
                                            plSqlToolQueryService.insertTemporaryCol(colInfos.get(g));
                                        }
                                    }
                                }
                            }
                        }
                    }else if (key_col.equalsIgnoreCase("DROP_TABLE")||key_col.equalsIgnoreCase("DROP_VIEW")){
                        TemporaryTabInfo temporaryTabInfo = new TemporaryTabInfo();
                        temporaryTabInfo.setTab_name(table_name);
                        temporaryTabInfo.setDb_type(this.testSqlBeans.get(m).getDbType());
                        temporaryTabInfo.setConn_id(this.testSqlBeans.get(m).getConn_id());
                        temporaryTabInfo.setResource_id(this.testSqlBeans.get(m).getResource_id());
                        temporaryTabInfo.setTenant_id(this.testSqlBeans.get(m).getTenant_id());
                        List<TemporaryTabInfo> temporaryTabInfos = plSqlToolQueryService.getTemporaryInfo(temporaryTabInfo);
                        if(!temporaryTabInfos.isEmpty()){
                            for (int i=0;i<temporaryTabInfos.size();i++){
                                plSqlToolQueryService.deleteTemporaryCol(temporaryTabInfos.get(i).getTab_id());
                                plSqlToolQueryService.deleteTemporaryTab(temporaryTabInfos.get(i));
                            }
                        }
                    }
                    long endTime = System.currentTimeMillis();    //获取结束时间
                    result.setTimeConsume(getExecuteTime(startTime,endTime));
                    result.setSql(this.testSqlBeans.get(m).getSql());
                    result.setExecuteType(execute_type);
                    // 更新数据库数据
                    executeLog.setUtime(result.getTimeConsume());
                    this.plSqlToolQueryService.updateExecuteLog(executeLog,"2");
                    //保存结果
                    ExecuteResult executeResult = new ExecuteResult();
                    executeResult.setTask_id(this.testSqlBeans.get(m).getTask_id());
                    RespInfo respInfo = new RespInfo();
                    respInfo.setRespResult("1");
                    respInfo.setRespData(result);
                    String jsonstr = JSON.toJSONString(respInfo);
                    executeResult.setResult(jsonstr);
                    this.plSqlToolQueryService.insertExecuteResult(executeResult);
                    results.add(result);
                }catch (Exception e1){
                    log.error(e1.getMessage(),e1);
                    log.error("执行任务组："+this.execute_group+"任务id："+log_id+"出现错误，错误为"+e1.getMessage(),e1);
                    result = new DgwSqlToolResult();
                    result.setIs_success(false);
                    if(StringUtils.isBlank(e1.getMessage())){
                        log.info("=============捕获异常信息为空，截取异常堆栈："+getStackTrace(e1));
                        result.setErr_msg(getStackTrace(e1));
                    }else{
                        result.setErr_msg(e1.getMessage());
                    }
                    RespInfo respInfo = new RespInfo();
                    respInfo.setRespResult("1");
                    respInfo.setRespData(result);
                    String jsonstr = JSON.toJSONString(respInfo);
                    ExecuteErrorInfo executeErrorInfo = new ExecuteErrorInfo();
                    executeErrorInfo.setLog_id(log_id);
                    executeErrorInfo.setError_info(jsonstr);
                    executeErrorInfo.setError_stacktrace(ExceptionUtils.getStackTrace(e1));
                    this.plSqlToolQueryService.updateExecuteLog(executeLog,"-1");
                    this.plSqlToolQueryService.insertErrorInfo(executeErrorInfo);
                    results.add(result);
                }finally {
                    closeAll(null,ps,rs,rsmd);
                }
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
            String errorMsg = "";
            if(StringUtils.isBlank(e.getMessage())){
                log.info("=============捕获异常信息为空，截取异常堆栈："+getStackTrace(e));
                errorMsg = getStackTrace(e);
            }else{
                errorMsg = e.getMessage();
            }
            for (int i=0;i<this.testSqlBeans.size();i++){
                if(i==0){
                    ExecuteErrorInfo executeErrorInfo = new ExecuteErrorInfo();
                    executeErrorInfo.setLog_id(this.testSqlBeans.get(i).getTask_id());
                    result = new DgwSqlToolResult();
                    result.setIs_success(false);
                    result.setErr_msg(errorMsg);
                    RespInfo respInfo = new RespInfo();
                    respInfo.setRespResult("1");
                    respInfo.setRespData(result);
                    String jsonstr = JSON.toJSONString(respInfo);
                    executeErrorInfo.setError_info(jsonstr);
                    executeErrorInfo.setError_stacktrace(ExceptionUtils.getStackTrace(e));
                    this.plSqlToolQueryService.insertErrorInfo(executeErrorInfo);
                }
                ExecuteLog executeLog1 = new ExecuteLog();
                executeLog1.setId(this.testSqlBeans.get(i).getTask_id());
                this.plSqlToolQueryService.updateExecuteLog(executeLog1,"-1");
            }
            result = new DgwSqlToolResult();
            result.setIs_success(false);
            result.setErr_msg(errorMsg);
            results.add(result);
        }finally {
            closeAll(conn,ps,rs,rsmd);
        }

        return results;
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

    public static String getStackTrace(Throwable t){
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        String stackMsg ;
        try{
            t.printStackTrace(pw);
            stackMsg = sw.toString();
            int casedInt = stackMsg.lastIndexOf("Caused by:");
            if (casedInt != -1){
                stackMsg = stackMsg.substring(casedInt, stackMsg.length());
                if (stackMsg.split("at").length > 15){
                    Pattern p = Pattern.compile("at");
                    Matcher m = p.matcher(stackMsg);
                    int num = 0, index = -1;
                    while(m.find()){
                        num++;
                        if(15 == num){
                            index = m.start();
                            System.out.println(m.start());
                            stackMsg = "</br>"+stackMsg.substring(0, index);
                            break;
                        }
                    }
                }
            }
            return stackMsg;
        }catch (Exception e){
            log.error(e.getMessage(), e);
        }finally{
            pw.close();
        }

        return sw.toString();
    }

    protected void dealOracleColumnInfo(TemporaryTabInfo temporaryTabInfo,String table_name) throws Exception{
        List<String> keyColumns = new ArrayList<>();
        List<String> parColumns = new ArrayList<>();
        List<String> indColumns = new ArrayList<>();
        PreparedStatement createTablePs = null;
        ResultSet createTableRs = null;
        try {
            String keySql = "SELECT col.column_name FROM user_constraints con,user_cons_columns col where con.constraint_name=col.constraint_name and con.constraint_type='P' and col.table_name='"+table_name.toUpperCase()+"'";
            log.info("oracle查询"+table_name+"建表主键，sql："+keySql);
            createTablePs = conn.prepareStatement(keySql);
            createTableRs = createTablePs.executeQuery();
            while (createTableRs.next()){
                if(StringUtils.isNotBlank(createTableRs.getString("COLUMN_NAME"))){
                    keyColumns.add(createTableRs.getString("COLUMN_NAME"));
                }
            }
        }catch (Exception keyExecption){
            log.info("查询主键信息失败");
            log.error(keyExecption.getMessage(),keyExecption);
        }finally {
            closeAll(null,createTablePs,createTableRs,null);
        }
        try {
            String parSql = "SELECT column_name FROM USER_PART_KEY_COLUMNS t where name = '"+table_name.toUpperCase()+"'";
            log.info("oracle查询"+table_name+"建表分区，sql："+parSql);
            createTablePs = conn.prepareStatement(parSql);
            createTableRs = createTablePs.executeQuery();
            while(createTableRs.next()){
                if(StringUtils.isNotBlank(createTableRs.getString("COLUMN_NAME"))){
                    parColumns.add(createTableRs.getString("COLUMN_NAME"));
                }
            }
        }catch (Exception parExecption){
            log.info("查询分区信息失败");
            log.error(parExecption.getMessage(),parExecption);
        }finally {
            closeAll(null,createTablePs,createTableRs,null);
        }
        try {
            String indSql = "select column_name from user_ind_columns where table_name='"+table_name.toUpperCase()+"'";
            log.info("oracle查询"+table_name+"建表索引，sql："+indSql);
            createTablePs = conn.prepareStatement(indSql);
            createTableRs = createTablePs.executeQuery();
            while (createTableRs.next()){
                if(StringUtils.isNotBlank(createTableRs.getString("COLUMN_NAME"))){
                    indColumns.add(createTableRs.getString("COLUMN_NAME"));
                }
            }
        }catch (Exception indExecption){
            log.info("查询索引信息失败");
            log.error(indExecption.getMessage(),indExecption);
        }finally {
            closeAll(null,createTablePs,createTableRs,null);
        }
        if(keyColumns.size()>0){
            for (int i=0;i<keyColumns.size();i++){
                for (int j=0;j<temporaryTabInfo.getColInfos().size();j++){
                    if(keyColumns.get(i).equalsIgnoreCase(temporaryTabInfo.getColInfos().get(j).getCol_id())){
                        temporaryTabInfo.getColInfos().get(j).setIs_key("1");
                        break;
                    }
                }
            }
        }
        if(parColumns.size()>0){
            for (int i=0;i<parColumns.size();i++){
                for (int j=0;j<temporaryTabInfo.getColInfos().size();j++){
                    if(parColumns.get(i).equalsIgnoreCase(temporaryTabInfo.getColInfos().get(j).getCol_id())){
                        temporaryTabInfo.getColInfos().get(j).setIs_partition("1");
                    }
                }
            }
        }
        if(indColumns.size()>0){
            for (int i=0;i<indColumns.size();i++){
                for (int j=0;j<temporaryTabInfo.getColInfos().size();j++){
                    if(indColumns.get(i).equalsIgnoreCase(temporaryTabInfo.getColInfos().get(j).getCol_id())){
                        temporaryTabInfo.getColInfos().get(j).setIs_index("1");
                    }
                }
            }
        }
    }

    public String queryTableByIndexNameOracle(String table_name) throws Exception{
        PreparedStatement index_ps = null;
        ResultSet index_rs = null;
        try {
            //约定drop_index语句中将index_name放在table_name中
            String index_name = table_name;
            String sql = "select distinct table_name from user_ind_columns where index_name = upper('"+index_name+"')";
            log.info("执行语句："+sql);
            index_ps = conn.prepareStatement(sql);
            index_rs = index_ps.executeQuery();
            table_name = "";
            while(index_rs.next()){
                //用索引名换取对应执行表
                table_name = index_rs.getString("TABLE_NAME");
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
        }finally {
            closeAll(null,index_ps,index_rs,null);
        }
        return table_name;
    }

    public String queryTableByIndexNameDB2(String table_name,String tabschema) throws Exception{
        PreparedStatement index_ps = null;
        ResultSet index_rs = null;
        try {
            //约定drop_index语句中将index_name放在table_name中
            String index_name = table_name;
            String sql = "select TABNAME from syscat.indexes where indname='"+index_name.toUpperCase()+"' and owner='"+tabschema.toUpperCase()+"'";
            log.info("执行语句："+sql);
            index_ps = conn.prepareStatement(sql);
            index_rs = index_ps.executeQuery();
            table_name = "";
            while(index_rs.next()){
                //用索引名换取对应执行表
                table_name = index_rs.getString("TABNAME");
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
        }finally {
            closeAll(null,index_ps,index_rs,null);
        }
        return table_name;
    }

    protected void dealDB2ColumnInfo(TemporaryTabInfo temporaryTabInfo,String table_name,String tabschema) throws Exception{
        List<String> keyColumns = new ArrayList<>();
        List<String> parColumns = new ArrayList<>();
        List<String> indColumns = new ArrayList<>();
        PreparedStatement createTablePs = null;
        ResultSet createTableRs = null;
        try {
            String keySql = "select COLNAME from syscat.keycoluse where tabname='"+table_name.toUpperCase()+"' and tabschema='"+tabschema.toUpperCase()+"'";
            log.info("db2查询"+table_name+"建表主键，sql："+keySql);
            createTablePs = conn.prepareStatement(keySql);
            createTableRs = createTablePs.executeQuery();
            while (createTableRs.next()){
                if(StringUtils.isNotBlank(createTableRs.getString("COLNAME"))){
                    keyColumns.add(createTableRs.getString("COLNAME"));
                }
            }
        }catch (Exception keyExecption){
            log.info("查询主键信息失败");
            log.error(keyExecption.getMessage(),keyExecption);
        }finally {
            closeAll(null,createTablePs,createTableRs,null);
        }
        try {
            String parSql = "select DATAPARTITIONNAME from syscat.datapartitions where tabname='"+table_name.toUpperCase()+"' and tabschema='"+tabschema.toUpperCase()+"'";
            log.info("db2查询"+table_name+"建表分区，sql："+parSql);
            createTablePs = conn.prepareStatement(parSql);
            createTableRs = createTablePs.executeQuery();
            while(createTableRs.next()){
                if(StringUtils.isNotBlank(createTableRs.getString("DATAPARTITIONNAME"))){
                    parColumns.add(createTableRs.getString("DATAPARTITIONNAME"));
                }
            }
        }catch (Exception parExecption){
            log.info("查询分区信息失败");
            log.error(parExecption.getMessage(),parExecption);
        }finally {
            closeAll(null,createTablePs,createTableRs,null);
        }
        try {
            String indSql = "select COLNAMES from syscat.indexes where tabname='"+table_name.toUpperCase()+"' and owner='"+tabschema.toUpperCase()+"'";
            log.info("db2查询"+table_name+"建表索引，sql："+indSql);
            createTablePs = conn.prepareStatement(indSql);
            createTableRs = createTablePs.executeQuery();
            String indexColumns = "";
            while (createTableRs.next()){
                if(StringUtils.isNotBlank(createTableRs.getString("COLNAMES"))){
                    indexColumns = createTableRs.getString("COLNAMES");
                }
            }
            String[] columns = indexColumns.split("\\+");
            if(columns!=null){
                for (int i=0;i<columns.length;i++){
                    if(StringUtils.isNotBlank(columns[i])){
                        indColumns.add(columns[i]);
                    }
                }
            }
        }catch (Exception indExecption){
            log.info("查询索引信息失败");
            log.error(indExecption.getMessage(),indExecption);
        }finally {
            closeAll(null,createTablePs,createTableRs,null);
        }
        if(keyColumns.size()>0){
            for (int i=0;i<keyColumns.size();i++){
                for (int j=0;j<temporaryTabInfo.getColInfos().size();j++){
                    if(keyColumns.get(i).equalsIgnoreCase(temporaryTabInfo.getColInfos().get(j).getCol_id())){
                        temporaryTabInfo.getColInfos().get(j).setIs_key("1");
                        break;
                    }
                }
            }
        }
        if(parColumns.size()>0){
            for (int i=0;i<parColumns.size();i++){
                for (int j=0;j<temporaryTabInfo.getColInfos().size();j++){
                    if(parColumns.get(i).equalsIgnoreCase(temporaryTabInfo.getColInfos().get(j).getCol_id())){
                        temporaryTabInfo.getColInfos().get(j).setIs_partition("1");
                    }
                }
            }
        }
        if(indColumns.size()>0){
            for (int i=0;i<indColumns.size();i++){
                for (int j=0;j<temporaryTabInfo.getColInfos().size();j++){
                    if(indColumns.get(i).equalsIgnoreCase(temporaryTabInfo.getColInfos().get(j).getCol_id())){
                        temporaryTabInfo.getColInfos().get(j).setIs_index("1");
                    }
                }
            }
        }
    }

    public String queryTableByIndexNameGreenplum(String table_name,String tabschema) throws Exception{
        PreparedStatement index_ps = null;
        ResultSet index_rs = null;
        try {
            //约定drop_index语句中将index_name放在table_name中
            String index_name = table_name;
            String sql = "select tablename from pg_indexes where upper(indexname)=upper('"+index_name+"')";
            log.info("执行语句："+sql);
            index_ps = conn.prepareStatement(sql);
            index_rs = index_ps.executeQuery();
            table_name = "";
            while(index_rs.next()){
                //用索引名换取对应执行表
                table_name = index_rs.getString("tablename");
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
        }finally {
            closeAll(null,index_ps,index_rs,null);
        }
        return table_name;
    }

    protected void dealGreenplumColumnInfo(TemporaryTabInfo temporaryTabInfo,String table_name,String tabschema) throws Exception{
        List<String> keyColumns = new ArrayList<>();
        List<String> parColumns = new ArrayList<>();
        List<String> indColumns = new ArrayList<>();
        PreparedStatement createTablePs = null;
        ResultSet createTableRs = null;
        try {
            String keySql = "select a.attname "
                            + "    from pg_catalog.pg_attribute a,pg_catalog.pg_class c, pg_catalog.pg_namespace n "
                            + "    where  a.attrelid=c.oid  and c.relname=lower('"+table_name+"') "
                            + "    and a.attnum>0 AND NOT a.attisdropped    and n.oid = c.relnamespace "
                            + "    and n.nspname=lower('"+tabschema+"')   order by a.attnum ";
            log.info("greenplum查询"+table_name+"建表主键，sql："+keySql);
            createTablePs = conn.prepareStatement(keySql);
            createTableRs = createTablePs.executeQuery();
            while (createTableRs.next()){
                if(StringUtils.isNotBlank(createTableRs.getString("attname"))){
                    keyColumns.add(createTableRs.getString("attname"));
                }
            }
        }catch (Exception keyExecption){
            log.info("查询主键信息失败");
            log.error(keyExecption.getMessage(),keyExecption);
        }finally {
            closeAll(null,createTablePs,createTableRs,null);
        }
        try {
            String parSql = "select t1.columnname from pg_catalog.pg_partition_columns t1 inner join pg_tables t2  on t1.tablename=t2.tablename and t2.tableowner='"+tabschema+"' where t1.tablename=lower('"+table_name+"')";
            log.info("greenplum查询"+table_name+"建表分区，sql："+parSql);
            createTablePs = conn.prepareStatement(parSql);
            createTableRs = createTablePs.executeQuery();
            while(createTableRs.next()){
                if(StringUtils.isNotBlank(createTableRs.getString("columnname"))){
                    String[] partition_cols = createTableRs.getString("columnname").split(",",-1);
                    for (int i=0;i<partition_cols.length;i++){
                        parColumns.add(partition_cols[i]);
                    }
                }
            }
        }catch (Exception parExecption){
            log.info("查询分区信息失败");
            log.error(parExecption.getMessage(),parExecption);
        }finally {
            closeAll(null,createTablePs,createTableRs,null);
        }
        try {
            String indSql = "select indexdef from pg_indexes where tablename=lower('"+table_name+"')";
            log.info("greenplum查询"+table_name+"建表索引，sql："+indSql);
            createTablePs = conn.prepareStatement(indSql);
            createTableRs = createTablePs.executeQuery();
            String indexColumns = "";
            while (createTableRs.next()){
                if(StringUtils.isNotBlank(createTableRs.getString("indexdef"))){
                    String indexSql = createTableRs.getString("indexdef");
                    int begin = indexSql.indexOf("(");
                    int end = indexSql.indexOf(")");
                    if(begin!=-1&&end!=-1){
                        indexSql = indexSql.substring(begin,end);
                        String[] columns = indexSql.split(",",-1);
                        for (int i=0;i<columns.length;i++){
                            if(StringUtils.isNotBlank(columns[i])){
                                indColumns.add(columns[i].trim());
                            }
                        }
                    }
                }
            }
        }catch (Exception indExecption){
            log.info("查询索引信息失败");
            log.error(indExecption.getMessage(),indExecption);
        }finally {
            closeAll(null,createTablePs,createTableRs,null);
        }
        if(keyColumns.size()>0){
            for (int i=0;i<keyColumns.size();i++){
                for (int j=0;j<temporaryTabInfo.getColInfos().size();j++){
                    if(keyColumns.get(i).equalsIgnoreCase(temporaryTabInfo.getColInfos().get(j).getCol_id())){
                        temporaryTabInfo.getColInfos().get(j).setIs_key("1");
                        break;
                    }
                }
            }
        }
        if(parColumns.size()>0){
            for (int i=0;i<parColumns.size();i++){
                for (int j=0;j<temporaryTabInfo.getColInfos().size();j++){
                    if(parColumns.get(i).equalsIgnoreCase(temporaryTabInfo.getColInfos().get(j).getCol_id())){
                        temporaryTabInfo.getColInfos().get(j).setIs_partition("1");
                    }
                }
            }
        }
        if(indColumns.size()>0){
            for (int i=0;i<indColumns.size();i++){
                for (int j=0;j<temporaryTabInfo.getColInfos().size();j++){
                    if(indColumns.get(i).equalsIgnoreCase(temporaryTabInfo.getColInfos().get(j).getCol_id())){
                        temporaryTabInfo.getColInfos().get(j).setIs_index("1");
                    }
                }
            }
        }
    }

    public String queryTableByIndexNameMysql(String table_name,String tabschema) throws Exception{
        PreparedStatement index_ps = null;
        ResultSet index_rs = null;
        try {
            //约定drop_index语句中将index_name放在table_name中
            String index_name = table_name;
            String sql = "select table_name as tablename from information_schema.statistics where upper(table_schema) =upper('"+tabschema+"') and upper(index_name)=upper('"+index_name+"')";
            log.info("执行语句："+sql);
            index_ps = conn.prepareStatement(sql);
            index_rs = index_ps.executeQuery();
            table_name = "";
            while(index_rs.next()){
                //用索引名换取对应执行表
                table_name = index_rs.getString("tablename");
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
        }finally {
            closeAll(null,index_ps,index_rs,null);
        }
        return table_name;
    }

    protected void dealMysqlColumnInfo(TemporaryTabInfo temporaryTabInfo,String table_name,String tabschema) throws Exception{
        List<String> keyColumns = new ArrayList<>();
        List<String> parColumns = new ArrayList<>();
        List<String> indColumns = new ArrayList<>();
        PreparedStatement createTablePs = null;
        ResultSet createTableRs = null;
        try {
            String keySql = "select column_name from information_schema.key_column_usage where upper(table_schema)=upper('"+tabschema+"') and upper(table_name)=upper('"+table_name+"')";
            log.info("mysql查询"+table_name+"建表主键，sql："+keySql);
            createTablePs = conn.prepareStatement(keySql);
            createTableRs = createTablePs.executeQuery();
            while (createTableRs.next()){
                if(StringUtils.isNotBlank(createTableRs.getString("column_name"))){
                    keyColumns.add(createTableRs.getString("column_name"));
                }
            }
        }catch (Exception keyExecption){
            log.info("查询主键信息失败");
            log.error(keyExecption.getMessage(),keyExecption);
        }finally {
            closeAll(null,createTablePs,createTableRs,null);
        }
        try {
            String parSql = "select distinct partition_expression as partition_column from information_schema.partitions where upper(table_schema)=upper('"+tabschema+"') and upper(table_name)=upper('"+table_name+"')";
            log.info("mysql查询"+table_name+"建表分区，sql："+parSql);
            createTablePs = conn.prepareStatement(parSql);
            createTableRs = createTablePs.executeQuery();
            while(createTableRs.next()){
                if(StringUtils.isNotBlank(createTableRs.getString("partition_column"))){
                    String parColumn = createTableRs.getString("partition_column");
                    if(parColumn.indexOf("'")!=-1){
                        parColumn.replace("'","");
                    }
                    if(parColumn.indexOf("(")!=-1&&parColumn.indexOf(")")!=-1){
                        parColumn = parColumn.substring(parColumn.indexOf("("),parColumn.indexOf(")")-parColumn.indexOf("("));
                    }
                    parColumns.add(parColumn);
                }
            }
        }catch (Exception parExecption){
            log.info("查询分区信息失败");
            log.error(parExecption.getMessage(),parExecption);
        }finally {
            closeAll(null,createTablePs,createTableRs,null);
        }
        try {
            String indSql = "select column_name from information_schema.statistics where upper(table_schema) =upper('"+tabschema+"') and upper(table_name)=upper('"+table_name+"')";
            log.info("mysql查询"+table_name+"建表索引，sql："+indSql);
            createTablePs = conn.prepareStatement(indSql);
            createTableRs = createTablePs.executeQuery();
            while (createTableRs.next()){
                if(StringUtils.isNotBlank(createTableRs.getString("column_name"))){
                    indColumns.add(createTableRs.getString("column_name"));
                }
            }
        }catch (Exception indExecption){
            log.info("查询索引信息失败");
            log.error(indExecption.getMessage(),indExecption);
        }finally {
            closeAll(null,createTablePs,createTableRs,null);
        }
        if(keyColumns.size()>0){
            for (int i=0;i<keyColumns.size();i++){
                for (int j=0;j<temporaryTabInfo.getColInfos().size();j++){
                    if(keyColumns.get(i).equalsIgnoreCase(temporaryTabInfo.getColInfos().get(j).getCol_id())){
                        temporaryTabInfo.getColInfos().get(j).setIs_key("1");
                        break;
                    }
                }
            }
        }
        if(parColumns.size()>0){
            for (int i=0;i<parColumns.size();i++){
                for (int j=0;j<temporaryTabInfo.getColInfos().size();j++){
                    if(parColumns.get(i).equalsIgnoreCase(temporaryTabInfo.getColInfos().get(j).getCol_id())){
                        temporaryTabInfo.getColInfos().get(j).setIs_partition("1");
                    }
                }
            }
        }
        if(indColumns.size()>0){
            for (int i=0;i<indColumns.size();i++){
                for (int j=0;j<temporaryTabInfo.getColInfos().size();j++){
                    if(indColumns.get(i).equalsIgnoreCase(temporaryTabInfo.getColInfos().get(j).getCol_id())){
                        temporaryTabInfo.getColInfos().get(j).setIs_index("1");
                    }
                }
            }
        }
    }

}
