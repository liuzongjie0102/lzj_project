package com.newland.edc.cct.plsqltool.interactive.dataimport;

import com.newland.bd.model.cfg.HadoopCfgBean;
import com.newland.edc.cct.dgw.common.DgwConstant;
import com.newland.edc.cct.plsqltool.interactive.client.testRunnable;
import com.newland.edc.cct.plsqltool.interactive.content.PlsqltoolContent;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.DbColumnConf;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.ImportResponseInfo;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.PartitionInfo;
import com.newland.edc.cct.plsqltool.interactive.tool.CallableTask;
import com.newland.edc.cct.plsqltool.interactive.util.ConfigServiceUtil;
import com.newland.edc.cct.plsqltool.interactive.util.JDBCUtils;
import com.newland.edc.cct.plsqltool.interactive.util.KerberosTool;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.PrivilegedExceptionAction;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class DataImportToHive implements IDataImport {
    private static final Logger        logger        = LoggerFactory.getLogger(DataImportToHive.class);
    private              String        sourceDataFilePath;
    private              String        sourceDataFileName;
    private              String        outputMode;
    private              String        targetTable;
    private              PartitionInfo partition;
    private              String        tempTableName = "";
    private              String        fileFullName  = "";
    private              String        tmpHdfsDir    = "/user/EDW_INTF/tmp/dataFile";
    private              String        partiStr      = "";
    private              Set<String>   partiColSet   = new HashSet();
    private              JDBCUtils     jdbcUtils     = null;
    private              HadoopCfgBean hadoopCfgBean = null;
    private              FileSystem    fs            = null;
    private              String        connId;
    private              Configuration conf;

    public DataImportToHive(String sourceDataFilePath, String sourceDataFileName, String outputMode, String targetTable, PartitionInfo partition, HadoopCfgBean hadoopCfgBean, String connId, String tmpHdfsDir) {
        this.tmpHdfsDir = tmpHdfsDir;
        this.hadoopCfgBean = hadoopCfgBean;
        this.sourceDataFilePath = sourceDataFilePath;
        this.sourceDataFileName = sourceDataFileName;
        this.outputMode = outputMode;
        this.targetTable = targetTable;
        this.partition = partition;
        this.connId = connId;
    }

    public void initParam() throws Exception {
        this.jdbcUtils = new JDBCUtils(this.connId);
        this.conf = ConfigServiceUtil.getConfiguration(this.hadoopCfgBean, this.connId);
//        UserGroupInformation ugi = KerberosTool.get(connId,DgwConstant.DB_TYPE_HDFS);
//        if( null  != ugi ){
//            this.fs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
//                @Override
//                public FileSystem run() throws Exception {
//                    return FileSystem.newInstance(conf);
//                }
//            });
//        }else{
//            this.fs = FileSystem.newInstance(conf);
//        }

        int index = this.sourceDataFileName.indexOf(".");
        String prefix = index > 0 ? this.sourceDataFileName.substring(0, index) : this.sourceDataFileName;
        this.tempTableName = prefix + "_" + this.targetTable;
        logger.info("临时表名={}", this.tempTableName);
        if (!this.sourceDataFilePath.endsWith("\\") && !this.sourceDataFilePath.endsWith("/")) {
            this.sourceDataFilePath = this.sourceDataFilePath + "/";
        }

        this.fileFullName = this.sourceDataFilePath + this.sourceDataFileName;
        logger.info("文件全路径名={}", this.fileFullName);
        List<String> partitions = this.partition.getPartitionList();
        if (partitions != null && partitions.size() > 0) {
            StringBuffer buffer = new StringBuffer();

            String partiColName;
            for(Iterator var6 = partitions.iterator(); var6.hasNext(); this.partiColSet.add(partiColName.trim())) {
                String p = (String)var6.next();
                buffer.append(p).append(",");
                partiColName = "";
                if (p.contains("=")) {
                    partiColName = p.substring(0, p.indexOf("="));
                } else {
                    partiColName = p;
                }
            }

            this.partiStr = buffer.toString();
            this.partiStr = this.partiStr.substring(0, this.partiStr.lastIndexOf(","));
            logger.info("拼接分区，分区信息为[{}]", this.partiStr);
        } else {
            logger.info("分区信息为空！");
        }

    }

    public ImportResponseInfo importDataToDb() {
        ImportResponseInfo responseInfo = new ImportResponseInfo();
        try {
            this.initParam();
            this.createTempTable();
            this.importDataToTmp();
            this.insertDataToTarget();
            this.removeTemp();
            responseInfo.setResultCode(1);
            responseInfo.setSuccessCount(this.getRecordCount());
        } catch (Exception var11) {
            logger.error(var11.toString(), var11);
            responseInfo.setResultCode(0);
            if(StringUtils.isBlank(var11.getMessage())){
                logger.info("=============捕获异常信息为空，截取异常堆栈："+ CallableTask.getStackTrace(var11));
                responseInfo.setDesc(CallableTask.getStackTrace(var11));
            }else{
                responseInfo.setDesc(var11.getMessage());
            }
            responseInfo.setFailureCount(this.getRecordCount());
        } finally {
//            try {
//                if (this.fs != null) {
//                    this.fs.close();
//                }
//            } catch (Exception var10) {
//                logger.error(var10.toString());
//            }

            if (this.jdbcUtils != null) {
                this.jdbcUtils.closeAll();
            }

        }

        return responseInfo;
    }

    public int getRecordCount() {
        int recordNum = 0;
        File file = new File(this.fileFullName);
        try (Reader reader = new FileReader(file);
                        LineNumberReader lineNumberReader = new LineNumberReader(reader)){
            lineNumberReader.skip(9223372036854775807L);
            recordNum = lineNumberReader.getLineNumber();
            logger.info("总记录数为：{}", recordNum);
        } catch (Exception var3) {
            logger.error(var3.toString());
        }

        return recordNum;
    }

    public void removeTemp() throws Exception {
        String dropSql = " drop table if exists " + this.tempTableName;
        UserGroupInformation ugiHive = KerberosTool.get(this.connId, DgwConstant.DB_TYPE_HIVE);
        JDBCUtils jdbcUtils = this.jdbcUtils;
        if(ugiHive!=null){
            ugiHive.doAs( new PrivilegedExceptionAction<Void>(){
                @Override
                public Void run() throws Exception{
                    jdbcUtils.executeSql(dropSql);
                    return null;
                }
            });
        }else {
            jdbcUtils.executeSql(dropSql);
        }
    }

    private void createTempTable() throws Exception {
        List<DbColumnConf> columnList = this.getTableColumns();
        String sql = this.concatCreateHiveTableSql(this.tempTableName, columnList);
        logger.info("临时表简表语句， sql={}", sql);
        UserGroupInformation ugiHive = KerberosTool.get(this.connId, DgwConstant.DB_TYPE_HIVE);
        JDBCUtils jdbcUtils = this.jdbcUtils;
        if(ugiHive!=null){
            ugiHive.doAs( new PrivilegedExceptionAction<Void>(){
                @Override
                public Void run() throws Exception{
                    jdbcUtils.executeSql(sql);
                    return null;
                }
            });
        }else {
            jdbcUtils.executeSql(sql);
        }
    }

    private List<DbColumnConf> getTableColumns() throws Exception {
        List<DbColumnConf> columnList = new ArrayList();
        ResultSet result = null;
        JDBCUtils jdbcUtils = this.jdbcUtils;
        String targetTable = this.targetTable;
        try {
            UserGroupInformation ugiHive = KerberosTool.get(this.connId, DgwConstant.DB_TYPE_HIVE);
            if(ugiHive!=null){
                result = ugiHive.doAs( new PrivilegedExceptionAction<ResultSet>(){
                    @Override
                    public ResultSet run() throws Exception{
                        return jdbcUtils.executeQuerySql("desc " + targetTable);
                    }
                });
            }else {
                result = jdbcUtils.executeQuerySql("desc " + targetTable);
            }

            while(result.next()) {
                String columnName = result.getString(1);
                if (!this.partiColSet.contains(columnName) && !StringUtils.isEmpty(columnName) && !columnName.contains("#")) {
                    String columnType = result.getString(2);
                    DbColumnConf column = new DbColumnConf();
                    column.setTarget_col_name(columnName);
                    column.setTarget_col_type(columnType);
                    columnList.add(column);
                }
            }

        } finally {
            if (result != null) {
                try {
                    result.close();
                } catch (SQLException var16) {
                }
            }
        }

        return columnList;
    }

    private String concatCreateHiveTableSql(String targetTableName, List<DbColumnConf> columnConfs) throws Exception {
        String dropSql = "drop table if exists " + targetTableName + ";";
        StringBuffer sb = new StringBuffer(dropSql);
        sb.append("create table ");
        sb.append(targetTableName);
        sb.append("(");
        List<String> colList = new ArrayList();
        Iterator var6 = columnConfs.iterator();

        while(var6.hasNext()) {
            DbColumnConf col = (DbColumnConf)var6.next();
            String column = col.getTarget_col_name() + " " + col.getTarget_col_type();
            colList.add(column);
        }

        sb.append(StringUtils.join(colList, ","));
        sb.append(")");
        sb.append(" row format delimited fields terminated by ',' LINES TERMINATED BY '\\n' stored as textfile;");
        return sb.toString();
    }

    private String colTypeToHiveType(String colType) {
        String var3 = colType.toUpperCase();
        byte var4 = -1;
        switch(var3.hashCode()) {
        case -1981034679:
            if (var3.equals("NUMBER")) {
                var4 = 1;
            }
            break;
        case -1618932450:
            if (var3.equals("INTEGER")) {
                var4 = 5;
            }
            break;
        case -1453246218:
            if (var3.equals("TIMESTAMP")) {
                var4 = 8;
            }
            break;
        case -1282431251:
            if (var3.equals("NUMERIC")) {
                var4 = 2;
            }
            break;
        case 72655:
            if (var3.equals("INT")) {
                var4 = 4;
            }
            break;
        case 2090926:
            if (var3.equals("DATE")) {
                var4 = 7;
            }
            break;
        case 2342524:
            if (var3.equals("LONG")) {
                var4 = 6;
            }
            break;
        case 66988604:
            if (var3.equals("FLOAT")) {
                var4 = 3;
            }
            break;
        case 2021790743:
            if (var3.equals("DOBULE")) {
                var4 = 0;
            }
        }

        String resultType;
        switch(var4) {
        case 0:
        case 1:
        case 2:
            resultType = "double";
            break;
        case 3:
            resultType = "float";
            break;
        case 4:
        case 5:
            resultType = "int";
            break;
        case 6:
            resultType = "bigint";
            break;
        case 7:
            resultType = "date";
            break;
        case 8:
            resultType = "timestamp";
            break;
        default:
            resultType = "string";
        }

        return resultType;
    }

    private String concatAddPartitionSql() {
        String addPartitionSql = "alter table " + this.tempTableName + " if not exists add partition (" + this.partiStr + ");";
        return addPartitionSql;
    }

    private void importDataToTmp() throws Exception {

        Path dirPath = new Path(this.tmpHdfsDir);
        String targetFile = this.tmpHdfsDir + "/" + this.sourceDataFileName;
//        FileSystem fileSystem = this.fs;
        Configuration conf = this.conf;

        UserGroupInformation ugiHdfs = KerberosTool.get(this.connId, DgwConstant.DB_TYPE_HDFS);
        String fileFullName = this.fileFullName;
        try {

            if(ugiHdfs!=null){
                ugiHdfs.doAs( new PrivilegedExceptionAction<Void>(){
                    @Override
                    public Void run() throws Exception{
                        System.out.println("取filesystem");
                        FileSystem fileSystem = FileSystem.newInstance(conf);
                        try {
                            System.out.println("判断目录"+dirPath.toString());
                            if (!fileSystem.exists(dirPath)) {
                                System.out.println("目录不存在"+dirPath.toString());
                                logger.info("{}目录不存在", dirPath.toString());
                                boolean isMkDir = fileSystem.mkdirs(dirPath);
                                logger.info("目录创建结果：{}", isMkDir);
                                fileSystem.setPermission(dirPath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
                            }else{
                                System.out.println("目录存在"+dirPath.toString());
                            }
                            System.out.println("----------------------copyFromLocalFile--------------------");
                            fileSystem.copyFromLocalFile(false, new Path(fileFullName), new Path(targetFile));
                            System.out.println("----------------------setPermission--------------------");
                            fileSystem.setPermission(new Path(targetFile), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
                            System.out.println("----------------------setPermission END--------------------");
                        }finally {
                            try {
                                if (fileSystem != null) {
                                    fileSystem.close();
                                }
                            } catch (Exception var10) {
                                logger.error(var10.toString());
                            }
                        }
                        return null;
                    }
                });
            }else {
                FileSystem fileSystem = FileSystem.newInstance(conf);
                try {
                    if (!fileSystem.exists(dirPath)) {
                        logger.info("{}目录不存在", dirPath.toString());
                        boolean isMkDir = fileSystem.mkdirs(dirPath);
                        logger.info("目录创建结果：{}", isMkDir);
                        fileSystem.setPermission(dirPath, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
                    }else {
                        logger.info("{}目录存在", dirPath.toString());
                    }
                    fileSystem.copyFromLocalFile(false, new Path(fileFullName), new Path(targetFile));
                    fileSystem.setPermission(new Path(targetFile), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
                }finally {
                    try {
                        if (fileSystem != null) {
                            fileSystem.close();
                        }
                    } catch (Exception var10) {
                        logger.error(var10.toString());
                    }
                }
            }

            UserGroupInformation ugiHive = KerberosTool.get(this.connId, DgwConstant.DB_TYPE_HIVE);
            String hql = "load data inpath '" + targetFile + "' into table " + this.tempTableName + ";";
            logger.info("本地数据文件导入临时表， sql={}", hql);
            JDBCUtils jdbcUtils = this.jdbcUtils;
            if(ugiHive!=null){
                ugiHive.doAs( new PrivilegedExceptionAction<Void>(){
                    @Override
                    public Void run() throws Exception{
                        jdbcUtils.executeSql(hql);
                        return null;
                    }
                });
            }else {
                jdbcUtils.executeSql(hql);
            }
        } finally {
            if(ugiHdfs!=null){

                ugiHdfs.doAs( new PrivilegedExceptionAction<Void>(){
                    @Override
                    public Void run() throws Exception{
                        FileSystem fileSystem = FileSystem.newInstance(conf);
                        try {
                            fileSystem.delete(new Path(targetFile), true);
                        }finally {
                            try {
                                if (fileSystem != null) {
                                    fileSystem.close();
                                }
                            } catch (Exception var10) {
                                logger.error(var10.toString());
                            }
                        }
                        return null;
                    }
                });
            }else {
                FileSystem fileSystem = FileSystem.newInstance(conf);
                try {
                    fileSystem.delete(new Path(targetFile), true);
                }finally {
                    try {
                        if (fileSystem != null) {
                            fileSystem.close();
                        }
                    } catch (Exception var10) {
                        logger.error(var10.toString());
                    }
                }
            }
        }
    }

    private void insertDataToTarget() throws Exception {
        StringBuffer sqlBuffer = new StringBuffer();
        if (!this.partition.isDynamicParti()) {
            sqlBuffer.append("set hive.stats.autogather=false;");
        } else {
            sqlBuffer.append("set hive.exec.dynamic.partition=true;");
            sqlBuffer.append("set hive.exec.dynamic.partition.mode=nonstrict;");
        }

        if (StringUtils.equals("cover", this.outputMode)) {
            sqlBuffer.append("insert overwrite table ");
        } else {
            sqlBuffer.append("insert into table  ");
        }

        sqlBuffer.append(this.targetTable);
        if (StringUtils.isNotEmpty(this.partiStr)) {
            sqlBuffer.append(" PARTITION(");
            sqlBuffer.append(this.partiStr).append(")");
        }

        sqlBuffer.append("  select * from ");
        sqlBuffer.append(this.tempTableName).append(";");
        logger.info("数据插入目标表, sql={}", sqlBuffer.toString());
        UserGroupInformation ugiHive = KerberosTool.get(this.connId, DgwConstant.DB_TYPE_HIVE);
        JDBCUtils jdbcUtils = this.jdbcUtils;
        if(ugiHive!=null){
            ugiHive.doAs( new PrivilegedExceptionAction<Void>(){
                @Override
                public Void run() throws Exception{
                    jdbcUtils.executeSql(sqlBuffer.toString());
                    return null;
                }
            });
        }else {
            jdbcUtils.executeSql(sqlBuffer.toString());
        }
    }


    public static void main(String[] args) throws InterruptedException {
        logger.info("test");
        ///app/jdk/jdk1.8.0_171/bin/java -Djava.ext.dirs=./:/app/jdk/jdk1.8.0_171/jre/lib/ext com.newland.edc.cct.plsqltool.interactive.dataimport.DataImportToHive
        Properties prop = new Properties();
        InputStream in = DataImportToHive.class.getResourceAsStream("/testConfig.properties");
        try {
            prop.load(in);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        String url = prop.getProperty("nl-edc-cct-sys-ms");
        Thread thread1 = new Thread(new testRunnable("1", prop.getProperty("connId1"), prop.getProperty("sqlString1"), url));
        Thread thread2 = new Thread(new testRunnable("2", prop.getProperty("connId1"), prop.getProperty("sqlString1"), url));
        Thread thread3 = new Thread(new testRunnable("3", prop.getProperty("connId1"), prop.getProperty("sqlString1"), url));
        Thread thread4 = new Thread(new testRunnable("4", prop.getProperty("connId2"), prop.getProperty("sqlString2"), url));
        Thread thread5 = new Thread(new testRunnable("5", prop.getProperty("connId2"), prop.getProperty("sqlString2"), url));
        Thread thread6 = new Thread(new testRunnable("6", prop.getProperty("connId2"), prop.getProperty("sqlString2"), url));

        thread1.start();
        thread2.start();
        thread3.start();
        thread4.start();
        thread5.start();
        thread6.start();

        thread1.join();
        thread2.join();
        thread3.join();
        thread4.join();
        
        thread5.join();
        thread6.join();
    }
}

