package com.newland.edc.cct.plsqltool.interactive.util;

import com.newland.edc.cct.plsqltool.interactive.model.javabean.DataAssetEntity;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.TableOptAuthority;
import com.newland.edc.cct.plsqltool.interactive.tool.ConcurrentExcutor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

public class DataCatch {

    private static Map<String, DataAssetEntity>   dataAssetEntityMap   = new HashMap<>();//数据缓存
    private static Map<String,String>             varsMap              = new HashMap<>();//变量缓存
    private static Map<String,Connection>         conMap               = new HashMap<>();//连接映射
    private static Map<String,PreparedStatement>  psMap                = new HashMap<>();//数据库对话
    private static Map<String, TableOptAuthority> tableOptAuthorityMap = new HashMap<>();//用户数据库操作权限地图
    private static boolean                        flag                 =true;

    private static ConcurrentExcutor ftpFileSynchronousPools;

    private static ConcurrentExcutor hiveAsyExecutePools;
    private static ConcurrentExcutor oracleAsyExecutePools;
    private static ConcurrentExcutor db2AsyExecutePools;
    private static ConcurrentExcutor greenplumAsyExecutePools;
    private static ConcurrentExcutor mysqlAsyExecutePools;
    private static ConcurrentExcutor gbaseAsyExecutePools;

    private static ConcurrentExcutor hiveSynExecutePools;
    private static ConcurrentExcutor oracleSynExecutePools;
    private static ConcurrentExcutor db2SynExecutePools;
    private static ConcurrentExcutor greenplumSynExecutePools;
    private static ConcurrentExcutor mysqlSynExecutePools;
    private static ConcurrentExcutor gbaseSynExecutePools;

    public static void createHiveAsyExecutePools(int PLS_EXECUTE_HIVE_ASY_POOL){
        hiveAsyExecutePools = new ConcurrentExcutor(PLS_EXECUTE_HIVE_ASY_POOL);
    }
    public static void createOracleAsyExecutePools(int PLS_EXECUTE_ORACLE_ASY_POOL){
        oracleAsyExecutePools = new ConcurrentExcutor(PLS_EXECUTE_ORACLE_ASY_POOL);
    }
    public static void createDB2AsyExecutePools(int PLS_EXECUTE_DB2_ASY_POOL){
        db2AsyExecutePools = new ConcurrentExcutor(PLS_EXECUTE_DB2_ASY_POOL);
    }
    public static void createGreenplumAsyExecutePools(int PLS_EXECUTE_GREENPLUM_ASY_POOL){
        greenplumAsyExecutePools = new ConcurrentExcutor(PLS_EXECUTE_GREENPLUM_ASY_POOL);
    }
    public static void createMysqlAsyExecutePools(int PLS_EXECUTE_MYSQL_ASY_POOL){
        mysqlAsyExecutePools = new ConcurrentExcutor(PLS_EXECUTE_MYSQL_ASY_POOL);
    }
    public static void createGbaseAsyExecutePools(int PLS_EXECUTE_GBASE_ASY_POOL){
        gbaseAsyExecutePools = new ConcurrentExcutor(PLS_EXECUTE_GBASE_ASY_POOL);
    }

    public static void createHiveSynExecutePools(int PLS_EXECUTE_HIVE_SYN_POOL){
        hiveSynExecutePools = new ConcurrentExcutor(PLS_EXECUTE_HIVE_SYN_POOL);
    }
    public static void createOracleSynExecutePools(int PLS_EXECUTE_ORACLE_SYN_POOL){
        oracleSynExecutePools = new ConcurrentExcutor(PLS_EXECUTE_ORACLE_SYN_POOL);
    }
    public static void createDB2SynExecutePools(int PLS_EXECUTE_DB2_SYN_POOL){
        db2SynExecutePools = new ConcurrentExcutor(PLS_EXECUTE_DB2_SYN_POOL);
    }
    public static void createGreenplumSynExecutePools(int PLS_EXECUTE_GREENPLUM_SYN_POOL){
        greenplumSynExecutePools = new ConcurrentExcutor(PLS_EXECUTE_GREENPLUM_SYN_POOL);
    }
    public static void createMysqlSynExecutePools(int PLS_EXECUTE_MYSQL_SYN_POOL){
        mysqlSynExecutePools = new ConcurrentExcutor(PLS_EXECUTE_MYSQL_SYN_POOL);
    }
    public static void createGbaseSynExecutePools(int PLS_EXECUTE_GBASE_SYN_POOL){
        gbaseSynExecutePools = new ConcurrentExcutor(PLS_EXECUTE_GBASE_SYN_POOL);
    }
    public static void createFtpFileSynchronousPools(int FTP_FILE_SYNCHRONOUS_POOL){
        ftpFileSynchronousPools = new ConcurrentExcutor(FTP_FILE_SYNCHRONOUS_POOL);
    }

    public static Map<String, DataAssetEntity> getDataAssetEntityMap(){
        return dataAssetEntityMap;
    }

    public static Map<String,String> getVarsMap(){ return varsMap; }

    public static void setVarsMap(Map<String,String> newMap){ varsMap = newMap; }

    public static synchronized  boolean getFlag(){
        return flag;
    }

    public static synchronized  void setFlag(boolean state){
        flag = state;
    }

    public static Map<String, Connection> getConMap() {
        return conMap;
    }

    public static Map<String, PreparedStatement> getPsMap() { return psMap; }

    public static Map<String, TableOptAuthority> getTableOptAuthorityMap() {return tableOptAuthorityMap; }

    public static ConcurrentExcutor getHiveAsyExecutePools() {
        return hiveAsyExecutePools;
    }

    public static void setHiveAsyExecutePools(ConcurrentExcutor hiveAsyExecutePools) {
        DataCatch.hiveAsyExecutePools = hiveAsyExecutePools;
    }

    public static ConcurrentExcutor getOracleAsyExecutePools() {
        return oracleAsyExecutePools;
    }

    public static void setOracleAsyExecutePools(ConcurrentExcutor oracleAsyExecutePools) {
        DataCatch.oracleAsyExecutePools = oracleAsyExecutePools;
    }

    public static ConcurrentExcutor getHiveSynExecutePools() {
        return hiveSynExecutePools;
    }

    public static void setHiveSynExecutePools(ConcurrentExcutor hiveSynExecutePools) {
        DataCatch.hiveSynExecutePools = hiveSynExecutePools;
    }

    public static ConcurrentExcutor getOracleSynExecutePools() {
        return oracleSynExecutePools;
    }

    public static void setOracleSynExecutePools(ConcurrentExcutor oracleSynExecutePools) {
        DataCatch.oracleSynExecutePools = oracleSynExecutePools;
    }

    public static ConcurrentExcutor getDb2AsyExecutePools() {
        return db2AsyExecutePools;
    }

    public static void setDb2AsyExecutePools(ConcurrentExcutor db2AsyExecutePools) {
        DataCatch.db2AsyExecutePools = db2AsyExecutePools;
    }

    public static ConcurrentExcutor getDb2SynExecutePools() {
        return db2SynExecutePools;
    }

    public static void setDb2SynExecutePools(ConcurrentExcutor db2SynExecutePools) {
        DataCatch.db2SynExecutePools = db2SynExecutePools;
    }

    public static ConcurrentExcutor getGreenplumAsyExecutePools() {
        return greenplumAsyExecutePools;
    }

    public static void setGreenplumAsyExecutePools(ConcurrentExcutor greenplumAsyExecutePools) {
        DataCatch.greenplumAsyExecutePools = greenplumAsyExecutePools;
    }

    public static ConcurrentExcutor getGreenplumSynExecutePools() {
        return greenplumSynExecutePools;
    }

    public static void setGreenplumSynExecutePools(ConcurrentExcutor greenplumSynExecutePools) {
        DataCatch.greenplumSynExecutePools = greenplumSynExecutePools;
    }

    public static ConcurrentExcutor getMysqlAsyExecutePools() {
        return mysqlAsyExecutePools;
    }

    public static void setMysqlAsyExecutePools(ConcurrentExcutor mysqlAsyExecutePools) {
        DataCatch.mysqlAsyExecutePools = mysqlAsyExecutePools;
    }

    public static ConcurrentExcutor getMysqlSynExecutePools() {
        return mysqlSynExecutePools;
    }

    public static void setMysqlSynExecutePools(ConcurrentExcutor mysqlSynExecutePools) {
        DataCatch.mysqlSynExecutePools = mysqlSynExecutePools;
    }

    public static ConcurrentExcutor getFtpFileSynchronousPools() {
        return ftpFileSynchronousPools;
    }

    public static void setFtpFileSynchronousPools(ConcurrentExcutor ftpFileSynchronousPools) {
        DataCatch.ftpFileSynchronousPools = ftpFileSynchronousPools;
    }

    public static ConcurrentExcutor getGbaseAsyExecutePools() {
        return gbaseAsyExecutePools;
    }

    public static void setGbaseAsyExecutePools(ConcurrentExcutor gbaseAsyExecutePools) {
        DataCatch.gbaseAsyExecutePools = gbaseAsyExecutePools;
    }

    public static ConcurrentExcutor getGbaseSynExecutePools() {
        return gbaseSynExecutePools;
    }

    public static void setGbaseSynExecutePools(ConcurrentExcutor gbaseSynExecutePools) {
        DataCatch.gbaseSynExecutePools = gbaseSynExecutePools;
    }
}
