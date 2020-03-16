package com.newland.edc.cct.plsqltool.interactive.client;

import com.alibaba.fastjson.JSONObject;
import com.newland.bd.model.cfg.GeneralDBBean;
import com.newland.bd.model.var.ConnInfo;
import com.newland.bd.ms.core.model.RespInfo;
import com.newland.bd.utils.commons.UUIDUtils;
import com.newland.bi.util.common.StringUtil;
import com.newland.edc.cct.dataasset.entity.model.javabean.DataDispBean;
import com.newland.edc.cct.dataasset.entity.model.javabean.ImporterInfo;
import com.newland.edc.cct.dataasset.entity.model.javabean.TabColBean;
import com.newland.edc.cct.dataasset.entity.model.javabean.TabInfoBean;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.ImportRequestInfo;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.ImportResponseInfo;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.TemporaryTabInfo;
import com.newland.edc.cct.plsqltool.interactive.service.PLSqlToolQueryService;
import com.newland.edc.cct.plsqltool.interactive.util.DataSourceAccess;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;

/**
 * @author liuzongjie
 * @version 1.0.0
 * @date 2019-03-22
 */
@Component // 此注解必加
public class ImportEntityBizClient {
    private static final Logger logger = LoggerFactory.getLogger(ImportEntityBizClient.class);

    static Map<String,String> hive_col_len = new HashMap<>();

    @Resource(name ="com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolQueryServiceImpl")
    private PLSqlToolQueryService plSqlToolQueryService;

    static{
        //初始化需要校验的字段长度
        hive_col_len.put("TINYINT","{\"type\":\"range\",\"begin\":\"0\",\"end\":\"255\",\"length\":\"1\"}");
        hive_col_len.put("SMALLINT","{\"type\":\"range\",\"begin\":\"-32768\",\"end\":\"32767\",\"length\":\"2\"}");
        hive_col_len.put("INT","{\"type\":\"range\",\"begin\":\"-2147483648\",\"end\":\"2147483647\",\"length\":\"4\"}");
        hive_col_len.put("BIGINT","{\"type\":\"range\",\"begin\":\"-9223372036854775808\",\"end\":\"9223372036854775807\",\"length\":\"8\"}");
        hive_col_len.put("CHAR","{\"type\":\"length\",\"begin\":\"\",\"end\":\"\",\"length\":\"255\"}");
    }

    public void checkFileHive(ImportRequestInfo requestInfo, ImporterInfo importerInfo) throws Exception{
        TabInfoBean tabInfoBean = getImportEntity(importerInfo);
        if(tabInfoBean==null){
            throw new Exception("无法获取表字段信息");
        }
        List<String> partitions = requestInfo.getPartitionInfo().getPartitionList();
        List<String> partiCols = new ArrayList<>();
        if (partitions != null && partitions.size() > 0) {
            String partiColName;
            for(Iterator var7 = partitions.iterator(); var7.hasNext(); partiCols.add(partiColName.trim())) {
                String p = (String)var7.next();
                if (p.contains("=")) {
                    partiColName = p.substring(0, p.indexOf("="));
                } else {
                    partiColName = p;
                }
            }
        } else {
            logger.info("分区信息为空！");
        }

        List<String> tabPartitions = new ArrayList<>();
        List<String> tabColNames = new ArrayList<>();
        for (TabColBean tabColBean : tabInfoBean.getTab_col_list()) {
            if (tabColBean.getIs_partition() == 1){
                tabPartitions.add(tabColBean.getCol_name());
            }else {
                tabColNames.add(tabColBean.getCol_name());
            }
        }

        File fileIn = new File(requestInfo.getSourceDataFilePath()+requestInfo.getSourceDataFileName());
        File fileOut = new File(UUIDUtils.getUUID());
        requestInfo.setSourceDataFilePath(fileOut.getAbsolutePath().substring(0, fileOut.getAbsolutePath().length()-fileOut.getName().length()));
        requestInfo.setSourceDataFileName(fileOut.getName());
        try (InputStream is = new FileInputStream(fileIn);
                InputStreamReader isr = new InputStreamReader(is,"GBK");
                BufferedReader buff = new BufferedReader(isr);
                OutputStream os = new FileOutputStream(fileOut);
                OutputStreamWriter osw = new OutputStreamWriter(os,"UTF-8")
            ){

            String line = buff.readLine();
            if (!StringUtil.isBlank(line)){
                line = line.toUpperCase();
            }
            String[] columns = line.split(",");
            if(columns.length!=tabColNames.size()){
                throw new Exception("字段数量有误");
            }
            if(partiCols.size()!=tabPartitions.size()){
                throw new Exception("分区数量有误");
            }
            //检测字段是否全在实体里面
            for (int i=0;i<columns.length;i++){
                if(!columns[i].equalsIgnoreCase(tabColNames.get(i))){
                    throw new Exception("无法匹配字段"+columns[i]+", 表中对应字段为"+tabColNames.get(i));
                }
            }
            //检测分区是否在实体中
            for (int i=0;i<partiCols.size();i++){
                boolean flag = false;
                for (String tabPartition : tabPartitions) {
                    if(partiCols.get(i).equalsIgnoreCase(tabPartition)){
                            flag = true;
                            break;
                    }
                }
                if(!flag){
                    throw new Exception("分区字段设置有误,"+partiCols.get(i)+"与表信息配置不一致");
                }
            }

            Map<Integer,String> map = new HashMap<>();
            Map<Integer, TabColBean> tabColBeanMap = new HashMap<>();
            for (int i=0;i<columns.length;i++){
                for (int j=0;j<tabInfoBean.getTab_col_list().size();j++){
                    if(columns[i].equalsIgnoreCase(tabInfoBean.getTab_col_list().get(j).getCol_name())){
                        if(hive_col_len.get(tabInfoBean.getTab_col_list().get(j).getCol_type().toUpperCase())!=null){
                            map.put(i,hive_col_len.get(tabInfoBean.getTab_col_list().get(j).getCol_type().toUpperCase()));
                            tabColBeanMap.put(i,tabInfoBean.getTab_col_list().get(j));
                            break;
                        }
                    }
                }
            }
            List<String> file_content = new ArrayList<>();
            String content = null;
            int lineNumber = 1;
            while((content=buff.readLine())!=null){
                file_content.add(content);
                ++lineNumber;
                String[] colContents = content.split(",",-1);
                for (int key : map.keySet()){
                    JSONObject object = JSONObject.parseObject(map.get(key));
                    if(object!=null){
                        if(object.get("type").equals("range")){
                            BigDecimal begin = new BigDecimal(object.get("begin").toString());
                            BigDecimal end = new BigDecimal(object.get("end").toString());
                            if(StringUtils.isBlank(colContents[key]) || (begin.compareTo(new BigDecimal(colContents[key]))<=0 && end.compareTo(new BigDecimal(colContents[key]))>=0)){
                                continue;
                            }else{
                                throw new Exception("第"+lineNumber+"行 第"+(key+1)+"列"+colContents[key]+"超界，请重新设置值("+begin+"-"+end+")");
                            }
                        }else if(object.get("type").equals("length")){
                            Long length = Long.parseLong(object.get("length").toString());
                            if(colContents[key].getBytes("UTF-8").length<=length){
                                if(tabColBeanMap.get(key)!=null&&!StringUtil.isBlank(tabColBeanMap.get(key).getCol_length())){
                                    if(colContents[key].getBytes("UTF-8").length<=Integer.parseInt(tabColBeanMap.get(key).getCol_length())){
                                        continue;
                                    }else{
                                        throw new Exception("字段长度超过表定义实际长度，请重新调整数据");
                                    }
                                }else{
                                    throw new Exception("查无该字段或字段长度未设置");
                                }
                            }else{
                                throw new Exception("第"+lineNumber+"行 第"+(key+1)+"列"+colContents[key]+"超界，请重新设置长度("+length+")");
                            }
                        }
                    }else{
                        throw new Exception("比对失败");
                    }
                }
            }

            logger.info("开始删除数据头, 文件路径："+fileOut.getAbsolutePath().substring(0, fileOut.getAbsolutePath().length()-fileOut.getName().length())+"  文件名："+fileOut.getName());
            for (int i=0;i<file_content.size();i++){
                osw.write(file_content.get(i));
                osw.write("\r\n");
            }
            logger.info("开始删除数据头结束");

        }catch (Exception e){
            logger.error(e.getMessage(),e);
            throw e;
        }
    }

    /**
     * 获取实体表、临时表信息
     * @param importerInfo
     * @return
     * @throws Exception
     */
    public TabInfoBean getImportEntity(ImporterInfo importerInfo) throws Exception{
        if(importerInfo.getSource_way()!=null&&importerInfo.getSource_way().equals("1")){
            //提取资产实体
            TabInfoBean tabInfoBean = plSqlToolQueryService.getTabEntityInfo(importerInfo.getTab_id());
            return tabInfoBean;
        }else if(importerInfo.getSource_way()!=null&&importerInfo.getSource_way().equals("2")){
            //请求plsqltool查询临时表字段信息
            TemporaryTabInfo temporaryTabInfo = new TemporaryTabInfo();
            if(!StringUtil.isBlank(importerInfo.getTab_id())){
                temporaryTabInfo.setTab_id(importerInfo.getTab_id());
            }
            if(!StringUtil.isBlank(importerInfo.getPhy_tab_name())){
                temporaryTabInfo.setTab_name(importerInfo.getPhy_tab_name());
            }
            if(!StringUtil.isBlank(importerInfo.getResource_id())){
                temporaryTabInfo.setResource_id(importerInfo.getResource_id());
            }
            if(!StringUtil.isBlank(importerInfo.getConn_id())){
                temporaryTabInfo.setConn_id(importerInfo.getConn_id());
            }
            RespInfo respInfo = plSqlToolQueryService.getTemporaryInfoByTrans(temporaryTabInfo);
            //请求并返回成功
            if("1".equals(respInfo.getRespResult())) {
                List<TabInfoBean> tabInfoBeans = (List<TabInfoBean>)respInfo.getRespData();
                if(tabInfoBeans!=null&&tabInfoBeans.size()>0){
                    return tabInfoBeans.get(0);
                }else{
                    throw new Exception("查无临时表信息,请确认该临时表是否存在");
                }
            }else{
                logger.info("查询临时表接口失败，失败信息："+respInfo.getRespErrorDesc());
                throw new Exception(respInfo.getRespErrorDesc());
            }
        }else{
            return null;
        }
    }

    /**
     * 数据导入 oracle
     * @param importerInfo
     * @throws Exception
     */
    public ImportResponseInfo dealCSVByJDBC(ImportRequestInfo requestInfo,ImporterInfo importerInfo) throws Exception{
        ImportResponseInfo responseInfo = new ImportResponseInfo();
        if(importerInfo==null){
            logger.info("请求实体为空");
            responseInfo.setResultCode(0);
            responseInfo.setDesc("请求实体为空");
            return responseInfo;
        }
        logger.info("插入表："+importerInfo.getPhy_tab_name());
        logger.info("资源id："+importerInfo.getResource_id());
        logger.info("连接id："+importerInfo.getConn_id());
        logger.info("文件源："+requestInfo.getSourceDataFilePath()+requestInfo.getSourceDataFileName());
        ConnInfo connInfo = DataSourceAccess.getConnInfoByConnId(importerInfo.getConn_id());
        GeneralDBBean generalDBBean = DataSourceAccess.getDBCfgByConnInfo(connInfo);
        //创建连接
        Class.forName(generalDBBean.getDriver());
        PreparedStatement stmt = null;
        Statement delStmt = null;
        StringBuilder rspMsg = new StringBuilder();
        File file = new File(requestInfo.getSourceDataFilePath()+requestInfo.getSourceDataFileName());
        try (InputStream down_file = new FileInputStream(file);
                        InputStreamReader isr = new InputStreamReader(down_file,"GBK");
                        BufferedReader reader = new BufferedReader(isr);
                        Connection conn = DriverManager.getConnection(generalDBBean.getUrl(), generalDBBean.getUsername(), generalDBBean.getPassword())
        ){
            String fieldline= reader.readLine();
            if(fieldline==null) {
                throw new Exception("字段为空");
            }
            List<String> file_content = new ArrayList<>();
            String content = null;
            //提取文件内容
            while((content=reader.readLine())!=null){
                file_content.add(content);
            }

            //获取表字段
            TabInfoBean tabInfoBean = getImportEntity(importerInfo);
            //获取数据绑定类型
            String fieldItems[] = fieldline.split(",",-1);
            Map<String,String> colToColtype = getColType(tabInfoBean);
            String col_name = "";
            String col_value = "";
            Set<String> tempSet = new HashSet<>();
            for (int i=0;i<fieldItems.length;i++){
                StringBuffer appender = new StringBuffer("");
                String nullField = fieldItems[i].toUpperCase();
                //字段过多，数据格式异常
                if(nullField.length() > 1000){
                    throw new Exception("数据格式异常");
                }
                if(colToColtype.get(fieldItems[i].toUpperCase()) == null){
                    //特殊字符处理
                    if (StringUtils.isNotBlank(nullField)) {
                        appender = new StringBuffer(nullField.length());
                        for (int j = 0; j < nullField.length(); j++) {
                            char ch = nullField.charAt(j);
                            if ((ch == 0x9) || (ch == 0xA) || (ch == 0xD) || ((ch >= 0x20) && (ch <= 0xD7FF)) || ((ch >= 0xE000) && (ch <= 0xFFFD))
                                    ||((ch >= 0x10000) && (ch <= 0x10FFFF)))
                                appender.append(ch);
                        }
                    }
                    throw new Exception("目标表中不存在" + appender + "字段");
                }
                if (!tempSet.add(fieldItems[i].toUpperCase())){
                    throw new Exception("导入文件中" + fieldItems[i].toUpperCase() + "字段重复");
                }
                if(i==0){
                    col_name = fieldItems[i].toUpperCase();
                    col_value = colToColtype.get(fieldItems[i].toUpperCase());
                }else{
                    col_name = col_name+","+fieldItems[i].toUpperCase();
                    col_value = col_value+","+colToColtype.get(fieldItems[i].toUpperCase());
                }
            }
            StringBuffer sql = new StringBuffer();

            sql.append("insert into "+importerInfo.getPhy_tab_name()+" ("+col_name+") values ("+col_value+")");
            logger.info("执行语句sql："+sql.toString());

            stmt = conn.prepareStatement(sql.toString());
            int succCount = 0;
            int failCount = 0;
            //覆盖操作
            if("cover".equals(requestInfo.getOutputMode())){
                try{
                    StringBuffer delSql = new StringBuffer();
                    delSql.append("TRUNCATE TABLE " + importerInfo.getPhy_tab_name());
                    delStmt = conn.createStatement();
                    delStmt.executeUpdate(delSql.toString());
                }catch (SQLException e){
                    logger.error("清空数据异常",e);
                }
            }
            for (int i=0;i<file_content.size();i++){
                try {
                    String[] str = file_content.get(i).split(",",-1);
                    if (str.length == 1 && str[0].isEmpty() && fieldItems.length != 1) continue;
                    for (int j=0;j<str.length;j++){
                        stmt.setString(j+1,str[j].isEmpty()? null : str[j]);
                    }
                    stmt.executeUpdate();
                    stmt.clearParameters();
                    succCount++;
                }catch (SQLException e){
                    failCount++;
                    rspMsg.append("第" + (i+2) + "行数据插入异常，" + e.getMessage()+ "\n");
                    logger.error("第" + (i+2) + "行数据插入异常",e);
                }catch (Exception e1){
                    failCount++;
                    rspMsg.append("第" + (i+2) + "行数据插入异常，数据字段数量有误\n");
                    logger.error("第" + (i+2) + "行数据插入异常",e1);
                }
            }
            if (rspMsg.toString().isEmpty()){
                responseInfo.setResultCode(1);
                responseInfo.setSuccessCount(succCount);
                responseInfo.setFailureCount(failCount);
            }else {
                responseInfo.setDesc(rspMsg.toString());
                responseInfo.setResultCode(0);
                responseInfo.setSuccessCount(succCount);
                responseInfo.setFailureCount(failCount);
            }
        }catch (Exception e){
            logger.error(e.getMessage(),e);
            responseInfo.setDesc(e.getMessage());
            responseInfo.setResultCode(0);
        }finally {
            if(stmt!=null){
                stmt.close();
            }
            if(delStmt != null){
                delStmt.close();
            }
        }
        return responseInfo;
    }

    /**
     * 判断字段类型
     * @param col_type
     * @return
     */
    public static String changeColType(String col_type) {
        String type = col_type;
        if (StringUtils.isNotEmpty(col_type)) {
            if (type.toLowerCase().indexOf("number") != -1 || type.toLowerCase().indexOf("float") != -1 || type.toLowerCase().indexOf("int") != -1
                            || type.toLowerCase().indexOf("decimal") != -1 || type.toLowerCase().indexOf("numeric") != -1 || type.toLowerCase().indexOf("serial") != -1
                            || type.toLowerCase().indexOf("double") != -1 || type.toLowerCase().indexOf("real") != -1) {
                type = "2";
            } else if (type.toLowerCase().indexOf("varchar") != -1 || type.toLowerCase().indexOf("blob") != -1 || type.toLowerCase().indexOf("clob") != -1
                            || type.toLowerCase().indexOf("char") != -1 || type.toLowerCase().indexOf("string") != -1 || type.toLowerCase().indexOf("text") != -1
                            || type.toLowerCase().indexOf("long") != -1) {
                type = "1";
            } else if (type.toLowerCase().indexOf("date") != -1 || type.toLowerCase().indexOf("time") != -1 || type.toLowerCase().indexOf("interval") != -1) {
                type = "3";
            }
        }
        return type;
    }

    /**
     * 查询字段类型
     * @param tabInfoBean
     * @return
     */
    public Map<String,String> getColType(TabInfoBean tabInfoBean){

        Map<String,String> colToColtype = new HashMap<>();
        if(tabInfoBean.getTab_col_list()!=null&&tabInfoBean.getTab_col_list().size()>0){
            for (int i=0;i<tabInfoBean.getTab_col_list().size();i++){
                String colId = tabInfoBean.getTab_col_list().get(i).getCol_id().toUpperCase();
                String colType = tabInfoBean.getTab_col_list().get(i).getCol_type();
                String colTypeDim = changeColType(colType);
                //                if(colTypeDim.equals("1")){
                //                    colToColtype.put(colId,"':"+colId+"'");
                //                }else if(colTypeDim.equals("2")){
                //                    colToColtype.put(colId,"':"+colId+"'");
                //                }else
                if(colTypeDim.equals("3")){
                    DataDispBean dispBean = tabInfoBean.getDispose_list().get(0);
                    if(dispBean!=null&&!dispBean.getEntity_type().isEmpty()&&dispBean.getEntity_type().toUpperCase().equals("MYSQL")){
                        colToColtype.put(colId,"date_format(?,'%Y-%m-%d %H%I%S')");
                    }else if (dispBean!=null&&!dispBean.getEntity_type().isEmpty()&&dispBean.getEntity_type().toUpperCase().equals("ORACLE")) {
                        colToColtype.put(colId,"to_date(?,'yyyy-mm-dd hh24:mi:ss')");
                    }else if (dispBean!=null&&!dispBean.getEntity_type().isEmpty()&&dispBean.getEntity_type().toUpperCase().equals("DB2")) {
                        colToColtype.put(colId,"?");
                    }else if (dispBean!=null&&!dispBean.getEntity_type().isEmpty()&&dispBean.getEntity_type().toUpperCase().equals("GREENPLUM")) {
                        switch (colType){
                            case "timestamp":
                                colToColtype.put(colId,"to_timestamp(?,'yyyy-mm-dd hh24:mi:ss')");
                                break;
                            case "date":
                                colToColtype.put(colId,"to_date(?,'yyyy-mm-dd')");
                                break;
                            case "time":
                                colToColtype.put(colId,"to_timestamp(?,'hh24:mi:ss')");
                                break;
                            case "interval":
                                colToColtype.put(colId,"interval'?'");
                                break;
                            default:
                                colToColtype.put(colId,"?");
                        }
                    }else {
                        if(dispBean != null){
                            logger.info("dispBean.getEntity_type() is "+ dispBean.getEntity_type());
                        }
                        colToColtype.put(colId,"?");
                    }
                }else{
                    colToColtype.put(colId,"?");
                }
            }
        }
        return colToColtype;
    }
}
