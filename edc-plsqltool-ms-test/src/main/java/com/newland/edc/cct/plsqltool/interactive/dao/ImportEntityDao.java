package com.newland.edc.cct.plsqltool.interactive.dao;

import com.newland.edc.cct.dataasset.entity.model.javabean.DataDispBean;
import com.newland.edc.cct.dataasset.entity.model.javabean.ImporterInfo;
import com.newland.edc.cct.dataasset.entity.model.javabean.LoadDataLog;
import com.newland.edc.cct.dataasset.entity.model.javabean.TabInfoBean;

import java.util.List;
import java.util.Map;

public interface ImportEntityDao {
    public static final String WAIT_LOAD="0";//等待 导入
    public static final String LOADING="1";//正在导入
    public static final String CANCEL_LOAD="2";//导入终止
    public static final String ERROR_LOAD="3";//导入错误
    public static final String LOADED="4";//导入完成
    public static final String CANCEL_LOADING="6";
    public Map<String,Object> getLoadDataLogList(LoadDataLog req_bean, int start_page, int page_count)throws Exception ;
//    public Map<String,Object> insertHiveData(ImporterInfo importerInfo,String []fieldItems  ,String[] items) throws Exception;
    public String insertLoadDataLog(ImporterInfo importerInfo, String user_id) throws Exception;
    public ImporterInfo getUnLoadLog() throws Exception ;
    public int getUnFinishLoadLogNum() throws Exception ;
    public void updateLoadDataLog(String success_cn, String fail_cn, String load_status, String load_seq)throws Exception ;
    public void updateLoadDataLog(String success_cn, String fail_cn, String load_status, String load_seq, String err_msg)throws Exception ;
    public List<DataDispBean> getDataDispBeans(List<String> resource_ids);
    public List<TabInfoBean> getTenantBeans(List<String> resource_ids);
}