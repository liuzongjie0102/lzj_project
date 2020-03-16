package com.newland.edc.cct.plsqltool.interactive.service;

import com.newland.edc.cct.dataasset.entity.model.DataAssetServiceRequestObject;
import com.newland.edc.cct.dataasset.entity.model.DataAssetServiceResponseObject;
import com.newland.edc.cct.dataasset.entity.model.javabean.ImporterInfo;

/**
 * @author limc
 *
 */
public interface ImportEntityBiz {
    /**
     * 查询导入日志
     * @param requestObject
     * @return
     * @throws Exception
     */
    public DataAssetServiceResponseObject getLoadDataLogList(DataAssetServiceRequestObject requestObject) throws Exception ;
    public DataAssetServiceResponseObject resolveCSVEntity(DataAssetServiceRequestObject requestObject) throws Exception;
    public DataAssetServiceResponseObject resolveCSVEntity(ImporterInfo importerInfo, String user_id)throws Exception;
//    public DataAssetServiceResponseObject getTempTabInfo(DataAssetServiceRequestObject requestObject) throws Exception;
    public void dealCSVEntityByLoadNew(ImporterInfo importerInfo) ;
}