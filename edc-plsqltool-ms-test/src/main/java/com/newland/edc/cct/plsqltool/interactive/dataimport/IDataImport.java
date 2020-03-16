package com.newland.edc.cct.plsqltool.interactive.dataimport;

import com.newland.edc.cct.plsqltool.interactive.model.javabean.ImportResponseInfo;

public interface IDataImport {
    void initParam() throws Exception;

    ImportResponseInfo importDataToDb();

    void removeTemp() throws Exception;
}

