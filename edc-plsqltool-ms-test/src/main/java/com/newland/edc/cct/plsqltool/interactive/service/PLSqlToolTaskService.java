package com.newland.edc.cct.plsqltool.interactive.service;

import com.newland.edc.cct.plsqltool.interactive.model.javabean.*;

public interface PLSqlToolTaskService {

    /**
     * 异步执行适配器
     * @param testGrammarBean
     * @throws Exception
     */
    public void executeAdapter(TestGrammarBean testGrammarBean) throws Exception;

    /**
     * 异步执行
     * @param testGrammarBean
     * @throws Exception
     */
    public void asyexecute(TestGrammarBean testGrammarBean) throws Exception;

    /**
     * 同步执行
     * @param testSqlBean
     * @return
     * @throws Exception
     */
    public DgwSqlToolResult synexecute(TestSqlBean testSqlBean) throws Exception;

    /**
     * 创建导出任务
     * @param exportLogBean
     * @throws Exception
     */
    public void createExportTask(ExportLog exportLogBean) throws Exception;

    /**
     * IOP同步任务
     * @param ftpFileSynchronousReq
     * @return
     * @throws Exception
     */
    public FtpSynchronousLog doFtpFileSynchronous(FtpFileSynchronousReq ftpFileSynchronousReq) throws Exception;

    /**
     * 删除导出文件
     * @param exportLogReq
     * @throws Exception
     */
    public void deleteExportTask(ExportLogReq exportLogReq) throws Exception;
}
