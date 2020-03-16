package com.newland.edc.cct.plsqltool.interactive.service;

import com.newland.bd.ms.core.model.RespInfo;
import com.newland.bd.workflow.sql.bean.DbType;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.TemporaryTabInfo;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.TestGrammarBean;

public interface PLSqlToolTestService {

    /**
     * 语法校验
     * @param testGrammarBean
     * @param dbType
     * @return
     */
    public TestGrammarBean testGrammar(TestGrammarBean testGrammarBean, DbType dbType);

    /**
     * 获取分区字段（hive）
     * @param sql
     * @param temporaryTabInfo
     */
    public void getPartitionCol(String sql, TemporaryTabInfo temporaryTabInfo);

    public String transExecuteType(String execute);

    public RespInfo grammarAdapter(TestGrammarBean testGrammarBean);
}
