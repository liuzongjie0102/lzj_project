package com.newland.edc.cct.plsqltool.interactive.tool;


import com.newland.edc.cct.plsqltool.interactive.model.javabean.DgwSqlToolResult;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.ExecuteRequestBean;

public abstract class DgwBaseTemplate {

    public abstract DgwSqlToolResult run(ExecuteRequestBean bean) throws Exception;

    protected abstract DgwSqlToolResult execute() throws Exception;

    public abstract DgwSqlToolResult commonDeal() throws Exception;
}
