package com.newland.edc.cct.plsqltool.interactive.tool;

import com.newland.bi.util.logger.BaseLogger;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.DgwSqlToolResult;
import com.newland.edc.cct.plsqltool.interactive.util.DataSourceAccess;

public class DgwBaseHiveCell extends DgwBaseCellTemplate{
    private static BaseLogger log = (BaseLogger) BaseLogger.getLogger(DgwBaseHiveCell.class);

    @Override
    protected DgwSqlToolResult execute() throws Exception {
        DgwSqlToolResult result = new DgwSqlToolResult();
        try {
            conn = DataSourceAccess.getConnByConnId(conn_id);
            if(execute_type.equalsIgnoreCase("SELECT")){
                //hive提取去数量限制1000条  没有字段信息无法用函数分页
                sql = "SELECT * FROM ("+ sql +") v_alias_12 limit 1000";
            }
            result = this.commonDeal();
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }finally {
            closeAll(conn,ps,rs,rsmd);
        }
        return result;
    }
}
