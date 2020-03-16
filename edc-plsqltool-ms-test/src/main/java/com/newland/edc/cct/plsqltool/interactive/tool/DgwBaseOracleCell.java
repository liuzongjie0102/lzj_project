package com.newland.edc.cct.plsqltool.interactive.tool;

import com.newland.bi.util.logger.BaseLogger;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.DgwSqlToolResult;
import com.newland.edc.cct.plsqltool.interactive.util.DataSourceAccess;

public class DgwBaseOracleCell extends DgwBaseCellTemplate {
    private static BaseLogger log = (BaseLogger) BaseLogger.getLogger(DgwBaseOracleCell.class);

    @Override
    protected DgwSqlToolResult execute() throws Exception {
        DgwSqlToolResult result = new DgwSqlToolResult();
        try {
            conn = DataSourceAccess.getConnByConnId(conn_id);

            if(execute_type.equalsIgnoreCase("SELECT")){
                //查询语句根据报文请求做分页查询，参数补全情况默认查询前1000条记录
                if(!start_page.equals("")&&!page_num.equals("")){
//                    long count = 0l;
//                    ps = conn.prepareStatement("select count(1) from ("+sql+")");
//                    rs = ps.executeQuery();
//                    while(rs.next()){
//                        count = rs.getLong("1");
//                    }
//                    if(count>0l){
//                        long maxpage = count/Long.parseLong(page_num) +1 ;
                    sql = "select * from (select rownum rownum_,t1.* from ("+sql+") t1) t2 where t2.rownum_>"+((Integer.parseInt(start_page)-1)*Integer.parseInt(page_num))+" and t2.rownum_<="+(Integer.parseInt(start_page)*Integer.parseInt(page_num));
//                    }
                }else{
                    sql = "select * from (select rownum rownum_,t1.* from ("+sql+") t1) t2 where t2.rownum_>=1 and t2.rownum_<=1000";
                }
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
