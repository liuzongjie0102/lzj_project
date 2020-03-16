package com.newland.edc.cct.plsqltool.interactive.service.impl;

import com.newland.bi.util.logger.BaseLogger;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.*;
import com.newland.edc.cct.plsqltool.interactive.service.PLSqlToolExecuteService;
import com.newland.edc.cct.plsqltool.interactive.tool.*;
import com.newland.edc.cct.plsqltool.interactive.util.DataCatch;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository("com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolExecuteServiceImpl")
public class PLSqlToolExecuteServiceImpl implements PLSqlToolExecuteService {

    private static BaseLogger log = (BaseLogger) BaseLogger.getLogger(PLSqlToolExecuteServiceImpl.class);

    @Override
    public DgwSqlToolResult executeSql(ExecuteRequestBean bean) throws Exception{
        DgwSqlToolResult result;
        try {
            result = access(bean);
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
        return result;
    }

    public DgwSqlToolResult access(ExecuteRequestBean bean)throws Exception{
        DgwSqlToolResult result = new DgwSqlToolResult();
        if(bean.getDbType().isEmpty()){
            result.setErr_msg("数据库类型为空");
            result.setIs_success(false);
            return result;
        }
        if(bean.getConnId().isEmpty()){
            result.setErr_msg("连接ID为空");
            result.setIs_success(false);
            return result;
        }
        if(bean.getSql().isEmpty()){
            result.setErr_msg("执行内容为空");
            result.setIs_success(false);
            return result;
        }
        if(bean.getDbType().equalsIgnoreCase("HIVE")){
            DgwBaseHiveCell cell = new DgwBaseHiveCell();
            result = cell.run(bean);
        }else if(bean.getDbType().equalsIgnoreCase("ORACLE")){
            DgwBaseOracleCell cell = new DgwBaseOracleCell();
            result = cell.run(bean);
        }
        return result;

    }

    @Override
    public List<ExplainInfo> executeExplain(List<TestSqlBean> testSqlBeans,String conn_id) throws Exception{
        ExplainTask explainTask = new ExplainTask(testSqlBeans,conn_id);
        List<ExplainInfo> explainInfos = explainTask.executeExplain();
        return explainInfos;
    }

    @Override
    public void closeExecute(TestSqlBean testSqlBean) throws Exception{
        if(StringUtils.isBlank(testSqlBean.getGroup_id())){
            throw new Exception("执行失败，任务组id为空");
        }
        if(StringUtils.isBlank(testSqlBean.getTask_id())){
            throw new Exception("执行失败，任务id为空");
        }
        if(StringUtils.isBlank(testSqlBean.getDbType())){
            throw new Exception("执行失败，数据库类型为空");
        }
        try {
            if(testSqlBean.getDbType().equals("hive")){
                if(DataCatch.getPsMap().get(testSqlBean.getTask_id())!=null){
                    DataCatch.getPsMap().get(testSqlBean.getTask_id()).cancel();
                }else{
                    //该statement可能执行完成被关闭
                    throw new Exception("该statement可能执行完成或被关闭");
                }
            }else{
                if(DataCatch.getConMap().get(testSqlBean.getGroup_id())!=null){
                    DataCatch.getConMap().get(testSqlBean.getGroup_id()).close();
                }else{
                    throw new Exception("该connection可能执行完成或被关闭");
                }
            }
        }catch (Exception e){
            log.error(e.getMessage(),e);
            throw e;
        }
    }

    @Override
    public void dropExecute(TemporaryReq temporaryReq) throws Exception{
        DropTableTask dropTableTask = new DropTableTask(temporaryReq.getConn_id(),temporaryReq.getDb_type(),temporaryReq.getTab_name());
        dropTableTask.executeDrop();
    }

    @Override
    public long sqlCountTask(String sqlString,String conn_id,String db_type) throws Exception{
        SqlCountTask sqlCountTask = new SqlCountTask(sqlString,conn_id,db_type);
        return sqlCountTask.execute();
    }

}
