package com.newland.edc.cct.plsqltool.interactive.tool;

import com.newland.bi.util.logger.BaseLogger;
import com.newland.edc.cct.plsqltool.interactive.model.javabean.DgwSqlToolResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class ConcurrentExcutor {
    private static BaseLogger log = (BaseLogger) BaseLogger.getLogger(ConcurrentExcutor.class);

    private int taskPool = 0;
    private ExecutorService es = null;
    private static Map<String,Future<Object>> futureMap = new HashMap<>();

    public ConcurrentExcutor(){
        taskPool = 20;
        es = Executors.newFixedThreadPool(taskPool);
    }

    public ConcurrentExcutor(int pool_size){
        taskPool = pool_size;
        es = Executors.newFixedThreadPool(taskPool);
    }

    public int getTaskPool() {
        return taskPool;
    }

    public void setTaskPool(int taskPool) {
        this.taskPool = taskPool;
    }

    public int getAliveTask() {
        return ((ThreadPoolExecutor)es).getActiveCount();
    }

    public void execute(CallableTask callableTask){
        Future future = es.submit(callableTask);
        futureMap.put(callableTask.getExecute_group(),future);
    }

    public void execute(FtpFileSynchronousTask ftpFileSynchronousTask){
        Future future = es.submit(ftpFileSynchronousTask);
    }
    /**
     * 提取任务结果，限定提取时限
     * @param taskName
     * @param second
     * @return
     * @throws Exception
     */
    public Object getResult(String taskName,int second) throws Exception{
        if(futureMap.get(taskName)!=null){
            log.info("正在提取任务id："+taskName+"");
            try {
                if(futureMap.get(taskName).get(second,TimeUnit.SECONDS)!=null){
                    log.info("任务id："+taskName+"已执行完成");
                    Object result = futureMap.get(taskName).get();
                    futureMap.remove(taskName);
                    return result;
                }
            }catch (Exception e){
                log.info(e.getMessage(),e);
                throw e;
            }
            return null;
        }else{
            log.info("任务id："+taskName+"在队列中无法查到");
            List<DgwSqlToolResult> results = new ArrayList<>();
            DgwSqlToolResult result = new DgwSqlToolResult();
            result.setIs_success(false);
            result.setErr_msg("任务id："+taskName+"在队列中无法查到");
            results.add(result);
            return results;
        }
    }

    /**
     * 提取任务结果
     * @param taskName
     * @return
     * @throws Exception
     */
    public Object getResult(String taskName) throws Exception{
        if(futureMap.get(taskName)!=null){
            log.info("正在提取任务id："+taskName+"");
            try {
                if(futureMap.get(taskName).get()!=null){
                    log.info("任务id："+taskName+"已执行完成");
                    Object result = futureMap.get(taskName).get();
                    futureMap.remove(taskName);
                    return result;
                }
            }catch (Exception e){
                log.info(e.getMessage(),e);
                throw e;
            }
            return null;
        }else{
            log.info("任务id："+taskName+"在队列中无法查到");
            List<DgwSqlToolResult> results = new ArrayList<>();
            DgwSqlToolResult result = new DgwSqlToolResult();
            result.setIs_success(false);
            result.setErr_msg("任务id："+taskName+"在队列中无法查到");
            results.add(result);
            return results;
        }
    }
}
