package com.newland.edc.cct.plsqltool.interactive.thread;


import com.newland.bi.util.logger.BaseLogger;
import com.newland.edc.cct.plsqltool.interactive.service.PLSqlToolQueryService;
import com.newland.edc.cct.plsqltool.interactive.util.DataCatch;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.HashMap;

@Component // 此注解必加
public class InitDataAssetEntity {
    private static BaseLogger log = (BaseLogger) BaseLogger.getLogger(InitDataAssetEntity.class);

    @Resource(name ="com.newland.edc.cct.plsqltool.interactive.service.impl.PLSqlToolQueryServiceImpl")
    private PLSqlToolQueryService plSqlToolQueryService;

//    @Scheduled(cron = "${DataAsset.QuerySubEntityList.Task:0 0 0 * * ?}")
    public void start() {
        if(DataCatch.getFlag()){
            try {
                DataCatch.setVarsMap(new HashMap<>());
//                List<TabTenantBean> tabTenantBeans = plSqlToolQueryService.getTenanetList();
//                if(tabTenantBeans!=null&&tabTenantBeans.size()>0){
                DataCatch.setFlag(false);
//                    for (int i=0;i<tabTenantBeans.size();i++){
//                        String tenant_id = tabTenantBeans.get(i).getTenant_id();
//                        DataAssetEntity dataAssetEntity= plSqlToolQueryService.queryEntityData(null,tenant_id);
//                        DataCatch.getDataAssetEntityMap().put(tenant_id,dataAssetEntity);
//                    }
                DataCatch.setFlag(true);
//                }

            }catch (Exception e){
                log.error(e.getMessage(),e);
                DataCatch.setFlag(true);
                throw e;
            }
        }

    }

}
