package com.newland.edc.cct.plsqltool.interactive.inf;

import com.newland.bd.ms.core.properties.WsProperties;
import com.newland.edc.cct.dataasset.dispose.service.IDataAssetEntityDisposeService;
import com.newland.edc.cct.dataasset.entity.service.IDataAssetEntityService;
import com.newland.edc.cct.dataasset.subscribe.service.IDataAssetSubscribeService;
import com.newland.edc.cct.dataasset.tenantres.service.ITenantResourceService;
import com.newland.edc.cct.dgw.utils.rest.RestFulServiceUtils;
import com.newland.edc.cct.frame.soap.service.ICctAuthService;
import org.apache.cxf.jaxws.JaxWsProxyFactoryBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.env.Environment;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class WsConfig {
    private static final Logger logger = LoggerFactory.getLogger(WsConfig.class);

    @Resource
    private WsProperties wsProperties;


    String IDataAssetEntityService_url = RestFulServiceUtils.getServiceUrl("com.newland.edc.cct.dataasset.entity.service.IDataAssetEntityService");

    String IDataAssetSubscribeService_url = RestFulServiceUtils.getServiceUrl("com.newland.edc.cct.dataasset.entity.service.IDataAssetSubscribeService");

    String IDataAssetEntityDisposeService_url = RestFulServiceUtils.getServiceUrl("com.newland.edc.cct.dataasset.dispose.service.IDataAssetEntityDisposeService");

    String ITenantResourceService_url = RestFulServiceUtils.getServiceUrl("com.newland.edc.cct.dataasset.tenantres.service.ITenantResourceService");

    String ICctAuthService_url = RestFulServiceUtils.getServiceUrl("com.newland.edc.cct.frame.soap.service.ICctAuthService");
    @Autowired
    public Environment env;//当前环境的application.properties的 配置
    /**
     * 返回 访问webservice代理工厂
     *
     * @return 访问webservice代理工厂
     */
    @Bean
    @Lazy
    public JaxWsProxyFactoryBean entityWsProxy() {
        JaxWsProxyFactoryBean wsProxy = new JaxWsProxyFactoryBean();
        wsProxy.setServiceClass(IDataAssetEntityService.class);
        wsProxy.setAddress(IDataAssetEntityService_url);

        return wsProxy;
    }
    @Bean
    public JaxWsProxyFactoryBean subscribeWsProxy() {
        JaxWsProxyFactoryBean wsProxy = new JaxWsProxyFactoryBean();
        wsProxy.setServiceClass(IDataAssetSubscribeService.class);
        wsProxy.setAddress(IDataAssetSubscribeService_url);

        return wsProxy;
    }
    @Bean
    public JaxWsProxyFactoryBean disposeWsProxy() {
        JaxWsProxyFactoryBean wsProxy = new JaxWsProxyFactoryBean();
        wsProxy.setServiceClass(IDataAssetEntityDisposeService.class);
        wsProxy.setAddress(IDataAssetEntityDisposeService_url);

        return wsProxy;
    }
    @Bean
    public JaxWsProxyFactoryBean resourceWsProxy(){
        JaxWsProxyFactoryBean wsProxy = new JaxWsProxyFactoryBean();
        wsProxy.setServiceClass(ITenantResourceService.class);
        wsProxy.setAddress(ITenantResourceService_url);

        return wsProxy;
    }
    @Bean
    public JaxWsProxyFactoryBean authWsProxy(){
        JaxWsProxyFactoryBean wsProxy = new JaxWsProxyFactoryBean();
        wsProxy.setServiceClass(ICctAuthService.class);
        wsProxy.setAddress(ICctAuthService_url);

        return wsProxy;
    }

    /**
     * 实体信息
     * @return
     */
    @Bean
    @Lazy
    public IDataAssetEntityService getTabInfoList(@Qualifier("entityWsProxy")JaxWsProxyFactoryBean entityWsProxy)
    {
        this.ignoreParameterConf(entityWsProxy);
        IDataAssetEntityService service=(IDataAssetEntityService) entityWsProxy.create();
        return service;
    }
    @Bean
    public IDataAssetEntityDisposeService getEntityDisposeCfg(@Qualifier("disposeWsProxy")JaxWsProxyFactoryBean entityWsProxy)
    {
        this.ignoreParameterConf(entityWsProxy);
        IDataAssetEntityDisposeService service=(IDataAssetEntityDisposeService) entityWsProxy.create();
        return service;
    }
    @Bean
    public IDataAssetSubscribeService querySubEntityList(@Qualifier("subscribeWsProxy")JaxWsProxyFactoryBean entityWsProxy){
        this.ignoreParameterConf(entityWsProxy);
        IDataAssetSubscribeService service = (IDataAssetSubscribeService) entityWsProxy.create();
        return service;
    }
    @Bean
    public ITenantResourceService getTenantResourceList(@Qualifier("resourceWsProxy")JaxWsProxyFactoryBean entityWsProxy){
        this.ignoreParameterConf(entityWsProxy);
        ITenantResourceService service = (ITenantResourceService) entityWsProxy.create();
        return service;
    }
    @Bean
    public ICctAuthService qryUserTenantList(@Qualifier("authWsProxy")JaxWsProxyFactoryBean entityWsProxy){
        this.ignoreParameterConf(entityWsProxy);
        ICctAuthService service = (ICctAuthService) entityWsProxy.create();
        return service;
    }

    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        // TODO Auto-generated method stub
        env=applicationContext.getEnvironment();
    }

    /**
     * 忽略参数不匹配问题
     * @param proxy
     */
    private void ignoreParameterConf(JaxWsProxyFactoryBean proxy) {

        Map<String, Object> properties = proxy.getProperties();
        if (properties == null) {
            properties = new HashMap<>();
            proxy.setProperties(properties);
        }
        properties.put("set-jaxb-validation-event-handler", Boolean.FALSE);
        logger.info

                        ("====registerSoapService set-jaxb-validation-event-handler   false====");


    }
}
