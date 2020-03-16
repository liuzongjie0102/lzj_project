package com.newland.edc.cct.plsqltool.interactive.common;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
//@ConfigurationProperties(prefix = "resaddress")
/**
 * 获取项目部署地址的配置信息
 */
public class ResAddress {
    @Value("${resaddress.version}")
    private String version;
    public String getVersion(){
        return this.version;
    }
    public void setVersion(String version){
        this.version = version;
    }
}
