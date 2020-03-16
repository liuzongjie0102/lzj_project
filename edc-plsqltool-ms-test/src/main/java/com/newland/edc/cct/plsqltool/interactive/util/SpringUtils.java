package com.newland.edc.cct.plsqltool.interactive.util;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.boot.ApplicationHome;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.io.File;

@Component
public class SpringUtils implements ApplicationContextAware {
    private static Logger log = LoggerFactory.getLogger(SpringUtils.class);

    /**
     * 当前工程根路径
     */
    private static String projectRootPath = "";


    public static ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        if(SpringUtils.applicationContext == null) {
            SpringUtils.applicationContext = applicationContext;
        }
//        System.out.println("---------------------------------------------------------------------");
//        System.out.println("========ApplicationContext配置成功,在普通类可以通过调用SpringUtils.getAppContext()获取applicationContext对象,applicationContext="+SpringUtils.applicationContext+"========");
//        System.out.println("---------------------------------------------------------------------");
    }

    //获取applicationContext
    public static ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    //通过name获取 Bean.
    public static Object getBean(String name){
        return getApplicationContext().getBean(name);
    }

    //通过class获取Bean.
    public static <T> T getBean(Class<T> clazz){
        return getApplicationContext().getBean(clazz);
    }

    //通过name,以及Clazz返回指定的Bean
    public static <T> T getBean(String name,Class<T> clazz){
        return getApplicationContext().getBean(name, clazz);
    }


    /**
     * 获得当前工程根目录
     *
     * @return
     */
    public static String getProjectRootPath() {
        String path = "";
        try {
            Resource resource = new ClassPathResource("/application.properties");
            path = resource.getURI().getPath();

            path = path.substring(0,path.lastIndexOf("/"));
        } catch (Exception e) {
            log.error("获取路径失败",e);
        }

        return path;

        //return getProjectRootPath(Application.class);
    }

    /**
     * 获得类所在的工程路径
     *
     * @param c
     * @return
     */
    public static String getProjectRootPath(Class c) {
        if (StringUtils.isEmpty(projectRootPath)) {
            ApplicationHome home = new ApplicationHome(c);
            File jarFile = home.getSource();
            projectRootPath = jarFile.getParentFile().getPath();

            log.info("current root path is：" + projectRootPath);
        }

        return projectRootPath;
    }


    public static String getConfigByKey(String key){
       return applicationContext.getEnvironment().getProperty(key);
    }

}