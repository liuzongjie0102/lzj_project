package com.lzj.test;

import com.alibaba.fastjson.JSON;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TestMain {

    public List<String> getParam() throws Exception{

        Properties prop = new Properties();
//        InputStream in = new FileInputStream("./testConfig.properties");
//        System.out.println(this.getClass().getResource("/").getPath());
        InputStream in = this.getClass().getResourceAsStream("/testConfig.properties");
        prop.load(in);
        List<String> list = new ArrayList<>();
        list.add(prop.getProperty("jsonParam1"));
        list.add(prop.getProperty("jsonParam2"));
        list.add(prop.getProperty("jsonParam3"));

        return list;
    }


    public static void main(String[] args) throws Exception {

        List<String> list = new TestMain().getParam();

        new Thread( () -> {
            while (true){
                for (String s : list) {
                    send(s);
                }
                try {
                    System.out.println("----- " + Thread.currentThread().getName() +" sleep 30s -----");
                    Thread.sleep(30000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread( () -> {
            while (true){
                for (String s : list) {
                    send(s);
                }
                try {
                    System.out.println("----- " + Thread.currentThread().getName() +" sleep 30s -----");
                    Thread.sleep(30000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

    }

    public static void send(String jsonParam){
        try {
            System.out.println("----" + Thread.currentThread().getName() + "send ----");
            HttpClient httpClient = HttpClients.createDefault();
            HttpPost httpPost = new HttpPost("http://localhost:52213/edc-plsqltool-ms-test/v1/querydata/testGrammar");
            Header header=new BasicHeader("Accept-Encoding",null);
            httpPost.setHeader(header);

            // 设置报文和通讯格式
            StringEntity stringEntity = new StringEntity(jsonParam,"UTF-8");
            stringEntity.setContentEncoding("UTF-8");
            stringEntity.setContentType("application/json");
            httpPost.setEntity(stringEntity);

            HttpResponse httpResponse = httpClient.execute(httpPost);
            HttpEntity httpEntity= httpResponse.getEntity();
            String result = EntityUtils.toString(httpEntity);

            Map respData = (Map<String, Object>)JSON.parseObject(result, Map.class).get("respData");
            Map object = JSON.parseArray(respData.get("testSqlBeans").toString(), Map.class).get(0);

            object.put("dbType", respData.get("dbType"));
            object.put("conn_id", respData.get("conn_id"));
            object.put("resource_id", respData.get("resource_id"));
            object.put("tenant_id", respData.get("tenant_id"));
            object.put("user_id", "999999");

//            System.out.println(object.toString());

            HttpPost httpPost2 = new HttpPost("http://localhost:52213/edc-plsqltool-ms-test/v1/querydata/synexecute");
            Header header2=new BasicHeader("Accept-Encoding",null);
            httpPost2.setHeader(header2);

            // 设置报文和通讯格式
            StringEntity stringEntity2 = new StringEntity(JSON.toJSONString(object),"UTF-8");
            stringEntity2.setContentEncoding("UTF-8");
            stringEntity2.setContentType("application/json");
            httpPost2.setEntity(stringEntity2);

            httpResponse = httpClient.execute(httpPost2);
            httpEntity= httpResponse.getEntity();
            result = EntityUtils.toString(httpEntity);

//            System.out.println("result:" + result);
            Map<String, Object> resMap = JSON.parseObject(result, Map.class);
            if (resMap.get("respResult").equals("1")){
                System.out.println("-----------------succeed--------------------");
            }else {
                System.out.println("-----------------fail--------------------" + resMap.get("respErrorMsg"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
