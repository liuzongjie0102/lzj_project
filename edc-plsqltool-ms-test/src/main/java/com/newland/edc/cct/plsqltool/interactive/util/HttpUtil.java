package com.newland.edc.cct.plsqltool.interactive.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.newland.bi.util.logger.BaseLogger;

import java.io.*;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URL;

public class HttpUtil {

    private static BaseLogger log = (BaseLogger) BaseLogger.getLogger(HttpUtil.class);

    public static String getReqBody(InputStream ins) {
        String line = null;
        StringBuffer data = new StringBuffer();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(ins,"UTF-8"));
            while((line=reader.readLine())!=null) {
                data.append(line);
            }
        } catch (IOException e) {
            log.error(e.getMessage(),e);
        } finally {
            if(reader!=null)
                try {
                    reader.close();
                } catch (IOException e) {
                    log.error(e.getMessage(),e);
                }
        }
        return data.toString();
    }

    /**
     * 发起http请求并获取结果
     *
     * @param requestUrl 请求地址
     * @param requestMethod 请求方式（GET、POST）
     * @param outputStr 提交的数据
     * @return JSONObject(通过JSONObject.get(key)的方式获取json对象的属性值)
     */
    public static JSONObject httpRequest(String requestUrl, String requestMethod, String outputStr) {
        JSONObject jsonObject = null;
        StringBuffer buffer = new StringBuffer();
        InputStream inputStream=null;
        try {
            URL url = new URL(requestUrl);
            HttpURLConnection httpUrlConn = (HttpURLConnection) url.openConnection();
            httpUrlConn.setDoOutput(true);
            httpUrlConn.setDoInput(true);
            httpUrlConn.setUseCaches(false);
            httpUrlConn.setRequestProperty("Content-type", "application/json");
            // 设置请求方式（GET/POST）
            httpUrlConn.setRequestMethod(requestMethod);
            if ("GET".equalsIgnoreCase(requestMethod))
                httpUrlConn.connect();

            // 当有数据需要提交时
            if (null != outputStr) {
                OutputStream outputStream = httpUrlConn.getOutputStream();
                // 注意编码格式，防止中文乱码
                outputStream.write(outputStr.getBytes("UTF-8"));
                outputStream.close();
            }
            //将返回的输入流转换成字符串
            inputStream = httpUrlConn.getInputStream();
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream, "utf-8");
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

            String str = null;
            while ((str = bufferedReader.readLine()) != null) {
                buffer.append(str);
            }
            bufferedReader.close();
            inputStreamReader.close();
            // 释放资源
            inputStream.close();
            inputStream = null;
            httpUrlConn.disconnect();
//            System.out.println(buffer.toString());
            jsonObject = JSON.parseObject(buffer.toString());
        } catch (ConnectException ce) {
            log.error(ce.getMessage(),ce);
            System.out.println("Server connection timed out");
        } catch (Exception e) {
            log.error(e.getMessage(),e);
            System.out.println("http request error:{}");
        }finally{
            try {
                if(inputStream!=null){
                    inputStream.close();
                }
            } catch (IOException e) {
                // TODO Auto-generated catch block
                log.error(e.getMessage(),e);
            }
        }
        return jsonObject;
    }

    /**
     * Get 请求restful服务
     * @param requestUrl
     * @return
     */
    public static String httpRequest(String requestUrl){
        StringBuffer buffer = new StringBuffer();
        InputStream inputStream=null;
        try {
            URL url = new URL(requestUrl);
            HttpURLConnection httpUrlConn = (HttpURLConnection) url.openConnection();
            httpUrlConn.setDoOutput(true);
            httpUrlConn.setDoInput(true);
            httpUrlConn.setUseCaches(false);
            httpUrlConn.setRequestProperty("Content-type", "application/json");
            // 设置请求方式（GET/POST）
            httpUrlConn.setRequestMethod("GET");
            httpUrlConn.connect();

            //将返回的输入流转换成字符串
            inputStream = httpUrlConn.getInputStream();
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream, "utf-8");
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String str = null;
            while ((str = bufferedReader.readLine()) != null) {
                buffer.append(str);
            }
            bufferedReader.close();
            inputStreamReader.close();
            // 释放资源
            inputStream.close();
            inputStream = null;
            httpUrlConn.disconnect();
        } catch (ConnectException ce) {
            log.error(ce.getMessage(),ce);
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }finally{
            try {
                if(inputStream!=null){
                    inputStream.close();
                }
            } catch (IOException e) {
                // TODO Auto-generated catch block
                log.error(e.getMessage(),e);
            }
        }
        return buffer.toString();
    }


}
