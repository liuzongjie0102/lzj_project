package com.newland.edc.cct.plsqltool.interactive.util;

import java.lang.reflect.Method;

public class ClassUtil {

    /**
     * 调用映射方法1
     * @param obj
     * @param methodName
     * @param args
     * @return
     * @throws Exception
     */
    public static Object invoke(Object obj, String methodName, Object ... args) throws Exception{
        Class [] parameterTypes = new Class[args.length];
        for(int i = 0; i < args.length; i++){
            parameterTypes[i] = args[i].getClass();
        }
        Method method = obj.getClass().getDeclaredMethod(methodName, parameterTypes);
        return method.invoke(obj, args);
    }

    /**
     * 调用映射方法2
     * @param obj
     * @param methodName
     * @return
     * @throws Exception
     */
    public static Object invoke(Object obj, String methodName) throws Exception{
        Method method = obj.getClass().getDeclaredMethod(methodName);
        return method.invoke(obj);
    }


    /**
     * 首字母大写
     * @param fildeName
     * @return
     * @throws Exception
     */
    public static String getMethodName(String fildeName) {
        byte[] items = fildeName.getBytes();
        items[0] = (byte) ((char) items[0] - 'a' + 'A');
        return new String(items);
    }
}
