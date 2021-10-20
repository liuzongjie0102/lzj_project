package service.advised;

import org.springframework.aop.MethodBeforeAdvice;

import java.lang.reflect.Method;
import java.util.Arrays;

public class ServerLogPlugin implements MethodBeforeAdvice {

    public void before(Method method, Object[] objects, Object o) throws Throwable {
        String result = String.format("%s.%s() 参数:%s",method.getDeclaringClass().getName(),method.getName(), Arrays.toString(objects));
        System.out.println(result);
    }
}
