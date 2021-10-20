import org.springframework.aop.framework.Advised;
import org.springframework.aop.framework.ProxyFactory;
import service.UserService;
import service.UserServiceImpl;
import service.advised.ServerLogPlugin;

public class app {
    public static void main(String[] args){

        UserServiceImpl user = new UserServiceImpl();
        ProxyFactory pf = new ProxyFactory(user);

        UserService service = (UserService) pf.getProxy();
        Advised advised = (Advised)service;
        advised.addAdvice(new ServerLogPlugin());
        service.getUser(123, "lzj");

    }
}
