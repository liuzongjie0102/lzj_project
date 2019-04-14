package com.lzj.conf;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.socket.config.annotation.AbstractWebSocketMessageBrokerConfigurer;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;

@Configuration
@EnableWebMvc
@EnableWebSocketMessageBroker
public class WebSocketConfig extends AbstractWebSocketMessageBrokerConfigurer {

//    @Override
//    public void configureMessageBroker(MessageBrokerRegistry config) {
//        ThreadPoolTaskScheduler te = new ThreadPoolTaskScheduler();
//        te.setPoolSize(1);
//        te.setThreadNamePrefix("wss-heartbeat-thread-");
//        te.initialize();
//        config.enableSimpleBroker("/topic","/user").setHeartbeatValue(new long[]{15000,15000}).setTaskScheduler(te);
////        config.enableSimpleBroker("/topic","/user");//topic用来广播，user用来实现p2p
//    }


    public void registerStompEndpoints(StompEndpointRegistry stompEndpointRegistry) {
        stompEndpointRegistry.addEndpoint("/webServer").setAllowedOrigins("*").withSockJS();

    }

    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry)
    {

        //表明在topic、queue、users这三个域上可以向客户端发消息。
        registry.enableSimpleBroker("/topic","/queue","/user");
        //客户端向服务端发起请求时，需要以/app为前缀。
//        registry.setApplicationDestinationPrefixes("/app");
        //给指定用户发送一对一的消息前缀是/users/。
//        registry.setUserDestinationPrefix("/users/");
    }
}
