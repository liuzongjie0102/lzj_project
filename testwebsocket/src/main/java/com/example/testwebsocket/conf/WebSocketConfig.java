package com.example.testwebsocket.conf;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.socket.config.annotation.AbstractWebSocketMessageBrokerConfigurer;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;

@Configuration
@EnableWebSocketMessageBroker //注解开启STOMP协议来传输基于代理的消息，此时控制器支持使用
@MessageMapping
public class WebSocketConfig extends AbstractWebSocketMessageBrokerConfigurer {

    @Override
    public void configureMessageBroker(MessageBrokerRegistry config) {
        ThreadPoolTaskScheduler te = new ThreadPoolTaskScheduler();
        te.setPoolSize(1);
        te.setThreadNamePrefix("wss-heartbeat-thread-");
        te.initialize();
        config.enableSimpleBroker("/topic","/user").setHeartbeatValue(new long[]{15000,15000}).setTaskScheduler(te);
//        config.enableSimpleBroker("/topic","/user");//topic用来广播，user用来实现p2p
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/webServer").setAllowedOrigins("*").withSockJS();
        registry.addEndpoint("/queueServer").setAllowedOrigins("*").withSockJS();//注册两个STOMP的endpoint，分别用于广播和点对点
    }

}
