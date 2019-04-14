package com.example.testwebsocket.api;

import org.springframework.stereotype.Component;

import javax.websocket.*;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;


//@ServerEndpoint( value = "/websocket" , encoders = {})
//@Component
public class MySocket {

    private static AtomicInteger onlineCount = new AtomicInteger(0);

    private static CopyOnWriteArraySet<MySocket> webSocketSet = new CopyOnWriteArraySet<>();

    private Session session;

    @OnOpen
    public void onOpen(Session session){
        this.session = session;
        webSocketSet.add(this);
        System.out.println("来自客户端的连接,当前在线人数为" + onlineCount.incrementAndGet());
    }

    @OnClose
    public void onClose() {
        webSocketSet.remove(this);  //从set中删除
        System.out.println("有一连接关闭！当前在线人数为" + onlineCount.decrementAndGet());
    }

    @OnMessage
    public void onMessage(String message, Session session){
//        System.out.println("来自客户端的消息:" + message);

        for (MySocket socket : webSocketSet  ) {
            try {
                socket.sendMessage(message);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @OnError
    public void onError(Session session,Throwable error){
        System.out.println("发生错误");
        error.printStackTrace();
    }

    public void sendMessage(String message) throws EncodeException, IOException {
        this.session.getBasicRemote().sendText(message);

    }

    public void sendObj(Object obj) throws EncodeException, IOException {
        this.session.getBasicRemote().sendObject(obj);
    }
    class msgObj {
        int flag;
        String message;

        public int getFlag() {
            return flag;
        }

        public void setFlag(int flag) {
            this.flag = flag;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

    }

    class EncoderServer {

    }
}
