package com.example.testwebsocket.api;

import com.example.testwebsocket.ReceiveMessage;
import com.example.testwebsocket.ResponseMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SubController {

    @Autowired
    public SimpMessagingTemplate template;


    @MessageMapping("/subscribe")
    public void subscribe(ReceiveMessage rm) {
        System.out.println("进入方法");
//        for(int i =1;i<=20;i++) {
            //广播使用convertAndSend方法，第一个参数为目的地，和js中订阅的目的地要一致
            template.convertAndSend("/topic/getResponse", rm.getName());
            System.out.println("/getResponse" +rm.getName());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
//        }

    }

    @MessageMapping("/queue")
    public void queue(ReceiveMessage message) {
        System.out.println("in doSocket");
        String name = message.getName();

        ResponseMessage respon = new ResponseMessage();
        respon.setName(name);
        for (int i=0; i < 20; i++){
            respon.setId(i+"");
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
            template.convertAndSendToUser(name,"/message",respon);
            System.out.println("do doSocket" + i);
        }
        respon.setId("");
        respon.setName("end doSocket...");
        template.convertAndSendToUser(name,"/message",respon);
        System.out.println("out doSocket");
    }

    @RequestMapping("/doSocket")
    public ResponseMessage doSocket(@RequestBody ReceiveMessage message){

        System.out.println("in doSocket");
        String name = message.getName();

        Thread thread = new Thread( () -> {
            ResponseMessage respon = new ResponseMessage();
            respon.setName(name);
                for (int i=0; i < 20; i++){
                    respon.setId(i+"");
                    try {
                        Thread.sleep(1000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    template.convertAndSendToUser(name,"/message",respon);
                    System.out.println("do doSocket" + i);
                }
                respon.setId("");
                respon.setName("end doSocket...");
                template.convertAndSendToUser(name,"/message",respon);
            });
        thread.start();
        System.out.println("out doSocket");
        ResponseMessage respon = new ResponseMessage();
        respon.setName("start doSocket...");
        return respon;
    }


}
