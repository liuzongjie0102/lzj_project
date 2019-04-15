<template>
  <div>
    username:<input id="text" type="text" v-model="username">
    <button @click="initWebsocket()">connect</button>
    <button @click="sendName()">Send</button>
    <button @click="doSocket()" disabled >doSocket</button>
    <button @click="reconnect()" disabled >reconnect</button>
    <button @click="closeStomp()">Close</button>
    <button @click="message = ''">Clean</button>
    <p v-html="message"/>
  </div>
</template>

<script>
import SockJS from  'sockjs-client';
import Stomp from 'stompjs';
import axios from 'axios'

export default {
  name: 'app',
  data () {
    return {
      username: "",
      lockReconnect: false,
      websock:null,
      stompClient:null,
      msg:"",
      message:"",
      timeout: 15*1000,
      startTimer:null,
      closeTimer:null
    }
  },
  methods: {
    errorCallback(){
      // alert("connect error!")
      console.log("connect error!");
      this.reconnect();
    },
    connectCallback(frame){
            console.log('Connected: ' + frame);
            this.message = this.message + "Connected succeed!!!" + "<br>";
            // this.stompClient.subscribe('/topic/getResponse', (response) => {
            //     this.message = this.message + response.body + "<br>";
            // });

            this.stompClient.subscribe('/user/'+this.username+'/message', (frame) => {
              // console.log("response:",frame);
              var body = JSON.parse(frame.body);
              // console.log("body:",body);
              this.message = this.message + body.name + body.id + "<br>";
            });
    },
    initWebsocket(){
      console.log("initWebsocket");

        if(this.username == ""){
          alert("username 不能为空")
          return
        }

        const wsui = "http://10.1.8.6:5006/WebsocketDemo/webServer"
        this.websock = new SockJS(wsui);
        // this.websock = new WebSocket(wsui);

        this.stompClient = Stomp.over(this.websock);
        this.stompClient.heartbeat.outgoing = 15000; 
        this.stompClient.heartbeat.incoming = 15000;
        this.stompClient.connect({username: this.username, password:'123456'}, this.connectCallback, this.errorCallback);
    },
    sendName() {
        this.stompClient.send("/queue", {}, JSON.stringify({ 'name': this.username }));
    },
    closeStomp(){
      this.stompClient.disconnect( () => {
         this.message = this.message + "See you next time!";
      })
    },
    doSocket(){
      axios
      .post("http://localhost:8888/doSocket", {name: this.username})
      .then( res => {
        // console.log("res:",res);
        this.message = this.message + res.data.name + "<br>";
      });

    },
    reconnect(){
      
        if(this.lockReconnect){
          return;
        }
        console.log("reconnect");
        this.lockReconnect = true;
        setTimeout( () => {
            this.initWebsocket();
            this.lockReconnect = false;
        },this.timeout);
    },
  },
  destroyed: function(){
    this.closeStomp();
  }

}
</script>

<style>

</style>
