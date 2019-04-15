<template>
  <div>
    <input id="text" type="text" v-model="msg">
    <button @click="websocketsend(msg)">Send</button>
    <button @click="initWebsocket()">connect</button>
    <button @click="reconnect()">reconnect</button>
    <button @click="closewebsocket()">Close</button>
    <p v-html="message"/>
  </div>
</template>

<script>
export default {
  name: 'app',
  data () {
    return {
      lockReconnect: false,
      websock:null,
      msg:"",
      message:"",
      timeout: 15*1000,
      startTimer:null,
      closeTimer:null
    }
  },
  methods: {
    initWebsocket(){
      console.log("initWebsocket");

      try{
        const wsui = "http:/localhost:8888/websocket"
        // this.websock = new SockJS()
        this.websock = new WebSocket(wsui);

        this.websock.onopen = this.websocketonopen;
        this.websock.onerror = this.websocketonerror;
        this.websock.onclose = this.websocketclose;
        this.websock.onmessage = this.websocketonmessage;
      }catch(e){
        console.log("catch initWebsocket error");
        this.reconnect();
      }
      
    },
    closewebsocket(){
        this.websock.close();
    },
    websocketonopen() {//连接成功事件
        console.log("WebSocket连接成功");
        this.start();
    },
    websocketonerror(e) {//连接失败事件
        console.log("WebSocket连接发生错误");
        this.reconnect();
    },
    websocketclose(e) {//连接关闭事件
        this.message = this.message + "connection closed (" + e.code + ")" + "<br>";
        this.reconnect();
    },
    websocketonmessage(event) {//接收服务器推送的信息
        this.message = this.message + "message:" + event.data + "<br>";
        this.reset();
    },
    websocketsend(msg) {//向服务器发送信息
        this.websock.send(msg);
        this.msg = "";
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
    reset(){
      clearTimeout(this.startTimer);
      clearTimeout(this.closeTimer);
      this.startTimer = setTimeout( () => {
          this.websocketsend("pa");
          this.closeTimer = setTimeout(() => {
            this.websock.close();
          },this.timeout);
        }, this.timeout);
    },
    start(){
        this.startTimer = setTimeout( () => {
          this.websocketsend("pa");
          this.closeTimer = setTimeout(() => {
            this.websock.close();
          },this.timeout);
        }, this.timeout);
    }
  },
  created(){
    this.initWebsocket();
  }
}
</script>

<style>

</style>
