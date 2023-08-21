package main

import (
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

func publishTestMessage(client mqtt.Client, i int) {
	client.Publish("test_topic_mqtt_receiver", 0, false, "Message: "+strconv.Itoa(i))
}

var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	fmt.Printf("Received message: %s from topic: %s\n", msg.Payload(), msg.Topic())
	if msg.Topic() == "test_trigger_mqtt" && string(msg.Payload()[:]) == "start" {
		for i := 1; i <= 10000; i++ {
			go publishTestMessage(client, i)
		}
	}
}

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	fmt.Println("Connected")
	client.Subscribe("test_trigger_mqtt", 2, messagePubHandler)
}

var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	fmt.Printf("Connect lost: %v", err)
}

func main() {
	keepAlive := make(chan os.Signal)
	signal.Notify(keepAlive, os.Interrupt, syscall.SIGTERM)

	var broker = "192.168.0.153"
	var port = 1883
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port))
	opts.SetPingTimeout(1000)
	opts.SetClientID("go_mqtt_client_1")
	opts.SetUsername("mosquitto_1")
	opts.SetPassword("public")
	opts.SetDefaultPublishHandler(messagePubHandler)
	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	<-keepAlive
}
