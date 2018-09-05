package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/streadway/amqp"
)

var (
	uri             = flag.String("uri", "", "AMQP URI (Mandatory). Eg.: guest:guest@localhost:5672/vhost ")
	mqttBroker      = flag.String("mqttBroker", "", "MQTT broker URI (Mandatory). Eg: 192.168.1.1:1883")
	reliable        = flag.Bool("reliable", true, "Wait for the publisher confirmation before exiting")
	cacertpath      = flag.String("cacert", "", "Path to the Root CA certificate")
	clcertpath      = flag.String("clcert", "", "Path to the client certificate")
	clkeypath       = flag.String("clkey", "", "Path to the client key")
	pubConnTopic    = flag.String("connTopic", "", "MQTT topic used to publish connection status changes (Mandatory). Eg: publisher/conn")
	subDataTopic    = flag.String("dataTopic", "", "MQTT topic used to subscribe to data other services want to publish (Mandatory). Eg: publisher/data")
	pubStorageTopic = flag.String("storageTopic", "", "MQTT topic used to subscribe to data other services want to publish (Mandatory). Eg: storage/data")
)

var (
	rabbitConn       *amqp.Connection
	rabbitCloseError chan *amqp.Error
	usingTLS         bool
	connectionIsUp   bool
)

type Message struct {
	Exchange string
	Key      string
	Payload  string
}

func init() {
	flag.Parse()
}

func connectMQTT() (mqtt.Client, error) {

	opts := mqtt.NewClientOptions().AddBroker("tcp://" + *mqttBroker)

	client := mqtt.NewClient(opts)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("Error connecting to MQTT broker: %s", token.Error())
	}

	return client, nil
}

func connectAMQP(client mqtt.Client) *amqp.Connection {

	for {

		log.Printf("Dialing without TLS: %s", *uri)

		rabbitConn, err := amqp.Dial("amqp://" + *uri)
		if err == nil {
			log.Printf("Connection established. Getting channel")
			connectionIsUp = true
			notifyConnectionStatus(client, connectionIsUp)
			return rabbitConn
		}

		log.Printf("Trying to reconnect...")
		connectionIsUp = false
		notifyConnectionStatus(client, connectionIsUp)
		time.Sleep(5 * time.Second)
	}

}

func connectAMQPWithTLS(client mqtt.Client) *amqp.Connection {

	cfg := new(tls.Config)
	cfg.RootCAs = x509.NewCertPool()

	cfg.InsecureSkipVerify = true

	//Read CA certificate
	if ca, err := ioutil.ReadFile(*cacertpath); err == nil {
		cfg.RootCAs.AppendCertsFromPEM(ca)
	} else if err != nil {
		log.Fatalf("Error reading CA certificate: %s", err)
		os.Exit(1)
	}

	//Read client key and certificate
	if cert, err := tls.LoadX509KeyPair(*clcertpath, *clkeypath); err == nil {
		cfg.Certificates = append(cfg.Certificates, cert)
	} else if err != nil {
		log.Fatalf("Error reading client certificate or key: %s", err)
		os.Exit(1)
	}

	for {
		log.Printf("Dialing with TLS: %s", *uri)

		rabbitConn, err := amqp.DialTLS("amqps://"+*uri, cfg)
		if err == nil {
			log.Printf("Connection established. Getting channel")
			connectionIsUp = true
			notifyConnectionStatus(client, connectionIsUp)
			return rabbitConn
		}

		log.Printf("Trying to reconnect...")
		connectionIsUp = false
		notifyConnectionStatus(client, connectionIsUp)
		time.Sleep(5 * time.Second)

	}

}

func rabbitConnector(client mqtt.Client) {
	var rabbitErr *amqp.Error

	for {
		rabbitErr = <-rabbitCloseError

		if rabbitErr != nil {

			if usingTLS {
				rabbitConn = connectAMQPWithTLS(client)
			} else {
				rabbitConn = connectAMQP(client)
			}

			rabbitCloseError = make(chan *amqp.Error)
			rabbitConn.NotifyClose(rabbitCloseError)
		}
	}
}

func publish(connection *amqp.Connection, reliable bool, exchange string, routingKey string, body string) error {

	var confirms chan amqp.Confirmation

	channel, err := connection.Channel()
	if err != nil {
		return fmt.Errorf("Error getting channel: %s", err)
	}

	// Reliable publisher confirms require confirm.select support from the
	// connection.
	if reliable {
		log.Printf("Enabling publishing confirms")
		if err := channel.Confirm(false); err != nil {
			return fmt.Errorf("Could not set channel into confirm mode: %s", err)
		}

		confirms = channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	}

	if err = channel.Publish(
		exchange,   // publish to an exchange
		routingKey, // routing to 0 or more queues
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            []byte(body),
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0,              // 0-9
		},
	); err != nil {
		return fmt.Errorf("Error publishing message: %s", err)
	}

	if confirmed := <-confirms; confirmed.Ack {
		log.Printf("Confirmed delivery with delivery tag: %d", confirmed.DeliveryTag)
		channel.Close()
	} else {
		log.Printf("Failed delivery of delivery tag: %d", confirmed.DeliveryTag)
	}

	return nil
}

func notifyConnectionStatus(client mqtt.Client, connectionIsUp bool) {

	var msg string = "0"
	if connectionIsUp {
		msg = "1"
	}

	if token := client.Publish(*pubConnTopic, 0, false, msg); token.Wait() && token.Error() != nil {
		log.Printf("Error publishing message to MQTT broker: %s", token.Error())
	}

}

//Sends a message to the storage service using the corresponding MQTT topic
func storeMessage(client mqtt.Client, msg string) {

	if token := client.Publish(*pubStorageTopic, 0, false, msg); token.Wait() && token.Error() != nil {
		log.Printf("Error publishing message to MQTT broker: %s", token.Error())
	}

}

//Publish a message using AMQP
func publishMessage(connection *amqp.Connection, msg string) error {

	var jsonMsg Message
	err := json.Unmarshal([]byte(msg), &jsonMsg)

	if err != nil || jsonMsg.Exchange == "" || jsonMsg.Key == "" || jsonMsg.Payload == "" {
		log.Printf("Error parsing JSON: %s", err)
	} else {

		if connectionIsUp {
			if err := publish(rabbitConn, *reliable, jsonMsg.Exchange, jsonMsg.Key, jsonMsg.Payload); err != nil {
				return fmt.Errorf("Error publishing message: %s", err)
			}
		} else {
			return fmt.Errorf("Connection is down. Storing message")
		}
	}
	return nil
}

//Callback called when a message arrives on the subscribed topic
func mqttCallback(client mqtt.Client, msg mqtt.Message) {

	log.Printf("Publishing message")

	err := publishMessage(rabbitConn, string(msg.Payload()))
	if err != nil {
		storeMessage(client, string(msg.Payload()))
	}
}

//******* MAIN ********/
func main() {

	//Check if required arguments have been specified
	if *uri == "" || *mqttBroker == "" || *pubStorageTopic == "" || *pubConnTopic == "" || *subDataTopic == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	//Channel used to block while receiving messages
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	clientMQTT, err := connectMQTT()
	if err != nil {
		log.Fatalf("Error connecting to MQTT broker: %s", err)
	}

	//Check if certificates are being passed to use TLS or not
	if *cacertpath == "" || *clcertpath == "" || *clkeypath == "" {
		usingTLS = false
	} else {
		usingTLS = true
	}

	rabbitCloseError = make(chan *amqp.Error)
	go rabbitConnector(clientMQTT)
	rabbitCloseError <- amqp.ErrClosed

	if token := clientMQTT.Subscribe(*subDataTopic, 0, mqttCallback); token.Wait() && token.Error() != nil {
		log.Fatalf("Error subscribing to MQTT topic: %s", token.Error())
	}

	<-c
}
