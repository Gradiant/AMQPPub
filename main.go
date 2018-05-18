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
	"time"

	"github.com/go-redis/redis"
	"github.com/streadway/amqp"
)

var (
	uri               = flag.String("uri", "", "AMQP URI (Mandatory). Eg.: guest:guest@localhost:5672/vhost ")
	redisServer       = flag.String("redisServer", "", "Redis server URI (Mandatory). Eg: 192.168.1.1:6379")
	reliable          = flag.Bool("reliable", true, "Wait for the publisher confirmation before exiting")
	cacertpath        = flag.String("cacert", "", "Path to the Root CA certificate")
	clcertpath        = flag.String("clcert", "", "Path to the client certificate")
	clkeypath         = flag.String("clkey", "", "Path to the client key")
	pubConnChannel    = flag.String("connChannel", "", "Redis channel used to publish connection status changes (Mandatory). Eg: publisher:conn")
	subDataChannel    = flag.String("dataChannel", "", "Redis channel used to subscribe to data other services want to publish (Mandatory). Eg: publisher:data")
	pubStorageChannel = flag.String("storageChannel", "", "Redis channel used to subscribe to data other services want to publish (Mandatory). Eg: publisher:data")
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

func connectRedis() *redis.Client {

	// Connect to Redis
	client := redis.NewClient(&redis.Options{
		Addr: *redisServer,
	})

	// Check connection with Redis is OK
	err := client.Ping().Err()
	if err != nil {
		log.Fatalf("Cannot connect to Redis: %s", err)
	}

	return client
}

func connectAMQP(client redis.Client) *amqp.Connection {

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

func connectAMQPWithTLS(client redis.Client) *amqp.Connection {

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

func rabbitConnector(client redis.Client) {
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

		confirms := channel.NotifyPublish(make(chan amqp.Confirmation, 1))

		defer confirmOne(confirms)
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

	return nil
}

func confirmOne(confirms <-chan amqp.Confirmation) {
	log.Printf("Waiting for confirmation of one publishing")

	if confirmed := <-confirms; confirmed.Ack {
		log.Printf("Confirmed delivery with delivery tag: %d", confirmed.DeliveryTag)
	} else {
		log.Printf("Failed delivery of delivery tag: %d", confirmed.DeliveryTag)
	}
}

func notifyConnectionStatus(client redis.Client, connectionIsUp bool) {

	err := client.Publish(*pubConnChannel, connectionIsUp).Err()
	if err != nil {
		log.Printf("Couldn't send connection notification")
	}

}

func storeMessage(client redis.Client, msg string) {

	err := client.Publish(*pubStorageChannel, msg).Err()
	if err != nil {
		log.Printf("Couldn't store message. Dropping...")
	}

}

func publishMessage(connection *amqp.Connection, msg string) error {

	var jsonMsg Message
	err := json.Unmarshal([]byte(msg), &jsonMsg)

	if err != nil || jsonMsg.Exchange == "" || jsonMsg.Key == "" || jsonMsg.Payload == "" {
		log.Printf("Error parsing JSON")
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

//******* MAIN ********/
func main() {

	//Check if required arguments have been specified
	if *uri == "" || *redisServer == "" || *pubStorageChannel == "" || *pubConnChannel == "" || *subDataChannel == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	client := connectRedis()

	//Check if certificates are being passed to use TLS or not
	if *cacertpath == "" || *clcertpath == "" || *clkeypath == "" {
		usingTLS = false
	} else {
		usingTLS = true
	}

	rabbitCloseError = make(chan *amqp.Error)
	go rabbitConnector(*client)
	rabbitCloseError <- amqp.ErrClosed

	pubsub := client.Subscribe(*subDataChannel)
	defer pubsub.Close()

	for {

		msg, err := pubsub.ReceiveMessage()

		if err != nil {
			log.Printf("Error receiving message from subscription")
		} else {
			log.Printf("received: %s from %s", msg.Payload, msg.Channel)
			err := publishMessage(rabbitConn, msg.Payload)
			if err != nil {
				storeMessage(*client, msg.Payload)
			}
		}

	}

}
