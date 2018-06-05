# AMQP publisher

Module that publishes through AMQP messages sent to it by other services

### Version

0.2

### Changelog

#### 0.2

- Changed service communication to MQTT

#### 0.1

- Initial version using Redis for communicating services

### Dependencies

- MQTT client https://github.com/eclipse/paho.mqtt.golang
- AMQP client: https://github.com/streadway/amqp
- A MQTT broker like Mosquitto is needed to talk to other services

### Installation

- sudo docker build -t amqppub .
- sudo docker run --name amqppub -d amqppub -uri user:pwd@amqp:5672/vhost -mqttBroker=mqtt:1883 -storageTopic=storage/data -connTopic=publisher/conn -dataTopic=publisher/data -cacert=./cacert.pem -clcert=./cert.pem -clkey=./key.pem

### Parameters

- uri: AMQP uri.
- mqttBroker: URL where the MQTT broker is located.
- storageTopic: MQTT topic where this service should send messages if there is no connection available so they can be stored.
- connTopic: MQTT topic where this service publishes updates of the status of the connection.
- dataTopic: MQTT topic where this service listens for messages it should publish to the AMQP broker.
- cacert: Path to the CA certificate.
- clcert: Path to the client certificate.
- clkey: Path to the client key.

### How it works

- If certificates and keys are provided the service tries to connect using TLS. Otherwise it connects without encryption.
- When providing certificates for TLS connection make sure that you update the needed files in the certs folder before starting the image.
- The service listens in dataTopic for messages to be published. The format of the message shall be in JSON as '{"exchange":"exc","key":"routing_key","payload":"payload_to_send"}'.
- When the service detects a loss of connection it notifies it through connTopic by sending a '0'. It tries to reconnect to the other endpoint each 5 seconds.
- When the connection is recovered a '1' is sent through connTopic.
