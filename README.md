# AMQP publisher

Module that publishes through AMQP messages sent to it by other services

### Version

0.1

### Changelog

#### 0.1

- Initial version using Redis for communicating services

### Dependencies

- Redis client https://github.com/go-redis/redis
- AMQP client: https://github.com/streadway/amqp

### Installation

- sudo docker build -t amqppub .
- sudo docker run --name amqppub -d amqppub -uri user:pwd@amqp:5672/vhost -redisServer=redis:6379 -storageChannel=storage:data -connChannel=publisher:conn -dataChannel=publisher:data -cacert=./cacert.pem -clcert=./cert.pem -clkey=./key.pem

### Parameters

- uri: AMQP uri.
- redisServer: URL where the Redis server is located.
- storageChannel: Redis channel where this service should send messages if there is no connection available so they can be stored.
- connChannel: Redis channel where this service publishes updates of the status of the connection.
- dataChannel: Redis channel where this service listens for messages it should publish to the AMQP broker.
- cacert: Path to the CA certificate.
- clcert: Path to the client certificate.
- clkey: Path to the client key.

### How it works

- If certificates and keys are provided the service tries to connect using TLS. Otherwise it connects without encryption.
- The service listens in dataChannel for messages to be published. The format of the message shall be in JSON as "{"exchange":"exc","key":"routing_key","payload":"payload_to_send"}".
- When the service detect a loss of connection it notifies it through connChannel by sending a '0'. It tries to reconnect to the other endpoint each 5 seconds.
- When the connection is recovered a '1' is sent through connChannel.
