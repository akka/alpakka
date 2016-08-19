All these tests require a locally running amqp server that does not require any authorization.

When using docker, a temporary server can by started by running:

```
docker run --rm -p 5672:5672 rabbitmq:3
```
