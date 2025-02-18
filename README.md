# Consumer-ToTsvConverter-poi

A consumer that converts multiple spreadsheet or csv formats to tsv files

## Execution

This is a scala application that runs in side the JVM

```bash
java $JAVA_OPTS -jar consumer.jar --config common.conf
```

## Runtime Configuration

The app allows runtime configuration via environment variables

* **MONGO_USERNAME** - login username for mongodb
* **MONGO_PASSWORD** - login password for mongodb
* **MONGO_HOST** - host to connect to
* **MONGO_PORT** - optional: port to connect to (default: 27017) 
* **MONGO_DOCLIB_DATABASE** - database to connect to
* **MONGO_AUTHSOURCE** - optional: database to authenticate against (default: admin)
* **MONGO_DOCUMENTS_COLLECTION** - default collection to read and write to 
* **RABBITMQ_USERNAME** - login username for rabbitmq
* **RABBITMQ_PASSWORD** - login password for rabbitmq
* **RABBITMQ_HOST** - host to connect to
* **RABBITMQ_PORT** - optional: port to connect to (default: 5672)
* **RABBITMQ_VHOST** - optional: vhost to connect to (default: /)
* **RABBITMQ_DOCLIB_EXCHANGE** - optional: exchange that the consumer should be bound to
* **CONSUMER_QUEUE** - name of the queue to consume, will auto create and auto bind to exchange
* **CONSUMER_CONCURRENCY** - optional: number of messages to handle concurrently (default: 1)
* **SPREADSHEET_MAX_TIMEOUT** - optional: how long a spreadsheet conversion is allowed to run for in milliseconds before it is marked as a failure (default: 600000 milliseconds ie 10 minutes)

#### These are the formats currently working with their mimetype counts in the document library. I've worked from the top down to make sure we can process as many docs as possible:

* { "_id" : "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "count" : 133252 }
* { "_id" : "application/vnd.ms-excel", "count" : 87792 }
* { "_id" : "text/csv", "count" : 70305 }
* { "_id" : "application/vnd.ms-excel.sheet.macroenabled.12", "count" : 142 }

#### Not currently processing:

* { "_id" : "application/vnd.oasis.opendocument.spreadsheet", "count" : 333 }
* { "_id" : "application/vnd.oasis.opendocument.spreadsheet-template", "count" : 3 }

## Testing
```bash
docker-compose up -d
sbt clean test it:test
```

### License
This project is licensed under the terms of the Apache 2 license, which can be found in the repository as `LICENSE.txt`