# Consumer-ToTsvConverter-poi

A consumer that converts multiple spreadsheet or csv formats to tsv files

## Execution

This is a scalal application that runs in side the JVM

```bash
java -jar consumer-totsvconverter-poi.jar
```

## Runtime Configuration

The app allows runtime configuration via environment variables

* **MONGO_USERNAME** - login username for mongodb
* **MONGO_PASSWORD** - login password for mongodb
* **MONGO_HOST** - host to connect to
* **MONGO_PORT** - optional: port to connect to (default: 27017) 
* **MONGO_DATABASE** - database to connect to
* **MONGO_AUTH_DB** - optional: database to authenticate against (default: admin)
* **MONGO_COLLECTION** - default collection to read and write to
* **MONGO_COLLECTION_WRITE** - optional: over-ride collection to write output to 
* **RABBITMQ_USERNAME** - login username for rabbitmq
* **RABBITMQ_PASSWORD** - login password for rabbitmq
* **RABBITMQ_HOST** - host to connect to
* **RABBITMQ_PORT** - optional: port to connect to (default: 5672)
* **RABBITMQ_VHOST** - optional: vhost to connect to (default: /)
* **RABBITMQ_EXCHANGE** - optional: exchange that the consumer should be bound to
* **LEADMINE_CONFIG** - S3 url to pull config from (s3://...)
* **LEADMINE_TARGET_PROP** - mongo document property that the output should be written to 
* **LEADMINE_SOURCE_TYPE** - optional: type of consumer that should happen i.e. file|string (default: file)
* **LEADMINE_SOURCE_PROPERTY** - optional: the property to obtain the source file path or content string to parse (default: source})
* **CONSUMER_QUEUE** - name of the queue to consume, will auto create and auto bind to exchange
* **CONSUMER_TOPICS** - optiona: topics to use as routing keys when attaching to exchange (default: klein.ner.leadmine)
* **CONSUMER_CONCURRENT** - optional: number of messages to handle concurrently (default: 1)
* 

#### These are the formats currently working with their mimetype counts in the document library. I've worked from the top down to make sure we can process as many docs as possible:

* { "_id" : "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "count" : 133252 }
* { "_id" : "application/vnd.ms-excel", "count" : 87792 }
* { "_id" : "text/csv", "count" : 70305 }
* { "_id" : "application/vnd.ms-excel.sheet.macroenabled.12", "count" : 142 }

#### Not currently processing:

* { "_id" : "application/vnd.oasis.opendocument.spreadsheet", "count" : 333 }
* { "_id" : "application/vnd.oasis.opendocument.spreadsheet-template", "count" : 3 }
