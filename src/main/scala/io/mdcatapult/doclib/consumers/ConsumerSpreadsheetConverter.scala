package io.mdcatapult.doclib.consumers

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.spingo.op_rabbit.SubscriptionRef
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import io.mdcatapult.doclib.handlers.SpreadsheetHandler
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg}
import io.mdcatapult.doclib.models.DoclibDoc
import io.mdcatapult.doclib.util.MongoCodecs
import io.mdcatapult.klein.mongo.Mongo
import io.mdcatapult.klein.queue.Queue
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.MongoCollection

import scala.concurrent.ExecutionContextExecutor

object ConsumerSpreadsheetConverter extends App with LazyLogging {

  /** initialise implicit dependencies **/
  implicit val system: ActorSystem = ActorSystem("consumer-spreadsheetconverter")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executor: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  implicit val config: Config = ConfigFactory.load()

  /** Initialise Mongo **/
  // Custom mongo codecs for saving message
  implicit val codecs: CodecRegistry = MongoCodecs.get
  implicit val mongo: Mongo = new Mongo()
  implicit val collection: MongoCollection[DoclibDoc] = mongo.database.getCollection(config.getString("mongo.collection"))

  /** initialise queues **/
  val upstream: Queue[DoclibMsg] = new Queue[DoclibMsg](config.getString("upstream.queue"), consumerName = Some("spreadsheet-converter"))
  val downstream: Queue[PrefetchMsg] = new Queue[PrefetchMsg](config.getString("downstream.queue"), consumerName = Some("spreadsheet-converter"))
  val subscription: SubscriptionRef = upstream.subscribe(new SpreadsheetHandler(downstream, upstream).handle, config.getInt("upstream.concurrent"))

}
