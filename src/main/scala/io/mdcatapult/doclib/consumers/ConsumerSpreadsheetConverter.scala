package io.mdcatapult.doclib.consumers

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.spingo.op_rabbit.SubscriptionRef
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import io.mdcatapult.doclib.consumer.AbstractConsumer
import io.mdcatapult.doclib.handlers.SpreadsheetHandler
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg, SupervisorMsg}
import io.mdcatapult.doclib.models.DoclibDoc
import io.mdcatapult.doclib.util.MongoCodecs
import io.mdcatapult.klein.mongo.Mongo
import io.mdcatapult.klein.queue.Queue
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.MongoCollection

import scala.concurrent.ExecutionContextExecutor

object ConsumerSpreadsheetConverter extends AbstractConsumer("consumer-unarchive") {

  def start()(implicit as: ActorSystem, materializer: ActorMaterializer, mongo: Mongo): SubscriptionRef = {
    implicit val ex: ExecutionContextExecutor = as.dispatcher
    implicit val collection: MongoCollection[DoclibDoc] = mongo.database.getCollection(config.getString("mongo.collection"))

    /** initialise queues **/
    val upstream: Queue[DoclibMsg] = new Queue[DoclibMsg](config.getString("upstream.queue"), consumerName = Some("spreadsheet-converter"))
    val downstream: Queue[PrefetchMsg] = new Queue[PrefetchMsg](config.getString("downstream.queue"), consumerName = Some("spreadsheet-converter"))
    val supervisor: Queue[SupervisorMsg] = new Queue[SupervisorMsg](config.getString("doclib.supervisor.queue"), Some("rawtext"))
    upstream.subscribe(new SpreadsheetHandler(downstream, supervisor).handle, config.getInt("upstream.concurrent"))
  }
}
