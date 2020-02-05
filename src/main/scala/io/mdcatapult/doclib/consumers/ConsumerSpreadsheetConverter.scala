package io.mdcatapult.doclib.consumers

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.spingo.op_rabbit.SubscriptionRef
import io.mdcatapult.doclib.consumer.AbstractConsumer
import io.mdcatapult.doclib.handlers.SpreadsheetHandler
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg, SupervisorMsg}
import io.mdcatapult.doclib.models.DoclibDoc
import io.mdcatapult.klein.mongo.Mongo
import io.mdcatapult.klein.queue.Queue
import org.mongodb.scala.MongoCollection

object ConsumerSpreadsheetConverter extends AbstractConsumer("consumer-unarchive") {

  def start()(implicit as: ActorSystem, materializer: ActorMaterializer, mongo: Mongo): SubscriptionRef = {
    import as.dispatcher
    implicit val collection: MongoCollection[DoclibDoc] = mongo.database.getCollection(config.getString("mongo.collection"))

    /** initialise queues **/
    val upstream: Queue[DoclibMsg] = new Queue[DoclibMsg](config.getString("upstream.queue"), consumerName = Some("spreadsheet-converter"))
    val downstream: Queue[PrefetchMsg] = new Queue[PrefetchMsg](config.getString("downstream.queue"), consumerName = Some("spreadsheet-converter"))
    val supervisor: Queue[SupervisorMsg] = new Queue[SupervisorMsg](config.getString("doclib.supervisor.queue"), Some("rawtext"))
    upstream.subscribe(new SpreadsheetHandler(downstream, supervisor).handle, config.getInt("upstream.concurrent"))
  }
}
