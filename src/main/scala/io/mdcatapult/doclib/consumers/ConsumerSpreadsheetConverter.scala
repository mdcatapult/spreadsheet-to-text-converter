package io.mdcatapult.doclib.consumers

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.spingo.op_rabbit.SubscriptionRef
import io.mdcatapult.doclib.consumer.AbstractConsumer
import io.mdcatapult.doclib.handlers.SpreadsheetHandler
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg, SupervisorMsg}
import io.mdcatapult.doclib.models.DoclibDoc
import io.mdcatapult.klein.mongo.Mongo
import io.mdcatapult.klein.queue.{Envelope, Queue}
import org.mongodb.scala.MongoCollection
import play.api.libs.json.Format

object ConsumerSpreadsheetConverter extends AbstractConsumer("consumer-unarchive") {

  override def start()(implicit as: ActorSystem, m: Materializer, mongo: Mongo): SubscriptionRef = {
    import as.dispatcher

    implicit val collection: MongoCollection[DoclibDoc] = mongo.database.getCollection(config.getString("mongo.collection"))

    def queue[T <: Envelope](property: String)(implicit f: Format[T]): Queue[T] =
      new Queue[T](config.getString(property), consumerName = Some("spreadsheet-converter"))

    /** initialise queues **/
    val upstream: Queue[DoclibMsg] = queue("upstream.queue")
    val downstream: Queue[PrefetchMsg] = queue("downstream.queue")
    val supervisor: Queue[SupervisorMsg] = queue("doclib.supervisor.queue")

    upstream.subscribe(
      new SpreadsheetHandler(downstream, supervisor).handle,
      config.getInt("upstream.concurrent")
    )
  }
}
