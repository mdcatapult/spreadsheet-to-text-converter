package io.mdcatapult.doclib.consumers

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.spingo.op_rabbit.SubscriptionRef
import io.mdcatapult.doclib.consumer.AbstractConsumer
import io.mdcatapult.doclib.handlers.SpreadsheetHandler
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg, SupervisorMsg}
import io.mdcatapult.doclib.models.{DoclibDoc, ParentChildMapping}
import io.mdcatapult.klein.mongo.Mongo
import io.mdcatapult.klein.queue.Queue
import io.mdcatapult.util.admin.{Server => AdminServer}
import org.mongodb.scala.MongoCollection

object ConsumerSpreadsheetConverter extends AbstractConsumer {

  override def start()(implicit as: ActorSystem, m: Materializer, mongo: Mongo): SubscriptionRef = {
    import as.dispatcher

    AdminServer(config).start()

    implicit val collection: MongoCollection[DoclibDoc] =
      mongo.getCollection(config.getString("mongo.doclib-database"), config.getString("mongo.documents-collection"))

    implicit val derivativesCollection: MongoCollection[ParentChildMapping] =
      mongo.getCollection(config.getString("mongo.doclib-database"), config.getString("mongo.derivative-collection"))

    /** initialise queues **/
    val upstream: Queue[DoclibMsg] = queue("consumer.queue")
    val downstream: Queue[PrefetchMsg] = queue("downstream.queue")
    val supervisor: Queue[SupervisorMsg] = queue("doclib.supervisor.queue")

    upstream.subscribe(
      SpreadsheetHandler.withWriteToFilesystem(
        downstream,
        supervisor,
      ).handle,
      config.getInt("consumer.concurrency")
    )
  }
}
