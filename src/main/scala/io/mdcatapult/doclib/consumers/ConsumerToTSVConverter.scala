package io.mdcatapult.doclib.consumers

import java.io._
import java.nio.file.Paths

import akka.actor.ActorSystem
import cats.data._
import cats.implicits._
import com.spingo.op_rabbit.SubscriptionRef
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import io.mdcatapult.doclib.messages.DoclibMsg
import io.mdcatapult.doclib.messages.legacy.PrefetchMsg
import io.mdcatapult.doclib.tabular.{Document ⇒ TabularDoc, Sheet ⇒ TabSheet}
import io.mdcatapult.klein.mongo.Mongo
import io.mdcatapult.klein.queue.Queue
import org.apache.commons.io.FilenameUtils
import org.bson.conversions.Bson
import org.bson.types.ObjectId
import org.mongodb.scala.Document
import org.mongodb.scala.bson._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Updates._
import org.mongodb.scala.result._

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

object ConsumerToTSVConverter extends App with LazyLogging {

  implicit val system: ActorSystem = ActorSystem("consumer-totsvconverter")
  implicit val executor: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  implicit val config: Config = ConfigFactory.load()

  val upstream: Queue[DoclibMsg] = new Queue[DoclibMsg](
    config.getString("upstream.queue"),
    Some(config.getString("upstream.topics")))
  val subscription: SubscriptionRef = upstream.subscribe(handle, config.getInt("upstream.concurrent"))
  val downstream: Queue[PrefetchMsg] = new Queue[PrefetchMsg](config.getString("downstream.queue"))


  val mongo = new Mongo()
  val collection = mongo.getCollection()

  def handle(msg: DoclibMsg, key: String): Future[Option[Any]] =
    Try(for {
      doc ← OptionT(collection.find(equal("_id", new ObjectId(msg.id))).first.toFutureOption())
      _  ← OptionT(persist(msg.id, set(config.getString("doclib.flag"), false)))
      paths ← OptionT.pure[Future](process(doc))
      _ ← OptionT(persist(msg.id, combine(
            set(config.getString("doclib.flag"), true),
            set("derivatives", paths))).andThen({
              case Success(_) ⇒ paths.foreach(path ⇒ enqueue(path, doc))
              case Failure(e) ⇒ throw e
              }))
    } yield paths) match {
      case Failure(e) ⇒ throw e
      case Success(r) ⇒
        logger.info(f"COMPLETED: ${msg.id}")
        r.value
    }


  def enqueue(source: String, doc: Document): String = {
    downstream.send(PrefetchMsg(
      source,
      doc.getObjectId("_id").toString,
      doc("tags").asArray().getValues.asScala.map(tag => tag.asString().getValue).toList
    ))
    source
  }


  def saveToFS(sheet: TabSheet, targetPath: String): TabSheet = {
    val target = new File(s"$targetPath/${sheet.index}_${sheet.name}.${config.getString("output.format")}")
    target.getParentFile.mkdirs()
    val w = new BufferedWriter(new FileWriter(target))
    w.write(sheet.content)
    w.close()
    sheet.copy(path=Some(targetPath))
  }


  def process(doc: Document): List[String] = {
    val targetRoot = config.getString("output.baseDirectory").replaceAll("/+$", "")
    val sourceName = FilenameUtils.removeExtension(FilenameUtils.getBaseName(doc.getString("source")))
    val targetPath = s"$targetRoot/$sourceName/"

    val d = new TabularDoc(Paths.get(doc.getString("source")))
    val sheets: List[String] = d.to(config.getString("output.format"))
      .filter(_.content.length == 0)
      .map(s ⇒ saveToFS(s, targetPath))
      .filter(_.path.isDefined)
      .map(_.path.get)

    (doc.get[BsonArray]("derivatives").getOrElse(BsonArray()).getValues.asScala.flatMap({
      case d: BsonString ⇒ Some(d.getValue)
      case _ ⇒ None
    }).toList ::: sheets).distinct
  }

  def persist(id: String, update: Bson): Future[Option[UpdateResult]] =
    collection.updateOne(equal("_id", new ObjectId(id)), update).toFutureOption()



}
