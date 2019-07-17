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
import io.mdcatapult.doclib.messages.PrefetchMsg
import io.mdcatapult.doclib.models.PrefetchOrigin
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
import scala.util.{Failure, Success}

object ConsumerSpreadsheetConverter extends App with LazyLogging {

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

  /**
    * default handler for messages
    * @param msg DoclibMsg
    * @param exchange String name of exchaneg message was sourced from
    * @return
    */
  def handle(msg: DoclibMsg, exchange: String): Future[Option[Any]] =
    (for {
      doc ← OptionT(collection.find(equal("_id", new ObjectId(msg.id))).first.toFutureOption())
      _ ← OptionT.fromOption[Future](validateMimetype(doc))
      _  ← OptionT(persist(msg.id, set(config.getString("doclib.flag"), null)))
      paths: List[String] ← OptionT.pure[Future](process(doc))
      if paths.nonEmpty
      derivatives ← OptionT.pure[Future](mergeDerivatives(doc, paths))
      _ ← OptionT(persist(msg.id, combine(
            set(config.getString("doclib.flag"), true),
            set("derivatives", derivatives))).andThen({
              case Success(_) ⇒ paths.foreach(path ⇒ enqueue(path, doc))
              case Failure(e) ⇒ throw e
              }))
    } yield paths).value.andThen({
      case Success(p) ⇒ p match {
        case Some(paths) ⇒
          persist(msg.id, set(config.getString("doclib.flag"), true)).andThen({
            case Failure(err) ⇒ throw err
            case Success(_) ⇒ logger.info(f"COMPLETED: ${msg.id} - found & created ${paths.length} derivatives")
          })
        case None ⇒ persist(msg.id, set(config.getString("doclib.flag"), false)).andThen({
          case Failure(err) ⇒ throw err
          case _ ⇒ // do nothing
        })
      }
      case Failure(err) ⇒ throw err
    })


  def validateMimetype(doc: Document): Option[Boolean] = {
    println(f"${doc.getObjectId("_id").toString} - ${doc.getString("source")}")
    if (List(
      "application/vnd.lotus-1-2-3",
      "application/vnd.ms-excel",
      "application/vnd.ms-excel.sheet.macroenabled.12",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.template",
      "application/vnd.stardivision.calc",
      "application/vnd.sun.xml.calc",
      "application/vnd.sun.xml.calc.template",
      "text/csv"
    ).contains(doc.getString("mimetype"))) {
      Some(true)
    } else throw new Exception("Document mimetype is not recognised")
  }

  /**
    * send new file to prefetch queue
    * @param source String
    * @param doc Document
    * @return
    */
  def enqueue(source: String, doc: Document): String = {
    downstream.send(PrefetchMsg(
      source=source,
      tags=Some((doc("tags").asArray().getValues.asScala.map(tag => tag.asString().getValue).toList ::: List("derivative")).distinct),
      metadata=None,
      origin=Some(List(PrefetchOrigin(
        scheme = "mongodb",
        metadata = Some(Map[String, Any](
          "db" → config.getString("mongo.database"),
          "collection" → config.getString("mongo.collection"),
          "_id" → doc.getObjectId("_id").toString))))),
    ))
    source
  }


  /**
    * Persist to FS and return new sheet with new path and normalised filename
    * @param sheet TabSheet
    * @param targetPath String
    * @return
    */
  def saveToFS(sheet: TabSheet, targetPath: String): TabSheet = {
    val filename = sheet.name.replaceAll(" ", "_").replaceAll("[^0-9a-zA-Z_-]", "-")
    val target = new File(s"$targetPath/${sheet.index}_$filename.${config.getString("output.format")}")
    target.getParentFile.mkdirs()
    val w = new BufferedWriter(new FileWriter(target))
    w.write(sheet.content)
    w.close()
    sheet.copy(path=Some(target.getAbsolutePath))
  }

  /**
    * determines common root paths for two path string
    * @param paths List[String]
    * @return String common path component
    */
  def commonPath(paths: List[String]): String = {
    val SEP = "/"
    val BOUNDARY_REGEX = s"(?=[$SEP])(?<=[^$SEP])|(?=[^$SEP])(?<=[$SEP])"
    def common(a: List[String], b: List[String]): List[String] = (a, b) match {
      case (aa :: as, bb :: bs) if aa equals bb => aa :: common(as, bs)
      case _ => Nil
    }
    if (paths.length < 2) paths.headOption.getOrElse("")
    else paths.map(_.split(BOUNDARY_REGEX).toList).reduceLeft(common).mkString
  }

  /**
    * generate new file path maintaining file path from origin but allowing for intersection of common root paths
    * @param source String
    * @return String full path to new target
    */
  def getTargetPath(source: String): String = {
    val targetRoot = config.getString("output.baseDirectory").replaceAll("/+$", "")
    val sourceName = FilenameUtils.removeExtension(source)
    val c = commonPath(List(targetRoot, sourceName))
    val scrubbed = sourceName.replaceAll(s"^$c", "").replaceAll("^/+|/+$", "")
    s"$targetRoot/$scrubbed/"
  }


  /**
    * generated new converted strings and save to the FS
    * @param doc Document
    * @return List[String] list of new paths created
    */
  def process(doc: Document): List[String] = {
    val targetPath = getTargetPath(doc.getString("source"))
    val d = new TabularDoc(Paths.get(doc.getString("source")))
    d.to(config.getString("output.format"))
      .filter(_.content.length > 0)
      .map(s ⇒ saveToFS(s, targetPath))
      .filter(_.path.isDefined)
      .map(_.path.get)
  }


  /**
    * merge list of new paths into existing paths in document
    * @param doc Document
    * @param derivatives List[String]
    * @return unique list of derivatives
    */
  def mergeDerivatives(doc: Document, derivatives: List[String]) =
    if (config.getBoolean("doclib.overwriteDerivatives")) {
      derivatives
    } else {
      (doc.get[BsonArray]("derivatives").getOrElse(BsonArray()).getValues.asScala.flatMap({
        case d: BsonString ⇒ Some(d.getValue)
        case _ ⇒ None
      }).toList ::: derivatives).distinct
    }

  def persist(id: String, update: Bson): Future[Option[UpdateResult]] =
    collection.updateOne(equal("_id", new ObjectId(id)), update).toFutureOption()

}
