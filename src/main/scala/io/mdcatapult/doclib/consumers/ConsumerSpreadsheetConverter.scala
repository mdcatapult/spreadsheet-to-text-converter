package io.mdcatapult.doclib.consumers

import java.io._
import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import cats.data._
import cats.implicits._
import com.spingo.op_rabbit.SubscriptionRef
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg}
import io.mdcatapult.doclib.models.metadata.{MetaString, MetaValueUntyped}
import io.mdcatapult.doclib.models.{Derivative, DoclibDoc, Origin}
import io.mdcatapult.doclib.tabular.{Document ⇒ TabularDoc, Sheet ⇒ TabSheet}
import io.mdcatapult.doclib.util.{DoclibFlags, MongoCodecs}
import io.mdcatapult.klein.mongo.Mongo
import io.mdcatapult.klein.queue.Queue
import org.apache.commons.io.FilenameUtils
import org.bson.codecs.configuration.CodecRegistry
import org.bson.conversions.Bson
import org.bson.types.ObjectId
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Updates._
import org.mongodb.scala.result._

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

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
  val subscription: SubscriptionRef = upstream.subscribe(handle, config.getInt("upstream.concurrent"))

  lazy val flags = new DoclibFlags(config.getString("doclib.flag"))

  /**
    * default handler for messages
    * @param msg DoclibMsg
    * @param exchange String name of exchaneg message was sourced from
    * @return
    */
  def handle(msg: DoclibMsg, exchange: String): Future[Option[Any]] =
    (for {
      doc ← OptionT(collection.find(equal("_id", new ObjectId(msg.id))).first.toFutureOption())
      started: UpdateResult ← OptionT(flags.start(doc))
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
      _ ← OptionT(flags.end(doc, started.getModifiedCount > 0))
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


  def validateMimetype(doc: DoclibDoc): Option[Boolean] = {

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
    ).contains(doc.mimetype)) {
      Some(true)
    } else throw new Exception("Document mimetype is not recognised")
  }

  /**
    * send new file to prefetch queue
    * @param source String
    * @param doc Document
    * @return
    */
  def enqueue(source: String, doc: DoclibDoc): String = {
    downstream.send(PrefetchMsg(
      source=source,
      tags=doc.tags,
      metadata=None,
      origin=Some(List(Origin(
        scheme = "mongodb",
        hostname = None,
        metadata = Some(List[MetaValueUntyped](
          MetaString("db", config.getString("mongo.database")),
          MetaString("collection", config.getString("mongo.collection")),
          MetaString("_id", doc._id.toString)))))
      ),
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
    * generate new converted strings and save to the FS
    * @param doc DoclibDoc
    * @return List[String] list of new paths created
    */
  def process(doc: DoclibDoc): List[String] = {
    val targetPath = getTargetPath(doc.source)
    val d = new TabularDoc(Paths.get(doc.source))
    d.to(config.getString("output.format"))
      .filter(_.content.length > 0)
      .map(s ⇒ saveToFS(s, targetPath))
      .filter(_.path.isDefined)
      .map(_.path.get)
  }


  /**
    * merge list of new paths into existing paths in DoclibDoc
    * @param doc DoclibDoc
    * @param derivatives List[String]
    * @return List[Derivative] unique list of derivatives
    */
  def mergeDerivatives(doc: DoclibDoc, derivatives: List[String]): List[Derivative] = {
    if (config.getBoolean("doclib.overwriteDerivatives")) {
      derivatives.map(d => Derivative(`type` = "spreadsheet_conversion", path = d))
    } else {
      (derivatives.map(d => Derivative(`type` = "spreadsheet_conversion", path = d)) ::: doc.derivatives.getOrElse(List[Derivative]())).distinct
    }
  }

  def persist(id: String, update: Bson): Future[Option[UpdateResult]] =
    collection.updateOne(equal("_id", new ObjectId(id)), update).toFutureOption()

}
