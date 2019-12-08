package io.mdcatapult.doclib.handlers

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import better.files.{File => ScalaFile, _}
import cats.data.OptionT
import cats.implicits._
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import io.mdcatapult.doclib.exception.DoclibDocException
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg, SupervisorMsg}
import io.mdcatapult.doclib.models.metadata.{MetaString, MetaValueUntyped}
import io.mdcatapult.doclib.models.{Derivative, DoclibDoc, Origin}
import io.mdcatapult.doclib.tabular.{Document => TabularDoc, Sheet => TabSheet}
import io.mdcatapult.doclib.util.DoclibFlags
import io.mdcatapult.klein.queue.Sendable
import org.bson.conversions.Bson
import org.bson.types.ObjectId
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.model.Filters.equal
import org.mongodb.scala.model.Updates.{combine, set}
import org.mongodb.scala.result.UpdateResult

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

class SpreadsheetHandler(downstream: Sendable[PrefetchMsg], supervisor: Sendable[SupervisorMsg])
                        (implicit ac: ActorSystem,
                         materializer: ActorMaterializer,
                         ex: ExecutionContextExecutor,
                         config: Config,
                         collection: MongoCollection[DoclibDoc]) extends LazyLogging {

  final case class MimetypeNotAllowed(doc: DoclibDoc,
                                cause: Throwable = None.orNull)
    extends DoclibDocException(
      doc,
      f"Document: ${doc._id.toHexString} - Mimetype '${doc.mimetype}' not allowed'",
      cause)

  lazy val flags = new DoclibFlags(config.getString("doclib.flag"))

  /**
   * default handler for messages
   * @param msg DoclibMsg
   * @param exchange String name of exchaneg message was sourced from
   * @return
   */
  def handle(msg: DoclibMsg, exchange: String): Future[Option[Any]] = {
    logger.info(f"RECEIVED: ${msg.id}")
    (for {
      doc ← OptionT(collection.find(equal("_id", new ObjectId(msg.id))).first.toFutureOption())
      started: UpdateResult ← OptionT(flags.start(doc))
      _ ← OptionT.fromOption[Future](validateMimetype(doc))
      paths: List[String] ← OptionT.pure[Future](process(doc))
      if paths.nonEmpty
      derivatives ← OptionT.pure[Future](mergeDerivatives(doc, paths))
      _ ← OptionT(persist(msg.id, combine(
        set("derivatives", derivatives))).andThen({
        case Success(_) ⇒ paths.foreach(path ⇒ enqueue(path, doc))
        case Failure(e) ⇒ throw e
      })
      )
      _ ← OptionT(flags.end(doc, started.getModifiedCount > 0))
    } yield (paths, doc)).value.andThen({
      case Success(result) ⇒ result match {
        case Some(r) ⇒
          supervisor.send(SupervisorMsg(id = r._2._id.toHexString))
          logger.info(f"COMPLETED: ${msg.id} - found & created ${r._1.length} derivatives")
        case None ⇒ // do nothing?
      }
      // Wait 10 seconds then fail
      case Failure(e: DoclibDocException) ⇒ flags.error(e.getDoc, noCheck = true)
      case Failure(_) ⇒ Try(Await.result(collection.find(equal("_id", new ObjectId(msg.id))).first.toFutureOption(), 10 seconds)) match {
        case Success(value: Option[DoclibDoc]) ⇒ value match {
          case Some(aDoc) ⇒ flags.error(aDoc, noCheck = true)
          case _ ⇒ // captured by error handling
        }
        case Failure(_) ⇒ // Error will bubble up
      }
    })
  }

  def validateMimetype(doc: DoclibDoc): Option[Boolean] = {
    if (List(
      """application/vnd\.lotus.*""".r,
      """application/vnd\.ms-excel.*""".r,
      """application/vnd\.openxmlformats-officedocument.spreadsheetml.*""".r,
      """application/vnd\.stardivision.calc""".r,
      """application/vnd\.sun\.xml\.calc.*""".r,
      """application/vnd\.oasis\.opendocument\.spreadsheet""".r
    ).count(_.findFirstIn(doc.mimetype).isDefined) > 0) {
      Some(true)
    } else throw new MimetypeNotAllowed(doc)
  }

  /**
   * send new file to prefetch queue
   * @param source String
   * @param doc Document
   * @return
   */
  def enqueue(source: String, doc: DoclibDoc): String = {
    // Let prefetch know that it is a spreadsheet derivative
    val derivativeMetadata = List[MetaValueUntyped](MetaString("derivative.type", "spreadsheet"))
    downstream.send(PrefetchMsg(
      source=source,
      tags=doc.tags,
      metadata = Some(doc.metadata.getOrElse(Nil) ::: derivativeMetadata),
      derivative=Some(true),
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
    val target = new File(s"$targetPath/${sheet.index}_$filename.${config.getString("convert.format")}")
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
   * The absolute path from file system root through doclib root to the actual file
   * @param path path to resolve
   * @return
   */
  def getAbsPath(path: String): String = {
    Paths.get(config.getString("doclib.root"), path).toAbsolutePath.toString
  }

  /**
   * generate new file path maintaining file path from origin but allowing for intersection of common root paths
   * @param source String
   * @return String full path to new target
   */
  def getTargetPath(source: String, base: String, prefix: Option[String] = None): String = {
    val targetRoot = base.replaceAll("/+$", "")
    val regex = """(.*)/(.*)$""".r
    source match {
      case regex(path, file) ⇒
        val c = commonPath(List(targetRoot, path))
        val targetPath  = scrub(path.replaceAll(s"^$c", "").replaceAll("^/+|/+$", ""))
        Paths.get(config.getString("doclib.local.temp-dir"), targetRoot, targetPath, s"${prefix.getOrElse("")}-$file").toString
      case _ ⇒ source
    }
  }

  def scrub(path: String):String  = path match {
    case path if path.startsWith(config.getString("doclib.local.target-dir")) ⇒
      scrub(path.replaceFirst(s"^${config.getString("doclib.local.target-dir")}/*", ""))
    case path if path.startsWith(config.getString("convert.to.path"))  ⇒
      scrub(path.replaceFirst(s"^${config.getString("convert.to.path")}/*", ""))
    case _ ⇒ path
  }

  /**
   * generate new converted strings and save to the FS
   * @param doc DoclibDoc
   * @return List[String] list of new paths created
   */
  def process(doc: DoclibDoc): List[String] = {
    val targetPath = getTargetPath(doc.source, config.getString("convert.to.path"), Some("spreadsheet_conv"))
    val sourceAbsPath:ScalaFile = config.getString("doclib.root")/doc.source
    val d = new TabularDoc(Paths.get(sourceAbsPath.toString()))
    d.convertTo(config.getString("convert.format"))
      .filter(_.content.length > 0)
      .map(s ⇒ saveToFS(s, getAbsPath(targetPath)))
      .filter(_.path.isDefined)
      .map(_.path.get)
      .map(sheet => {
        val root: ScalaFile = config.getString("doclib.root")/""
        sheet.replaceFirst(s"${root.toString()}/", "")
      })
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
