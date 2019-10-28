package io.mdcatapult.doclib.handlers

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.Paths

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import cats.data.OptionT
import cats.data._
import cats.implicits._
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

import better.files._
import better.files.{File => ScalaFile, _}
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg}
import io.mdcatapult.doclib.models.{Derivative, DoclibDoc, Origin}
import io.mdcatapult.doclib.models.metadata.{MetaString, MetaValueUntyped}
import io.mdcatapult.doclib.util.DoclibFlags
import io.mdcatapult.klein.queue.Sendable
import io.mdcatapult.doclib.tabular.{Document ⇒ TabularDoc, Sheet ⇒ TabSheet}
import org.apache.commons.io.FilenameUtils
import org.bson.codecs.configuration.CodecRegistry
import org.bson.conversions.Bson
import org.bson.types.ObjectId
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.model.Filters.equal
import org.mongodb.scala.model.Updates.{combine, set}
import org.mongodb.scala.result.UpdateResult

import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class SpreadsheetHandler(downstream: Sendable[PrefetchMsg], upstream: Sendable[DoclibMsg])
                        (implicit ac: ActorSystem,
                         materializer: ActorMaterializer,
                         ex: ExecutionContextExecutor,
                         config: Config,
                         collection: MongoCollection[DoclibDoc],
                         codecs: CodecRegistry) extends LazyLogging {

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
    } yield paths).value.andThen({
      case Success(p) ⇒ p match {
        case Some(paths) ⇒ logger.info(f"COMPLETED: ${msg.id} - found & created ${paths.length} derivatives")
        case None ⇒ // do nothing?
      }
      // Wait 10 seconds then fail
      case Failure(_) ⇒ Try(Await.result(collection.find(equal("_id", new ObjectId(msg.id))).first.toFutureOption(), 10 seconds)) match {
        case Success(value: Option[DoclibDoc]) ⇒ value match {
          case Some(aDoc) ⇒ flags.error(aDoc, noCheck = true)
          case _ ⇒ // captured by error handling
        }
        case Failure(_) ⇒ // Error will bubble up
      }
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
   * @param path
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
        val scrubbed = path.replaceAll(s"^$c", "").replaceAll("^/+|/+$", "")
        val targetPath = scrubbed match {
          case path if path.startsWith(config.getString("doclib.local.target-dir")) => path.replaceFirst(s"^${config.getString("doclib.local.target-dir")}/*", "")
          case path if path.startsWith(config.getString("doclib.remote.target-dir")) => path
        }
        Paths.get(config.getString("doclib.local.temp-dir"), targetRoot, targetPath, s"${prefix.getOrElse("")}-$file").toString
      case _ ⇒ source
    }
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
