package io.mdcatapult.doclib.handlers

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.Paths
import java.util.UUID

import better.files.{File => ScalaFile, _}
import cats.data.OptionT
import cats.implicits._
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import io.mdcatapult.doclib.exception.DoclibDocException
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg, SupervisorMsg}
import io.mdcatapult.doclib.models.metadata.{MetaString, MetaValueUntyped}
import io.mdcatapult.doclib.models.{DoclibDoc, DoclibDocExtractor, Origin, ParentChildMapping}
import io.mdcatapult.doclib.tabular.{Document => TabularDoc, Sheet => TabSheet}
import io.mdcatapult.doclib.util.DoclibFlags
import io.mdcatapult.klein.queue.Sendable
import org.bson.types.ObjectId
import org.mongodb.scala.model.Filters.equal
import org.mongodb.scala.result.{DeleteResult, UpdateResult}
import org.mongodb.scala.{Completed, MongoCollection}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class SpreadsheetHandler(downstream: Sendable[PrefetchMsg], supervisor: Sendable[SupervisorMsg])
                        (implicit ex: ExecutionContext,
                         config: Config,
                         collection: MongoCollection[DoclibDoc], derivativesCollection: MongoCollection[ParentChildMapping]) extends LazyLogging {

  private val docExtractor = DoclibDocExtractor()

  private val overwriteDerivatives = config.getBoolean("doclib.overwriteDerivatives")
  private val convertToFormat = config.getString("convert.format")
  private val doclibRoot = config.getString("doclib.root")
  private val tempDir = config.getString("doclib.local.temp-dir")
  private val localTargetDir = config.getString("doclib.local.target-dir")
  private val convertToPath = config.getString("convert.to.path")

  case class MimetypeNotAllowed(doc: DoclibDoc,
                                cause: Throwable = None.orNull)
    extends DoclibDocException(
      doc,
      f"Document: ${doc._id.toHexString} - Mimetype '${doc.mimetype}' not allowed'",
      cause)

  lazy val flags = new DoclibFlags(docExtractor.defaultFlagKey)

  /**
   * default handler for messages
   * @param msg DoclibMsg
   * @param exchange String name of exchange message was sourced from
   * @return
   */
  def handle(msg: DoclibMsg, exchange: String): Future[Option[Any]] = {
    logger.info(f"RECEIVED: ${msg.id}")

    (for {
      doc <- OptionT(collection.find(equal("_id", new ObjectId(msg.id))).first.toFutureOption())
      if !docExtractor.isRunRecently(doc)
      started: UpdateResult <- OptionT(flags.start(doc))
      _ <- OptionT.fromOption[Future](validateMimetype(doc))
      paths: List[String] <- OptionT.pure[Future](process(doc))
      if paths.nonEmpty
      derivatives <- OptionT.pure[Future](createDerivativesFromPaths(doc, paths))
      _ <- OptionT(deleteExistingDerivatives(doc))
      _ <- OptionT(persist(derivatives)
        .andThen({
          case Success(_) => paths.foreach(path => enqueue(path, doc))
          case Failure(e) => throw e
        })
      )
      _ <- OptionT(flags.end(doc, noCheck = started.getModifiedCount > 0))
    } yield (paths, doc)).value.andThen({
      case Success(result) => result match {
        case Some(r) =>
          supervisor.send(SupervisorMsg(id = r._2._id.toHexString))
          logger.info(f"COMPLETED: ${msg.id} - found & created ${r._1.length} derivatives")
        case None => () // do nothing?
      }
      // Wait 10 seconds then fail
      case Failure(e: DoclibDocException) => flags.error(e.getDoc, noCheck = true)
      case Failure(_) => Try(Await.result(collection.find(equal("_id", new ObjectId(msg.id))).first.toFutureOption(), 10.seconds)) match {
        case Success(value: Option[DoclibDoc]) => value match {
          case Some(aDoc) => flags.error(aDoc, noCheck = true)
          case _ => () // captured by error handling
        }
        case Failure(_) => () // Error will bubble up
      }
    })
  }

  def validateMimetype(doc: DoclibDoc): Option[Boolean] = {
    val knownMimetypes =
      Seq(
        """application/vnd\.lotus.*""".r,
        """application/vnd\.ms-excel.*""".r,
        """application/vnd\.openxmlformats-officedocument.spreadsheetml.*""".r,
        """application/vnd\.stardivision.calc""".r,
        """application/vnd\.sun\.xml\.calc.*""".r,
        """application/vnd\.oasis\.opendocument\.spreadsheet""".r,
      )

    if (knownMimetypes.exists(_.matches(doc.mimetype)))
      Some(true)
    else
      throw MimetypeNotAllowed(doc)
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
      )
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
    val target = new File(s"$targetPath/${sheet.index}_$filename.$convertToFormat}")
    target.getParentFile.mkdirs()
    val fileWriter = new FileWriter(target)

    try {
      val w = new BufferedWriter(fileWriter)
      try {
        w.write(sheet.content)
      } finally {
        w.close()
      }
      sheet.copy(path = Some(target.getAbsolutePath))
    } finally {
      fileWriter.close()
    }
  }

  /**
   * determines common root paths for two path string
   * @param paths List[String]
   * @return String common path component
   */
  def commonPath(paths: List[String]): String = {
    val SEP = "/"
    val BOUNDARY_REGEX = s"(?=[$SEP])(?<=[^$SEP])|(?=[^$SEP])(?<=[$SEP])"

    def common(a: List[String], b: List[String]): List[String] =
      (a, b) match {
        case (aa :: as, bb :: bs) if aa equals bb => aa :: common(as, bs)
        case _ => Nil
      }

    if (paths.length < 2)
      paths.headOption.getOrElse("")
    else
      paths
        .map(_.split(BOUNDARY_REGEX).toList)
        .reduceLeft(common)
        .mkString
  }

  /**
   * The absolute path from file system root through doclib root to the actual file
   * @param path path to resolve
   * @return
   */
  def getAbsPath(path: String): String =
    Paths.get(doclibRoot, path).toAbsolutePath.toString

  /**
   * generate new file path maintaining file path from origin but allowing for intersection of common root paths
   * @param source String
   * @return String full path to new target
   */
  def getTargetPath(source: String, base: String, prefix: Option[String] = None): String = {
    val targetRoot = base.replaceAll("/+$", "")
    val regex = """(.*)/(.*)$""".r

    source match {
      case regex(path, file) =>
        val c = commonPath(List(targetRoot, path))
        val targetPath =
          scrub(
            path
              .replaceAll(s"^$c", "")
              .replaceAll("^/+|/+$", "")
          )

        Paths.get(tempDir, targetRoot, targetPath, s"${prefix.getOrElse("")}-$file").toString
      case _ => source
    }
  }

  def scrub(path: String): String = path match {
    case path if path.startsWith(localTargetDir) =>
      scrub(path.replaceFirst(s"^$localTargetDir/*", ""))
    case path if path.startsWith(convertToPath)  =>
      scrub(path.replaceFirst(s"^$convertToPath/*", ""))
    case _ => path
  }

  /**
   * generate new converted strings and save to the FS
   * @param doc DoclibDoc
   * @return List[String] list of new paths created
   */
  def process(doc: DoclibDoc): List[String] = {
    val targetPath = getTargetPath(doc.source, convertToPath, Some("spreadsheet_conv"))
    val sourceAbsPath:ScalaFile = doclibRoot/doc.source
    val d = new TabularDoc(Paths.get(sourceAbsPath.toString))

    d.convertTo(convertToFormat)
      .filter(_.content.length > 0)
      .map(s => saveToFS(s, getAbsPath(targetPath)))
      .filter(_.path.isDefined)
      .map(_.path.get)
      .map(sheet => {
        val root: ScalaFile = doclibRoot/""
        sheet.replaceFirst(s"$root/", "")
      })
  }


  /**
   * Create list of parent child mappings
   * @param doc DoclibDoc
   * @param paths List[String]
   * @return List[Derivative] unique list of derivatives
   */
  def createDerivativesFromPaths(doc: DoclibDoc, paths: List[String]): List[ParentChildMapping] =
    paths.map(d => ParentChildMapping(_id = UUID.randomUUID, childPath = d, parent = doc._id, consumer = Some("spreadsheet_conversion")))

  def deleteExistingDerivatives(doc: DoclibDoc): Future[Option[DeleteResult]] = {
    // TODO should we delete the doclib docs as well ie child in the existing mappings
    //  else we could end up with orphaned docs? You would already get that in the previous
    //  version. Maybe overwriteDerivatives should default to true.
    if (overwriteDerivatives)
      derivativesCollection.deleteMany(equal("parent", doc._id)).toFutureOption()
    else
      Future.successful(None)
  }

  def persist(derivatives: List[ParentChildMapping]): Future[Option[Completed]] = {
    //TODO This assumes that these are all new mappings. If we haven't deleted any existing ones
    // then we could get clashes on save if they haven't been prefetched yet. Or problems further
    // down the line when they get prefetched and the mapping gets updated with the new path.
    derivativesCollection.insertMany(derivatives).toFutureOption()
  }

}
