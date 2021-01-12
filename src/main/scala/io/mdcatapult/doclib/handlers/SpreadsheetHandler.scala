package io.mdcatapult.doclib.handlers

import java.util.UUID

import better.files.{File => ScalaFile}
import cats.data.OptionT
import cats.implicits._
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import io.mdcatapult.doclib.ConsumerName
import io.mdcatapult.doclib.exception.DoclibDocException
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg, SupervisorMsg}
import io.mdcatapult.doclib.metrics.Metrics.handlerCount
import io.mdcatapult.doclib.models._
import io.mdcatapult.doclib.models.metadata.{MetaString, MetaValueUntyped}
import io.mdcatapult.doclib.tabular.{Document => TabularDoc}
import io.mdcatapult.doclib.flag.{FlagContext, MongoFlagStore}
import io.mdcatapult.klein.queue.Sendable
import io.mdcatapult.util.time.nowUtc
import io.mdcatapult.util.models.Version
import io.mdcatapult.util.models.result.UpdatedResult
import org.bson.types.ObjectId
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.model.Filters.equal
import org.mongodb.scala.result.{DeleteResult, InsertManyResult}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object SpreadsheetHandler {

  def withWriteToFilesystem(
             downstream: Sendable[PrefetchMsg],
             supervisor: Sendable[SupervisorMsg],
           )
           (implicit ex: ExecutionContext,
            config: Config,
            collection: MongoCollection[DoclibDoc],
            derivativesCollection: MongoCollection[ParentChildMapping],
           ): SpreadsheetHandler =
    new SpreadsheetHandler(
      downstream,
      supervisor,
      new ConsumerPaths(),
      new FileSheetWriter(config.getString("convert.format")),
    )
}

class SpreadsheetHandler(
                          downstream: Sendable[PrefetchMsg],
                          supervisor: Sendable[SupervisorMsg],
                          paths: ConsumerPaths,
                          sheetWriter: SheetWriter,
                        )
                        (implicit ex: ExecutionContext,
                         config: Config,
                         collection: MongoCollection[DoclibDoc],
                         derivativesCollection: MongoCollection[ParentChildMapping]) extends LazyLogging {

  private val docExtractor = DoclibDocExtractor()
  private val version = Version.fromConfig(config)
  private val flags = new MongoFlagStore(version, docExtractor, collection, nowUtc)

  private val overwriteDerivatives = config.getBoolean("doclib.overwriteDerivatives")

  private val knownMimetypes =
    Seq(
      """application/vnd\.lotus.*""".r,
      """application/vnd\.ms-excel.*""".r,
      """application/vnd\.openxmlformats-officedocument.spreadsheetml.*""".r,
      """application/vnd\.stardivision.calc""".r,
      """application/vnd\.sun\.xml\.calc.*""".r,
      """application/vnd\.oasis\.opendocument\.spreadsheet""".r,
    )

  case class MimetypeNotAllowed(doc: DoclibDoc,
                                cause: Throwable = None.orNull)
    extends DoclibDocException(
      doc,
      s"Document: ${doc._id.toHexString} - Mimetype '${doc.mimetype}' not allowed'",
      cause)

  /**
   * default handler for messages
   * @param msg DoclibMsg
   * @param exchange String name of exchange message was sourced from
   * @return
   */
  def handle(msg: DoclibMsg, exchange: String): Future[Option[Any]] = {
    logger.info(f"RECEIVED: ${msg.id}")

    val flagContext: FlagContext = flags.findFlagContext(Some(config.getString("upstream.queue")))

    val spreadSheetProcess = for {
      doc <- OptionT(collection.find(equal("_id", new ObjectId(msg.id))).first().toFutureOption())
      if !docExtractor.isRunRecently(doc)

      started: UpdatedResult <- OptionT.liftF(flagContext.start(doc))
      _ <- OptionT.fromOption[Future](validateMimetype(doc))
      paths: List[String] <- OptionT.pure[Future](process(doc))
      if paths.nonEmpty
      derivatives <- OptionT.pure[Future](createDerivativesFromPaths(doc, paths))
      _ <- OptionT.liftF(deleteExistingDerivatives(doc))
      _ <- OptionT(persist(derivatives))
      _ <- OptionT.pure[Future](paths.foreach(path => enqueue(path, doc)))
      _ <- OptionT.liftF(
        flagContext.end(
          doc,
          state = Option(DoclibFlagState(paths.length.toString, nowUtc.now())),
          noCheck = started.changesMade,
        ))
    } yield (paths, doc)

    spreadSheetProcess.value.andThen {
      case Success(result) => result.map(r => {
        supervisor.send(SupervisorMsg(id = r._2._id.toHexString))
        incrementHandlerCount("success")
        logger.info(f"COMPLETED: ${msg.id} - found & created ${r._1.length} derivatives")
      })
      case Failure(e: DoclibDocException) =>
        incrementHandlerCount("doclib_doc_exception")
        flagContext.error(e.getDoc, noCheck = true).andThen {
          case Failure(e) =>
            incrementHandlerCount("error_attempting_error_flag_write")
            logger.error("error attempting error flag write", e)
        }
      case Failure(_) =>
        handlerCount.labels(ConsumerName, config.getString("upstream.queue"), "unknown_error").inc()

        collection.find(equal("_id", new ObjectId(msg.id))).first().toFutureOption().onComplete {
          case Failure(e) =>
            incrementHandlerCount("error_retrieving_document")
            logger.error(s"error retrieving document", e)
          case Success(value) => value match {
            case Some(foundDoc) =>
              flagContext.error(foundDoc, noCheck = true).andThen {
                case Failure(e) =>
                  incrementHandlerCount("error_attempting_error_flag_write")
                  logger.error("error attempting error flag write", e)
              }
            case None =>
              val message = f"${msg.id} - no document found"
              incrementHandlerCount("error_no_document")
              logger.error(message, new Exception(message))
          }
        }
    }
  }

  private def incrementHandlerCount(labels: String*): Unit = {
    val labelsWithDefaults = Seq(ConsumerName, config.getString("upstream.queue")) ++ labels
    handlerCount.labels(labelsWithDefaults: _*).inc()
  }

  def validateMimetype(doc: DoclibDoc): Option[Boolean] = {
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
    val derivativeMetadata = List[MetaValueUntyped](MetaString("derivative.type", "tabular.totsv"))

    downstream.send(PrefetchMsg(
      source=source,
      tags=doc.tags,
      metadata = Some(doc.metadata.getOrElse(Nil) ::: derivativeMetadata),
      derivative=Some(true),
      origins=Some(List(Origin(
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
   * generate new converted strings and save to the FS
   * @param doc DoclibDoc
   * @return List[String] list of new paths created
   */
  def process(doc: DoclibDoc): List[String] = {
    val targetPath = paths.getTargetPath(doc.source, Some("spreadsheet_conv"))
    val sourceAbsPath = paths.absolutePath(doc.source)

    val d = new TabularDoc(sourceAbsPath)

    d.convertTo(sheetWriter.convertToFormat)
      .filter(_.content.length > 0)
      .map(s => sheetWriter.writeSheet(s, paths.absolutePath(targetPath)))
      .filter(_.path.isDefined)
      .map(_.path.get)
      .map(sheet => {
        val root: ScalaFile = paths.absoluteRootPath
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

  def persist(derivatives: List[ParentChildMapping]): Future[Option[InsertManyResult]] = {
    //TODO This assumes that these are all new mappings. They all have unique ids. Could we
    // have problems with them clashing with existing mappings?
    derivativesCollection.insertMany(derivatives).toFutureOption()
  }

}
