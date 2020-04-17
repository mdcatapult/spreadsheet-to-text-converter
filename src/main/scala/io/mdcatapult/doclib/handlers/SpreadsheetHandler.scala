package io.mdcatapult.doclib.handlers

import better.files.{File => ScalaFile}
import cats.data.OptionT
import cats.implicits._
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import io.mdcatapult.doclib.exception.DoclibDocException
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg, SupervisorMsg}
import io.mdcatapult.doclib.models.metadata.{MetaString, MetaValueUntyped}
import io.mdcatapult.doclib.models.{Derivative, DoclibDoc, DoclibDocExtractor, Origin}
import io.mdcatapult.doclib.tabular.{Document => TabularDoc}
import io.mdcatapult.doclib.util.DoclibFlags
import io.mdcatapult.klein.queue.Sendable
import org.bson.conversions.Bson
import org.bson.types.ObjectId
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.model.Filters.equal
import org.mongodb.scala.model.Updates.{combine, set}
import org.mongodb.scala.result.UpdateResult

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object SpreadsheetHandler {

  def withWriteToFilesystem(
             downstream: Sendable[PrefetchMsg],
             supervisor: Sendable[SupervisorMsg],
           )
           (implicit ex: ExecutionContext,
            config: Config,
            collection: MongoCollection[DoclibDoc]): SpreadsheetHandler =
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
                         collection: MongoCollection[DoclibDoc]) extends LazyLogging {

  private val docExtractor = DoclibDocExtractor()

  private val overwriteDerivatives = config.getBoolean("doclib.overwriteDerivatives")

  case class MimetypeNotAllowed(doc: DoclibDoc,
                                cause: Throwable = None.orNull)
    extends DoclibDocException(
      doc,
      s"Document: ${doc._id.toHexString} - Mimetype '${doc.mimetype}' not allowed'",
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
      derivatives <- OptionT.pure[Future](mergeDerivatives(doc, paths))
      _ <- OptionT(
        persist(
          msg.id,
          combine(
            set("derivatives", derivatives)
          )
        ).andThen({
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
      ),
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
   * merge list of new paths into existing paths in DoclibDoc
   * @param doc DoclibDoc
   * @param derivatives List[String]
   * @return List[Derivative] unique list of derivatives
   */
  def mergeDerivatives(doc: DoclibDoc, derivatives: List[String]): List[Derivative] = {
    val newDerivatives = derivatives.map(d => Derivative(`type` = "spreadsheet_conversion", path = d))

    if (overwriteDerivatives) {
      newDerivatives
    } else {
      (newDerivatives ::: doc.derivatives.getOrElse(Nil)).distinct
    }
  }

  def persist(id: String, update: Bson): Future[Option[UpdateResult]] =
    collection.updateOne(equal("_id", new ObjectId(id)), update).toFutureOption()

}
