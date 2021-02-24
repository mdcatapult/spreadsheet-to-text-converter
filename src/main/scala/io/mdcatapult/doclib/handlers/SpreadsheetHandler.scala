package io.mdcatapult.doclib.handlers

import better.files.{File => ScalaFile}
import cats.data.OptionT
import cats.implicits._
import com.typesafe.config.Config
import io.mdcatapult.doclib.consumer.{ConsumerHandler, HandlerResultWithDerivatives}
import io.mdcatapult.doclib.flag.MongoFlagContext
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg, SupervisorMsg}
import io.mdcatapult.doclib.models._
import io.mdcatapult.doclib.models.metadata.{MetaString, MetaValueUntyped}
import io.mdcatapult.doclib.tabular.{Document => TabularDoc}
import io.mdcatapult.klein.queue.Sendable
import io.mdcatapult.util.concurrency.LimitedExecution
import io.mdcatapult.util.models.Version
import io.mdcatapult.util.models.result.UpdatedResult
import io.mdcatapult.util.time.nowUtc
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.result.InsertManyResult

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

object SpreadsheetHandler {

  def withWriteToFilesystem(downstream: Sendable[PrefetchMsg],
                            supervisor: Sendable[SupervisorMsg],
                            readLimiter: LimitedExecution,
                            writeLimiter: LimitedExecution)
                           (implicit ex: ExecutionContext,
                            config: Config,
                            collection: MongoCollection[DoclibDoc],
                            derivativesCollection: MongoCollection[ParentChildMapping]): SpreadsheetHandler =
    new SpreadsheetHandler(
      downstream,
      supervisor,
      new ConsumerPaths(),
      new FileSheetWriter(config.getString("convert.format")),
      readLimiter,
      writeLimiter
    )
}

class SpreadsheetHandler(downstream: Sendable[PrefetchMsg],
                         supervisor: Sendable[SupervisorMsg],
                         paths: ConsumerPaths,
                         sheetWriter: SheetWriter,
                         val readLimiter: LimitedExecution,
                         val writeLimiter: LimitedExecution)
                        (implicit ex: ExecutionContext,
                         config: Config,
                         collection: MongoCollection[DoclibDoc],
                         derivativesCollection: MongoCollection[ParentChildMapping]) extends ConsumerHandler[DoclibMsg] {

  private implicit val consumerNameAndQueue: ConsumerNameAndQueue =
    ConsumerNameAndQueue(config.getString("consumer.name"), config.getString("consumer.queue"))

  private val version: Version = Version.fromConfig(config)
  private val flagContext = new MongoFlagContext(consumerNameAndQueue.name, version, collection, nowUtc)

  /**
    * default handler for messages
    *
    * @param msg      DoclibMsg
    * @param exchange String name of exchange message was sourced from
    * @return
    */
  override def handle(msg: DoclibMsg): Future[Option[HandlerResultWithDerivatives]] = {
    logReceived(msg.id)

    val spreadSheetProcess = for {
      doc <- OptionT(findDocById(collection, msg.id))
      if !flagContext.isRunRecently(doc)

      started: UpdatedResult <- OptionT.liftF(flagContext.start(doc))
      _ <- OptionT.fromOption[Future](Mimetypes.validateMimetype(doc))
      paths: List[String] <- OptionT.pure[Future](process(doc))
      if paths.nonEmpty
      derivatives <- OptionT.pure[Future](createDerivativesFromPaths(doc, paths))
      _ <- OptionT(persist(derivatives))
      _ <- OptionT.pure[Future](paths.foreach(path => enqueue(path, doc)))
      _ <- OptionT.liftF(
        flagContext.end(
          doc,
          state = Option(DoclibFlagState(paths.length.toString, nowUtc.now())),
          noCheck = started.changesMade,
        )
      )
    } yield HandlerResultWithDerivatives(doc, Some(paths))

    postHandleProcess(
      messageId = msg.id,
      handlerResult = spreadSheetProcess.value,
      flagContext = flagContext,
      supervisor,
      collection
    )
  }

  /**
    * send new file to prefetch queue
    *
    * @param source String
    * @param doc    Document
    * @return
    */
  def enqueue(source: String, doc: DoclibDoc): String = {
    // Let prefetch know that it is a spreadsheet derivative
    val derivativeMetadata = List[MetaValueUntyped](MetaString("derivative.type", "tabular.totsv"))

    downstream.send(PrefetchMsg(
      source = source,
      tags = doc.tags,
      metadata = Some(doc.metadata.getOrElse(Nil) ::: derivativeMetadata),
      derivative = Some(true),
      origins = Some(List(Origin(
        scheme = "mongodb",
        hostname = None,
        metadata = Some(List[MetaValueUntyped](
          MetaString("db", config.getString("mongo.doclib-database")),
          MetaString("collection", config.getString("mongo.documents-collection")),
          MetaString("_id", doc._id.toString)))))
      )
    ))

    source
  }

  /**
    * generate new converted strings and save to the FS
    *
    * @param doc DoclibDoc
    * @return List[String] list of new paths created
    */
  def process(doc: DoclibDoc): List[String] = {
    val targetPath = paths.getTargetPath(doc.source, Try(config.getString("consumer.name")).toOption)
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
    *
    * @param doc   DoclibDoc
    * @param paths List[String]
    * @return List[Derivative] unique list of derivatives
    */
  def createDerivativesFromPaths(doc: DoclibDoc, paths: List[String]): List[ParentChildMapping] =
    paths.map(d => ParentChildMapping(_id = UUID.randomUUID, childPath = d, parent = doc._id, consumer = Try(config.getString("consumer.name")).toOption))

  def persist(derivatives: List[ParentChildMapping]): Future[Option[InsertManyResult]] = {
    //TODO This assumes that these are all new mappings. They all have unique ids. Could we
    // have problems with them clashing with existing mappings?
    derivativesCollection.insertMany(derivatives).toFutureOption()
  }

}
