/*
 * Copyright 2024 Medicines Discovery Catapult
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mdcatapult.doclib.handlers

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.connectors.amqp.scaladsl.CommittableReadResult
import better.files.{File => ScalaFile}
import cats.data.OptionT
import cats.implicits._
import com.typesafe.config.Config
import io.mdcatapult.doclib.consumer.AbstractHandler
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
import org.mongodb.scala.model.ReplaceOptions
import org.mongodb.scala.result.UpdateResult
import org.mongodb.scala.model.Filters.equal
import play.api.libs.json.Json

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object SpreadsheetHandler {

  def withWriteToFilesystem(downstream: Sendable[PrefetchMsg],
                            supervisor: Sendable[SupervisorMsg],
                            readLimiter: LimitedExecution,
                            writeLimiter: LimitedExecution)
                           (implicit ex: ExecutionContext,
                            config: Config,
                            collection: MongoCollection[DoclibDoc],
                            derivativesCollection: MongoCollection[ParentChildMapping],
                            appConfig: AppConfig,
                            system: ActorSystem): SpreadsheetHandler =
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
                         derivativesCollection: MongoCollection[ParentChildMapping],
                         appConfig: AppConfig,
                         system: ActorSystem) extends AbstractHandler[DoclibMsg, SpreadSheetHandlerResult] {


  private val version: Version = Version.fromConfig(config)
  private val flagContext = new MongoFlagContext(appConfig.name, version, collection, nowUtc)

  /**
    * default handler for messages
    *
    * @param msg      DoclibMsg
    * @return
    */
  override def handle(doclibMsgWrapper: CommittableReadResult): Future[(CommittableReadResult, Try[SpreadSheetHandlerResult])] = {
    Try {
      Json.parse(doclibMsgWrapper.message.bytes.utf8String).as[DoclibMsg]
    } match {
      case Success(msg: DoclibMsg) => {
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
        } yield SpreadSheetHandlerResult(doc, paths)
        val finalResult = spreadSheetProcess.value.transformWith({
          case Success(Some(value: SpreadSheetHandlerResult)) => Future((doclibMsgWrapper, Success(value)))
          case Success(None) => Future((doclibMsgWrapper, Failure(new Exception(s"No spreadsheet result was present for ${msg.id}"))))
          case Failure(e) => Future((doclibMsgWrapper, Failure(e)))
        })

        postHandleProcess(
          documentId = msg.id,
          handlerResult = finalResult,
          flagContext = flagContext,
          supervisor,
          collection
        )
      }
      case Failure(x: Throwable) => Future((doclibMsgWrapper, Failure(new Exception(s"Unable to decode message received. ${x.getMessage}"))))
    }
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

    val d = new TabularDoc(sourceAbsPath)(system, config)

    d.convertTo(sheetWriter.convertToFormat).get
      .filter(_.content.nonEmpty)
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

  /**
   * Insert parent-child mappings, upserting in case there are any clashes with existing mappings

   * @param derivatives
   * @return
   */
  def persist(derivatives: List[ParentChildMapping]): Future[Option[UpdateResult]] = {
    derivatives.map(d => derivativesCollection.replaceOne(equal("_id", d._id), d, ReplaceOptions().upsert(true)).toFutureOption)
      .sequence
      .map(_.flatten)
      .map(_.headOption)
  }

}
