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

package io.mdcatapult.doclib.consumers

import org.apache.pekko.Done
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.{ImplicitSender, TestKit}
import com.mongodb.reactivestreams.client.{MongoCollection => JMongoCollection}
import com.rabbitmq.client.AMQP
import com.typesafe.config.{Config, ConfigFactory}
import io.mdcatapult.doclib.codec.MongoCodecs
import io.mdcatapult.doclib.handlers.{Mimetypes, SpreadsheetHandler}
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg, SupervisorMsg}
import io.mdcatapult.doclib.models.{AppConfig, Derivative, DoclibDoc, ParentChildMapping}
import io.mdcatapult.klein.queue.Sendable
import io.mdcatapult.util.concurrency.SemaphoreLimitedExecution
import org.bson.codecs.configuration.CodecRegistry
import org.bson.types.ObjectId
import org.mongodb.scala.MongoCollection
import org.scalamock.matchers.Matchers
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike

import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future
import scala.util.Try

class ConsumerSpreadsheetConverterSpec extends TestKit(ActorSystem("SpreadsheetConverterSpec", ConfigFactory.parseString(
  """
  akka.loggers = ["akka.testkit.TestEventListener"]
  """))) with ImplicitSender with AnyFlatSpecLike with Matchers with MockFactory with ScalaFutures {

  // Note: we are going to overwrite this in a later test so var not val.
  implicit var config: Config = ConfigFactory.parseString(
    """
      |doclib {
      |  root: "test-assets"
      |  local {
      |    target-dir: "local"
      |    temp-dir: "ingress"
      |  }
      |  remote {
      |    target-dir: "remote"
      |    temp-dir: "remote-ingress"
      |  }
      |  archive {
      |    target-dir: "archive"
      |  }
      |  derivative {
      |    target-dir: "derivatives"
      |  }
      |}
      |convert {
      |  format: "tsv"
      |}
      |mongo {
      |  doclib-database: "prefetch-test"
      |  documents-collection: "documents"
      |  connection {
      |    username: "doclib"
      |    password: "doclib"
      |    database: "admin"
      |    hosts: ["localhost"]
      |  }
      |  read-limit = 100
      |  write-limit = 50
      |}
      |version {
      |  number = "2.0.17-SNAPSHOT",
      |  major = 2,
      |  minor =  0,
      |  patch = 17,
      |  hash =  "ca00f0cf"
      |}
      |consumer {
      |  name : spreadsheet-converter
      |  queue : spreadsheet-converter
      |  exchange : doclib
      |  concurrency: 5
      |}
    """.stripMargin)

  import system.dispatcher

  implicit val consumerNameAndQueue: AppConfig =
    AppConfig(
      config.getString("consumer.name"),
      config.getInt("consumer.concurrency"),
      config.getString("consumer.queue"),
      Try(config.getString("consumer.exchange")).toOption
    )

  implicit val mongoCodecs: CodecRegistry = MongoCodecs.get
  val wrappedCollection: JMongoCollection[DoclibDoc] = mock[JMongoCollection[DoclibDoc]]
  implicit val collection: MongoCollection[DoclibDoc] = MongoCollection[DoclibDoc](wrappedCollection)

  val wrappedDerivativesCollection: JMongoCollection[ParentChildMapping] = mock[JMongoCollection[ParentChildMapping]]
  implicit val derivativesCollection: MongoCollection[ParentChildMapping] = MongoCollection[ParentChildMapping](wrappedDerivativesCollection)

  private val readLimiter = SemaphoreLimitedExecution.create(1)
  private val writeLimiter = SemaphoreLimitedExecution.create(1)


  // Fake the queues, we are not interacting with them
  class QP extends Sendable[PrefetchMsg] {
    val name = "prefetch-message-queue"
    val sent: AtomicInteger = new AtomicInteger(0)
    override val persistent: Boolean = false

    override def send(envelope: PrefetchMsg, properties: Option[AMQP.BasicProperties]): Future[Done] = {
      sent.set(sent.get() + 1)
      Future(Done)
    }
  }

  class QD extends Sendable[DoclibMsg] {
    val name = "doclib-message-queue"
    val sent: AtomicInteger = new AtomicInteger(0)
    override val persistent: Boolean = false

    def send(envelope: DoclibMsg, properties: Option[AMQP.BasicProperties]): Future[Done] = {
      sent.set(sent.get() + 1)
      Future(Done)
    }
  }

  class QS extends Sendable[SupervisorMsg] {
    val name = "doclib-message-queue"
    val sent: AtomicInteger = new AtomicInteger(0)
    override val persistent: Boolean = false

    def send(envelope: SupervisorMsg, properties: Option[AMQP.BasicProperties]): Future[Done] = {
      sent.set(sent.get() + 1)
      Future(Done)
    }
  }

  private val downstream = mock[QP]
  private val supervisor = mock[QS]


  private val spreadsheetHandler = SpreadsheetHandler.withWriteToFilesystem(downstream, supervisor, readLimiter, writeLimiter)

  private val validDoc = DoclibDoc(
    _id = new ObjectId("5d970056b3e8083540798f90"),
    source = "local/resources/test.csv",
    hash = "01234567890",
    mimetype = "application/vnd.ms-excel",
    created = LocalDateTime.parse("2019-10-01T12:00:00"),
    updated = LocalDateTime.parse("2019-10-01T12:00:01")
  )

  private val invalidDoc = DoclibDoc(
    _id = new ObjectId("5d970056b3e8083540798f90"),
    source = "local/resources/test.csv",
    hash = "01234567890",
    mimetype = "text/plain",
    created = LocalDateTime.parse("2019-10-01T12:00:00"),
    updated = LocalDateTime.parse("2019-10-01T12:00:01")
  )

  "A doclib doc with a valid mimetype" should "be validated" in {
    assert(Mimetypes.validateMimetype(validDoc).get)
  }

  "A doclib doc with an invalid mimetype" should "not be validated" in {
    val caught = intercept[Exception] {
      Mimetypes.validateMimetype(invalidDoc)
    }
    assert(caught.getMessage == "Document: 5d970056b3e8083540798f90 - Mimetype 'text/plain' not allowed'")
  }

  //"A list of derivatives and a list of paths"
  // This test doesn't really match how we now do derivatives via parent-child mappings.
  // Needs some clarity around what to do with existing mappings.
  ignore can "be merged" in {
    val derivatives: List[Derivative] = List[Derivative](
      Derivative(`type` = "unarchive", path = "ingress/derivatives/remote/a_derivative.txt"),
      Derivative(`type` = "unarchive", path = "ingress/derivatives/remote/another_derivative.txt")
    )
    val derDoc = DoclibDoc(
      _id = new ObjectId("5d970056b3e8083540798f90"),
      source = "local/resources/test.csv",
      hash = "01234567890",
      mimetype = "text/csv",
      derivatives = Some(derivatives),
      created = LocalDateTime.parse("2019-10-01T12:00:00"),
      updated = LocalDateTime.parse("2019-10-01T12:00:01")
    )
    val derivativePaths = List[String]("ingress/derivatives/remote/spreadsheet_conv-test.csv/0_sheet1.tsv", "ingress/derivatives/remote/spreadsheet_conv-test.csv/1_sheet2.tsv")
    val a = spreadsheetHandler.createDerivativesFromPaths(derDoc, derivativePaths)
    assert(a.length == 4)
  }

  "Derivatives" should "be overwritten if flag set" in {
    config = ConfigFactory.parseString(
      """
        |doclib {
        |  root: "test-assets"
        |  flag: "tabular.totsv"
        |  overwriteDerivatives: true
        |  local {
        |    target-dir: "local"
        |    temp-dir: "ingress"
        |  }
        |  remote {
        |    target-dir: "remote"
        |    temp-dir: "remote-ingress"
        |  }
        |  archive {
        |    target-dir: "archive"
        |  }
        |  derivative {
        |    target-dir: derivatives
        |  }
        |}
        |convert {
        |  format: "tsv"
        |}
        |mongo {
        |  doclib-database: "prefetch-test"
        |  documents-collection: "documents"
        |  connection {
        |    username: "doclib"
        |    password: "doclib"
        |    database: "admin"
        |    hosts: ["localhost"]
        |  }
        |  read-limit = 100
        |  write-limit = 50
        |}
        |version {
        |  number = "2.0.17-SNAPSHOT",
        |  major = 2,
        |  minor =  0,
        |  patch = 17,
        |  hash =  "ca00f0cf"
        |}
        |consumer {
        |  name : "spreadsheet-converter"
        |  queue: "spreadsheet-converter"
        |}
    """.stripMargin)

    val mySpreadsheetHandler = SpreadsheetHandler.withWriteToFilesystem(downstream, supervisor, readLimiter, writeLimiter)

    val derivatives: List[Derivative] = List[Derivative](
      Derivative(`type` = "unarchive", path = "ingress/derivatives/remote/a_derivative.txt"),
      Derivative(`type` = "unarchive", path = "ingress/derivatives/remote/another_derivative.txt")
    )
    val derDoc = DoclibDoc(
      _id = new ObjectId("5d970056b3e8083540798f90"),
      source = "local/resources/test.csv",
      hash = "01234567890",
      mimetype = "text/csv",
      derivatives = Some(derivatives),
      created = LocalDateTime.parse("2019-10-01T12:00:00"),
      updated = LocalDateTime.parse("2019-10-01T12:00:01")
    )
    val derivativePaths = List[String]("ingress/derivatives/remote/spreadsheet_conv-test.csv/0_sheet1.tsv", "ingress/derivatives/remote/spreadsheet_conv-test.csv/1_sheet2.tsv")
    val a = mySpreadsheetHandler.createDerivativesFromPaths(derDoc, derivativePaths)
    assert(a.length == 2)
  }

  "Enqueue" should "send a message to the prefetch queue" in {
    val qp = new QP

    val mySpreadsheetHandler = SpreadsheetHandler.withWriteToFilesystem(qp, supervisor, readLimiter, writeLimiter)

    val derivatives: List[Derivative] = List[Derivative](
      Derivative(`type` = "unarchive", path = "ingress/derivatives/remote/a_derivative.txt"),
      Derivative(`type` = "unarchive", path = "ingress/derivatives/remote/another_derivative.txt")
    )
    val derDoc = DoclibDoc(
      _id = new ObjectId("5d970056b3e8083540798f90"),
      source = "local/resources/test.csv",
      hash = "01234567890",
      mimetype = "text/csv",
      derivatives = Some(derivatives),
      created = LocalDateTime.parse("2019-10-01T12:00:00"),
      updated = LocalDateTime.parse("2019-10-01T12:00:01")
    )
    val result = mySpreadsheetHandler.enqueue("ingress/aFile.txt", derDoc)
    assert(result == "ingress/aFile.txt")
    assert(qp.sent.intValue() == 1)
  }

  "A list of parent child derivatives" can "be created from a list of child paths" in {
    val pathList = List[String]("/a/path/1", "/a/path/2")
    val doc = DoclibDoc(
      _id = new ObjectId("5d970056b3e8083540798f90"),
      source = "local/resources/test.csv",
      hash = "01234567890",
      mimetype = "text/csv",
      created = LocalDateTime.parse("2019-10-01T12:00:00"),
      updated = LocalDateTime.parse("2019-10-01T12:00:01")
    )
    val derivatives = spreadsheetHandler.createDerivativesFromPaths(doc, pathList)
    assert(derivatives.length == 2)
    assert(derivatives.exists(p => p.parent == doc._id && pathList.contains(p.childPath)))
  }
}
