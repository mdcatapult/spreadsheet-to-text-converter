package io.mdcatapult.doclib.consumers

import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import com.mongodb.reactivestreams.client.{MongoCollection => JMongoCollection}
import com.spingo.op_rabbit.properties.MessageProperty
import com.typesafe.config.{Config, ConfigFactory}
import io.mdcatapult.doclib.handlers.SpreadsheetHandler
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg, SupervisorMsg}
import io.mdcatapult.doclib.models.{Derivative, DoclibDoc, ParentChildMapping}
import io.mdcatapult.doclib.util.MongoCodecs
import io.mdcatapult.klein.queue.Sendable
import org.bson.codecs.configuration.CodecRegistry
import org.bson.types.ObjectId
import org.mongodb.scala.MongoCollection
import org.scalamock.matchers.Matchers
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike

class ConsumerSpreadsheetConverterSpec extends TestKit(ActorSystem("SpreadsheetConverterSpec", ConfigFactory.parseString(
  """
  akka.loggers = ["akka.testkit.TestEventListener"]
  """))) with ImplicitSender with AnyFlatSpecLike with Matchers with MockFactory with ScalaFutures {

  // Note: we are going to overwrite this in a later test so var not val.
  implicit var config: Config = ConfigFactory.parseString(
    """
      |doclib {
      |  root: "test-assets"
      |  flag: "tabular.totsv"
      |  overwriteDerivatives: false
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
      |}
      |convert {
      |  format: "tsv"
      |  to: {
      |    path: "derivatives"
      |  }
      |}
      |mongo {
      |  database: "prefetch-test"
      |  collection: "documents"
      |  connection {
      |    username: "doclib"
      |    password: "doclib"
      |    database: "admin"
      |    hosts: ["localhost"]
      |  }
      |}
    """.stripMargin)

  import system.dispatcher

  implicit val mongoCodecs: CodecRegistry = MongoCodecs.get
  val wrappedCollection: JMongoCollection[DoclibDoc] = mock[JMongoCollection[DoclibDoc]]
  implicit val collection: MongoCollection[DoclibDoc] = MongoCollection[DoclibDoc](wrappedCollection)

  val wrappedDerivativesCollection: JMongoCollection[ParentChildMapping] = mock[JMongoCollection[ParentChildMapping]]
  implicit val derivativesCollection: MongoCollection[ParentChildMapping] = MongoCollection[ParentChildMapping](wrappedDerivativesCollection)

  // Fake the queues, we are not interacting with them
  class QP extends Sendable[PrefetchMsg] {
    val name = "prefetch-message-queue"
    val rabbit: ActorRef = testActor
    val sent: AtomicInteger = new AtomicInteger(0)
    def send(envelope: PrefetchMsg,  properties: Seq[MessageProperty] = Seq.empty): Unit = {
      sent.set(sent.get() + 1)
    }
  }

  class QD extends Sendable[DoclibMsg] {
    val name = "doclib-message-queue"
    val rabbit: ActorRef = testActor
    val sent: AtomicInteger = new AtomicInteger(0)
    def send(envelope: DoclibMsg,  properties: Seq[MessageProperty] = Seq.empty): Unit = {
      sent.set(sent.get() + 1)
    }
  }

  class QS extends Sendable[SupervisorMsg] {
    val name = "doclib-message-queue"
    val rabbit: ActorRef = testActor
    val sent: AtomicInteger = new AtomicInteger(0)

    def send(envelope: SupervisorMsg,  properties: Seq[MessageProperty] = Seq.empty): Unit = {
      sent.set(sent.get() + 1)
    }
  }

  private val downstream = mock[QP]
  private val supervisor = mock[QS]

  private val spreadsheetHandler = SpreadsheetHandler.withWriteToFilesystem(downstream, supervisor)

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
    assert(spreadsheetHandler.validateMimetype(validDoc).get)

  }

  "A doclib doc with an invalid mimetype" should "not be validated" in {
    val caught = intercept[Exception] {
      spreadsheetHandler.validateMimetype(invalidDoc)
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
        |}
        |convert {
        |  format: "tsv"
        |  to: {
        |    path: "derivatives"
        |  }
        |}
        |mongo {
        |  database: "prefetch-test"
        |  collection: "documents"
        |  connection {
        |    username: "doclib"
        |    password: "doclib"
        |    database: "admin"
        |    hosts: ["localhost"]
        |  }
        |}
    """.stripMargin)

    val mySpreadsheetHandler = SpreadsheetHandler.withWriteToFilesystem(downstream, supervisor)

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

    val mySpreadsheetHandler = SpreadsheetHandler.withWriteToFilesystem(qp, supervisor)

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
