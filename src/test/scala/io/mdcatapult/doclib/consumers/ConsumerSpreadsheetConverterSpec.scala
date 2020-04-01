package io.mdcatapult.doclib.consumers

import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import com.mongodb.async.client.{MongoCollection => JMongoCollection}
import com.spingo.op_rabbit.properties.MessageProperty
import com.typesafe.config.{Config, ConfigFactory}
import io.mdcatapult.doclib.handlers.SpreadsheetHandler
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg, SupervisorMsg}
import io.mdcatapult.doclib.models.{Derivative, DoclibDoc}
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

  val spreadsheetHandler = new SpreadsheetHandler(downstream, supervisor)

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


  "Spreadsheet handler" should "create a target path from a doclib doc source" in {
    val result = spreadsheetHandler.getTargetPath(validDoc.source, config.getString("convert.to.path"), Some("spreadsheet_conv"))
    assert(result == "ingress/derivatives/resources/spreadsheet_conv-test.csv")
  }

  "A derivative" should "be ingested into doclib-root/temp-dir" in {
    val source = "local/test.csv"
    val target = spreadsheetHandler.getTargetPath(source, config.getString("convert.to.path"), Some("spreadsheet_conv"))
    assert(target == "ingress/derivatives/spreadsheet_conv-test.csv")
  }

  "A spreadsheet source" should "have a target path within doclib.temp-dir" in {
    val source = "remote/test.csv"
    val target = spreadsheetHandler.getTargetPath(source, config.getString("convert.to.path"), Some("spreadsheet_conv"))
    assert(target == s"${config.getString("doclib.local.temp-dir")}/derivatives/remote/spreadsheet_conv-test.csv")
  }

  "An existing derivative" should "only have derivative once in the path" in {
    val source = "local/derivatives/test.csv"
    val target = spreadsheetHandler.getTargetPath(source, config.getString("convert.to.path"), Some("spreadsheet_conv"))
    assert(target == "ingress/derivatives/spreadsheet_conv-test.csv")
  }

  "A path with multiple derivative segments" should "only have derivative once in the path" in {
    val source = "local/derivatives/derivatives/derivatives/test.csv"
    val target = spreadsheetHandler.getTargetPath(source, config.getString("convert.to.path"), Some("spreadsheet_conv"))
    assert(target == "ingress/derivatives/spreadsheet_conv-test.csv")
  }

  "An existing spreadsheet source" should "have a target path within doclib.temp-dir" in {
    val source = "local/derivatives/remote/test.csv"
    val target = spreadsheetHandler.getTargetPath(source, config.getString("convert.to.path"), Some("spreadsheet_conv"))
    assert(target == s"${config.getString("doclib.local.temp-dir")}/derivatives/remote/spreadsheet_conv-test.csv")
  }

  "A list of derivatives and a list of paths" can "be merged" in {
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
    val a = spreadsheetHandler.mergeDerivatives(derDoc, derivativePaths)
    assert(a.length == 4)
  }

  "Derivatives" should "be overwritten if flag set" in {
    config = ConfigFactory.parseString(
      """
        |doclib {
        |  root: "test-assets"
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
    val mySpreadsheetHandler = new SpreadsheetHandler(downstream, supervisor)
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
    val a = mySpreadsheetHandler.mergeDerivatives(derDoc, derivativePaths)
    assert(a.length == 2)
  }

  "Enqueue" should "send a message to the prefetch queue" in {
    val qp = new QP
    val mySpreadsheetHandler = new SpreadsheetHandler(qp, supervisor)
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
}
