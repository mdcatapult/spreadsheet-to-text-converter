package io.mdcatapult.doclib.consumers

import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.ActorMaterializer
import akka.testkit.{ImplicitSender, TestKit}
import com.mongodb.async.client.{MongoCollection ⇒ JMongoCollection}
import com.spingo.op_rabbit.properties.MessageProperty
import com.typesafe.config.{Config, ConfigFactory}
import io.mdcatapult.doclib.handlers.SpreadsheetHandler
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg}
import io.mdcatapult.doclib.models.{Derivative, DoclibDoc}
import io.mdcatapult.doclib.util.MongoCodecs
import io.mdcatapult.klein.queue.Queue
import org.bson.{BsonString, BsonValue}
import org.bson.codecs.configuration.CodecRegistry
import org.bson.types.ObjectId
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.Updates.{combine, set}
import org.mongodb.scala.result.UpdateResult
import org.mongodb.scala.{MongoCollection, SingleObservable}
import org.scalamock.scalatest.MockFactory
import org.scalatest.FlatSpecLike
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}

class ConsumerSpreadsheetConverterSpec extends TestKit(ActorSystem("SpreadsheetConverterSpec", ConfigFactory.parseString(
  """
  akka.loggers = ["akka.testkit.TestEventListener"]
  """))) with ImplicitSender with FlatSpecLike with MockFactory with ScalaFutures {

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

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executor: ExecutionContextExecutor = system.getDispatcher

  implicit val mongoCodecs: CodecRegistry = MongoCodecs.get
  val wrappedCollection: JMongoCollection[DoclibDoc] = stub[JMongoCollection[DoclibDoc]]

  // Fake class for mongo db since mocking it is soooooooooo hard
  class MI extends MongoCollection[DoclibDoc](wrappedCollection) {
    override def updateOne(filter: Bson, update: Bson): SingleObservable[UpdateResult] = {
      //TODO pull the id out of the filter and return it as the upserted id
      SingleObservable(new UpdateResult(

      ) {
        override def wasAcknowledged(): Boolean = true

        override def getMatchedCount: Long = ???

        override def isModifiedCountAvailable: Boolean = ???

        override def getModifiedCount: Long = 1

        override def getUpsertedId: BsonValue = new BsonString("45678")
      })
    }
  }

  implicit val collection: MongoCollection[DoclibDoc] = new MI

  // Fake the queues, we are not interacting with them
  class QP extends Queue[PrefetchMsg](name = "prefetch-message-queue") {

    // keep track of number of messages sent
    val sent: AtomicInteger = new AtomicInteger(0)
    override def send(envelope: PrefetchMsg,  properties: Seq[MessageProperty] = Seq.empty): Unit = {
      sent.set(sent.get() + 1)
    }
  }

  class QD extends Queue[DoclibMsg](name = "doclib-message-queue") {

  }

  val downstream = mock[QP]
  val upstream = mock[QD]

  val spreadsheetHandler = new SpreadsheetHandler(downstream, upstream)

  val validDoc = DoclibDoc(
    _id = new ObjectId("5d970056b3e8083540798f90"),
    source = "local/resources/test.csv",
    hash = "01234567890",
    mimetype = "text/csv",
    created = LocalDateTime.parse("2019-10-01T12:00:00"),
    updated = LocalDateTime.parse("2019-10-01T12:00:01")
  )

  val invalidDoc = DoclibDoc(
    _id = new ObjectId("5d970056b3e8083540798f90"),
    source = "local/resources/test.csv",
    hash = "01234567890",
    mimetype = "text/plain",
    created = LocalDateTime.parse("2019-10-01T12:00:00"),
    updated = LocalDateTime.parse("2019-10-01T12:00:01")
  )

  "A doclib doc with a valid mimetype" should "be validated" in {
    assert(spreadsheetHandler.validateMimetype(validDoc).get == true)

  }

  "A doclib doc with an valid mimetype" should "not be validated" in {
    val caught = intercept[Exception] {
      spreadsheetHandler.validateMimetype(invalidDoc)
    }
    assert(caught.getMessage == "Document mimetype is not recognised")
  }


  "Spreadsheet handler" should "create a target path from a doclib doc source" in {
    val result = spreadsheetHandler.getTargetPath(validDoc.source, config.getString("convert.to.path"), Some("spreadsheet_conv"))
    assert(result == "ingress/derivatives/resources/spreadsheet_conv-test.csv")
  }

  "It" should "save a document in the mongo collection" in {
    val id = new ObjectId()
    // Just testing that the persist attempts to update the collection using the fake mongodb implementation
    val result = Await.result(spreadsheetHandler.persist(id.toString, combine(
      set("derivatives", List[Derivative]()))
    ), 5 seconds)
    assert(result.get.wasAcknowledged == true)
    assert(result.get.getModifiedCount == 1)
    // The mock class has "45678" so it confirms that this was called
    assert(result.get.getUpsertedId.asString.getValue == "45678")
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
    val mySpreadsheetHandler = new SpreadsheetHandler(downstream, upstream)
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
    val mySpreadsheetHandler = new SpreadsheetHandler(qp, upstream)
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
