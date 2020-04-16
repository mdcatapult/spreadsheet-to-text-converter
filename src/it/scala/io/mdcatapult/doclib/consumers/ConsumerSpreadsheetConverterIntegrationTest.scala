package io.mdcatapult.doclib.consumers

import java.io.File
import java.time.LocalDateTime
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import better.files.Dsl.pwd
import better.files.{File => ScalaFile, _}
import com.spingo.op_rabbit.properties.MessageProperty
import com.typesafe.config.{Config, ConfigFactory}
import io.mdcatapult.doclib.handlers.SpreadsheetHandler
import io.mdcatapult.doclib.messages.{PrefetchMsg, SupervisorMsg}
import io.mdcatapult.doclib.models.{DoclibDoc, ParentChildMapping}
import io.mdcatapult.doclib.util.{DirectoryDelete, MongoCodecs}
import io.mdcatapult.klein.mongo.Mongo
import io.mdcatapult.klein.queue.Sendable
import org.bson.codecs.configuration.CodecRegistry
import org.bson.types.ObjectId
import org.mongodb.scala.model.Filters.{equal => Mequal}
import org.mongodb.scala.{Completed, MongoCollection}
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.Await
import scala.concurrent.duration._

class ConsumerSpreadsheetConverterIntegrationTest extends TestKit(ActorSystem("SpreadsheetConverterSpec", ConfigFactory.parseString(
  """
  akka.loggers = ["akka.testkit.TestEventListener"]
  """))) with ImplicitSender with AnyFlatSpecLike with MockFactory with ScalaFutures with BeforeAndAfterAll with DirectoryDelete {

  val sheets: Map[String, Int] = Map[String, Int]( "/test.csv" -> 1, "/test.xls" -> 2, "/test.xlsx" -> 2, "test.ods" -> 2)

  implicit val config: Config = ConfigFactory.parseString(
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
      |  database: "spreadsheet-test"
      |  collection: "documents"
      |  derivative_collection: "derivatives"
      |  connection {
      |    username: "doclib"
      |    password: "doclib"
      |    database: "admin"
      |    hosts: ["localhost"]
      |  }
      |}
    """.stripMargin)

  import system.dispatcher

  implicit val codecs: CodecRegistry = MongoCodecs.get
  val mongo: Mongo = new Mongo()

  implicit val collection: MongoCollection[DoclibDoc] = mongo.database.getCollection(config.getString("mongo.collection"))
  implicit val derivativesCollection: MongoCollection[ParentChildMapping] = mongo.database.getCollection(config.getString("mongo.derivative_collection"))

  // Fake the queues, we are not interacting with them
  class QP extends Sendable[PrefetchMsg] {
    override val name: String = "prefetch-message-queue"
    override val rabbit: ActorRef = testActor
    val sent: AtomicInteger = new AtomicInteger(0)

    def send(envelope: PrefetchMsg,  properties: Seq[MessageProperty] = Seq.empty): Unit = {
      sent.set(sent.get() + 1)
    }
  }

  class QS extends Sendable[SupervisorMsg] {
    override val name: String = "doclib-message-queue"
    override val rabbit: ActorRef = testActor
    val sent: AtomicInteger = new AtomicInteger(0)

    def send(envelope: SupervisorMsg,  properties: Seq[MessageProperty] = Seq.empty): Unit = {
      sent.set(sent.get() + 1)
    }
  }

  private val downstream = mock[QP]
  private val upstream = mock[QS]

  val spreadsheetHandler = new SpreadsheetHandler(downstream, upstream)

  "A spreadsheet can be converted" should "be validated" in {
    sheets.foreach(x => {
      val path = new File("local", x._1)
      val doc = DoclibDoc(
        _id = new ObjectId("5d970056b3e8083540798f90"),
        source = path.toString,
        hash = "01234567890",
        mimetype = "text/csv",
        created = LocalDateTime.parse("2019-10-01T12:00:00"),
        updated = LocalDateTime.parse("2019-10-01T12:00:01")
      )
      val res = spreadsheetHandler.process(doc)
      assert(res.length == x._2)
    })
  }

  "A converted sheet" should "be in local temp dir" in {
    sheets.foreach(x => {
      val path = new File("local", x._1)
      val doc = DoclibDoc(
        _id = new ObjectId("5d970056b3e8083540798f90"),
        source = path.toString,
        hash = "01234567890",
        mimetype = "text/csv",
        created = LocalDateTime.parse("2019-10-01T12:00:00"),
        updated = LocalDateTime.parse("2019-10-01T12:00:01")
      )
      val res = spreadsheetHandler.process(doc)
      val absPath: ScalaFile = config.getString("doclib.root")/""
      res.map(sheet => assert(!sheet.startsWith(absPath.toString())))
    })
  }

  "It" should "save parent child mappings in the mongo derivatives collection" in {
    val id = new ObjectId()
    // Just testing that the persist attempts to update the collection using the fake mongodb implementation
    val mappingOneID = UUID.randomUUID
    val mappingTwoID = UUID.randomUUID
    val parentID = new ObjectId
    val childOneID = new ObjectId
    val childTwoId = new ObjectId
    val childOnePath = "/a/path/to/file1.txt"
    val childTwoPath = "/a/path/to/file2.txt"
    val mappingOne = ParentChildMapping(_id = mappingOneID, parent = parentID, child = Some(childOneID), childPath = childOnePath, consumer = Some("consumer"))
    val mappingTwo = ParentChildMapping(_id = mappingTwoID, parent = parentID, child = Some(childTwoId), childPath = childTwoPath, consumer = Some("consumer"))
    val parentChildMappings = List[ParentChildMapping](mappingOne, mappingTwo)
    val result = Await.result(spreadsheetHandler.persist(id, parentChildMappings), 5.seconds)
    assert(result.get.isInstanceOf[Completed])
    assert(result.get.toString == "The operation completed successfully")
    val findOne = Await.result(derivativesCollection.find(Mequal("_id", mappingOneID)).toFuture(), 5.seconds)
    assert(findOne.head == mappingOne)
    val findTwo = Await.result(derivativesCollection.find(Mequal("_id", mappingTwoID)).toFuture(), 5.seconds)
    assert(findTwo.head == mappingTwo)
  }


  override def afterAll(): Unit = {
    // These may or may not exist but are all removed anyway
    deleteDirectories(List(pwd/"test-assets"/"ingress"))
  }

}
