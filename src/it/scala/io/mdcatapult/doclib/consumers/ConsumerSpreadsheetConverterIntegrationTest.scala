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
import io.mdcatapult.doclib.models.{AppConfig, DoclibDoc, ParentChildMapping}
import io.mdcatapult.doclib.codec.MongoCodecs
import io.mdcatapult.klein.mongo.Mongo
import io.mdcatapult.klein.queue.Sendable
import io.mdcatapult.util.concurrency.SemaphoreLimitedExecution
import io.mdcatapult.util.path.DirectoryDeleter.deleteDirectories
import org.bson.codecs.configuration.CodecRegistry
import org.bson.types.ObjectId
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.model.Filters.{equal => Mequal}
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Await
import scala.concurrent.duration._

class ConsumerSpreadsheetConverterIntegrationTest extends TestKit(ActorSystem("SpreadsheetConverterSpec", ConfigFactory.parseString(
  """
  akka.loggers = ["akka.testkit.TestEventListener"]
  """)))
  with ImplicitSender
  with AnyFlatSpecLike
  with Matchers
  with MockFactory
  with ScalaFutures
  with BeforeAndAfterAll {

  val sheets: Map[String, Int] = Map[String, Int]( "/test.csv" -> 1, "/test.xls" -> 2, "/test.xlsx" -> 2, "test.ods" -> 2, "/test.xlsx" -> 3)

  implicit val config: Config = ConfigFactory.load()

  import system.dispatcher

  implicit val codecs: CodecRegistry = MongoCodecs.get
  val mongo: Mongo = new Mongo()

  implicit val collection: MongoCollection[DoclibDoc] = mongo.getCollection(config.getString("mongo.doclib-database"), config.getString("mongo.documents-collection"))
  implicit val derivativesCollection: MongoCollection[ParentChildMapping] = mongo.getCollection(config.getString("mongo.doclib-database"), config.getString("mongo.derivative-collection"))

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
  private val readLimiter = SemaphoreLimitedExecution.create(config.getInt("mongo.read-limit"))
  private val writeLimiter = SemaphoreLimitedExecution.create(config.getInt("mongo.write-limit"))

  implicit val consumerNameAndQueue: AppConfig =
    AppConfig(
      config.getString("consumer.name"),
      config.getInt("consumer.concurrency"),
      config.getString("consumer.queue"),
      Option(config.getString("consumer.exchange"))
    )

  private val spreadsheetHandler = SpreadsheetHandler.withWriteToFilesystem(downstream, upstream, readLimiter, writeLimiter)

  "A spreadsheet can be converted and" should "be validated" in {
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

  "The handler" should "create parent child mappings" in {
    val parentID = new ObjectId
    val mappingOneID = UUID.randomUUID
    val mappingTwoID = UUID.randomUUID
    val childOneID = new ObjectId
    val childTwoId = new ObjectId
    val childOnePath = "/a/path/to/file1.txt"
    val childTwoPath = "/a/path/to/file2.txt"
    val mappingOne = ParentChildMapping(_id = mappingOneID, parent = parentID, child = Some(childOneID), childPath = childOnePath, consumer = Some("consumer"))
    val mappingTwo = ParentChildMapping(_id = mappingTwoID, parent = parentID, child = Some(childTwoId), childPath = childTwoPath, consumer = Some("consumer"))
    val parentChildMappings = List[ParentChildMapping](mappingOne, mappingTwo)
    val result = Await.result(spreadsheetHandler.persist(parentChildMappings), 5.seconds)

    assert(result.exists(_.wasAcknowledged()))
    val findOne = Await.result(derivativesCollection.find(Mequal("_id", mappingOneID)).toFuture(), 5.seconds)
    assert(findOne.head == mappingOne)
    val findTwo = Await.result(derivativesCollection.find(Mequal("_id", mappingTwoID)).toFuture(), 5.seconds)
    assert(findTwo.head == mappingTwo)
  }

  "Processing a sheet" can "timeout" in {
    val path = new File("local", "/difficult.xls")
    val doc = DoclibDoc(
      _id = new ObjectId("5d970056b3e8083540798f90"),
      source = path.toString,
      hash = "01234567890",
      mimetype = "text/csv",
      created = LocalDateTime.parse("2019-10-01T12:00:00"),
      updated = LocalDateTime.parse("2019-10-01T12:00:01")
    )

    val thrown = intercept[Exception] {
      spreadsheetHandler.process(doc)
    }
    assert(thrown.getMessage === "Circuit Breaker Timed out.")
  }

  override def beforeAll(): Unit = {
    Await.result(collection.drop().toFuture(), 5.seconds)
    Await.result(derivativesCollection.drop().toFuture(), 5.seconds)
  }


  override def afterAll(): Unit = {
    // These may or may not exist but are all removed anyway
    deleteDirectories(List(pwd/"test-assets"/"ingress"))
  }

}
