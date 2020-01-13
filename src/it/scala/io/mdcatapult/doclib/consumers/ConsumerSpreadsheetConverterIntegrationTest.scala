package io.mdcatapult.doclib.consumers

import java.io.File
import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.ActorMaterializer
import akka.testkit.{ImplicitSender, TestKit}
import better.files.Dsl.pwd
import better.files.{File ⇒ ScalaFile, _}
import com.mongodb.async.client.{MongoCollection ⇒ JMongoCollection}
import com.spingo.op_rabbit.properties.MessageProperty
import com.typesafe.config.{Config, ConfigFactory}
import io.mdcatapult.doclib.handlers.SpreadsheetHandler
import io.mdcatapult.doclib.messages.{PrefetchMsg, SupervisorMsg}
import io.mdcatapult.doclib.models.{Derivative, DoclibDoc}
import io.mdcatapult.doclib.util.{DirectoryDelete, MongoCodecs}
import io.mdcatapult.klein.queue.Sendable
import org.bson.codecs.configuration.CodecRegistry
import org.bson.conversions.Bson
import org.bson.types.ObjectId
import org.bson.{BsonString, BsonValue}
import org.mongodb.scala.model.Updates.{combine, set}
import org.mongodb.scala.result.UpdateResult
import org.mongodb.scala.{MongoCollection, SingleObservable}
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}

class ConsumerSpreadsheetConverterIntegrationTest extends TestKit(ActorSystem("SpreadsheetConverterSpec", ConfigFactory.parseString(
  """
  akka.loggers = ["akka.testkit.TestEventListener"]
  """))) with ImplicitSender with FlatSpecLike with MockFactory with ScalaFutures with BeforeAndAfterAll with DirectoryDelete {

  val sheets: Map[String, Int] = Map[String, Int]( "/test.csv" → 1, "/test.xls" → 2, "/test.xlsx" → 2, "test.ods" → 2)

  implicit val config: Config = ConfigFactory.parseString(
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
    """.stripMargin)

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executor: ExecutionContextExecutor = system.getDispatcher

  implicit val mongoCodecs: CodecRegistry = MongoCodecs.get
  val wrappedCollection: JMongoCollection[DoclibDoc] = stub[JMongoCollection[DoclibDoc]]
//  implicit val collection: MongoCollection[DoclibDoc] = MongoCollection[DoclibDoc](wrappedCollection)

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

  val downstream = mock[QP]
  val upstream = mock[QS]

  val spreadsheetHandler = new SpreadsheetHandler(downstream, upstream)

  "A spreadsheet can be converted" should "be validated" in {
    sheets.foreach(x ⇒ {
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
    sheets.foreach(x ⇒ {
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
      res.map(sheet ⇒ assert(!sheet.startsWith(absPath.toString())))
    })
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


  override def afterAll(): Unit = {
    // These may or may not exist but are all removed anyway
    deleteDirectories(List((pwd/"test-assets"/"ingress")))
  }

}
