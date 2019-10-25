package io.mdcatapult.doclib.consumers

import java.nio.file.Paths
import java.time.LocalDateTime

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import io.mdcatapult.doclib.models.DoclibDoc
import io.mdcatapult.doclib.util.MongoCodecs
import org.bson.codecs.configuration.CodecRegistry
import org.bson.types.ObjectId
import com.mongodb.async.client.{MongoCollection ⇒ JMongoCollection}
import io.mdcatapult.doclib.handlers.SpreadsheetHandler
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg}
import io.mdcatapult.klein.queue.Queue
import org.mongodb.scala.MongoCollection
import org.scalamock.scalatest.MockFactory
import org.scalatest.FlatSpecLike
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.ExecutionContextExecutor

class ConsumerSpreadsheetConverterIntegrationTest extends TestKit(ActorSystem("SpreadsheetConverterSpec", ConfigFactory.parseString(
  """
  akka.loggers = ["akka.testkit.TestEventListener"]
  """))) with ImplicitSender with FlatSpecLike with MockFactory with ScalaFutures {

  val sheets: Map[String, Int] = Map[String, Int]("/test.csv" → 1, "/test.xls" → 2, "/test.xlsx" → 2)

  implicit val config: Config = ConfigFactory.parseString(
    """
      |doclib {
      |  root: "test"
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
      |output {
      |  format: "tsv"
      |  baseDirectory: "test/derivatives"
      |}
    """.stripMargin)

  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executor: ExecutionContextExecutor = system.getDispatcher

  implicit val mongoCodecs: CodecRegistry = MongoCodecs.get
  val wrappedCollection: JMongoCollection[DoclibDoc] = stub[JMongoCollection[DoclibDoc]]
  implicit val collection: MongoCollection[DoclibDoc] = MongoCollection[DoclibDoc](wrappedCollection)


  // Fake the queues, we are not interacting with them
  class QP extends Queue[PrefetchMsg](name = "prefetch-message-queue") {

  }

  class QD extends Queue[DoclibMsg](name = "doclib-message-queue") {

  }

  val downstream = mock[QP]
  val upstream = mock[QD]

  val spreadsheetHandler = new SpreadsheetHandler(downstream, upstream)

  "A spreadsheet can be converted" should "be validated" in {
    sheets.foreach(x ⇒ {
      val testFile = getClass.getResource(x._1)
      val path = Paths.get(testFile.toURI)
      println(path)
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

}
