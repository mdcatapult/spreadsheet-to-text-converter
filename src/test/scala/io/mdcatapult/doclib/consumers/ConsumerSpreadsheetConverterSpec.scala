package io.mdcatapult.doclib.consumers

import java.time.LocalDateTime

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg}
import io.mdcatapult.doclib.models.DoclibDoc
import io.mdcatapult.klein.queue.Queue
import io.mdcatapult.doclib.handlers.SpreadsheetHandler
import org.bson.types.ObjectId
import org.scalamock.scalatest.MockFactory
import org.scalatest.FlatSpecLike
import com.mongodb.async.client.{MongoCollection ⇒ JMongoCollection}
import io.mdcatapult.doclib.util.MongoCodecs
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.MongoCollection

import scala.concurrent.ExecutionContextExecutor

class ConsumerSpreadsheetConverterSpec extends TestKit(ActorSystem("SpreadsheetConverterSpec", ConfigFactory.parseString(
  """
  akka.loggers = ["akka.testkit.TestEventListener"]
  """))) with ImplicitSender with FlatSpecLike with MockFactory {

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

  val validDoc = DoclibDoc(
    _id = new ObjectId("5d970056b3e8083540798f90"),
    source = "test/resources/test.csv",
    hash = "01234567890",
    mimetype = "text/csv",
    created = LocalDateTime.parse("2019-10-01T12:00:00"),
    updated = LocalDateTime.parse("2019-10-01T12:00:01")
  )

  val invalidDoc = DoclibDoc(
    _id = new ObjectId("5d970056b3e8083540798f90"),
    source = "test/resources/test.csv",
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


  "It" should "do something" in {
    val result = spreadsheetHandler.getTargetPath(validDoc.source)
    assert(result == "test/derivatives/resources/test/")
  }
}
