package io.mdcatapult.doclib.consumers

import java.time.LocalDateTime

import io.mdcatapult.doclib.models.DoclibDoc
import org.bson.types.ObjectId
import org.scalatest.FlatSpec

class ConsumerSpreadsheetConverterSpec extends FlatSpec {

  val validDoc =   DoclibDoc(
    _id = new ObjectId("5d970056b3e8083540798f90"),
    source = "/path/to/file.txt",
    hash = "01234567890",
    mimetype = "text/csv",
    created = LocalDateTime.parse("2019-10-01T12:00:00"),
    updated = LocalDateTime.parse("2019-10-01T12:00:01")
  )

  val invalidDoc =   DoclibDoc(
    _id = new ObjectId("5d970056b3e8083540798f90"),
    source = "/path/to/file.txt",
    hash = "01234567890",
    mimetype = "text/plain",
    created = LocalDateTime.parse("2019-10-01T12:00:00"),
    updated = LocalDateTime.parse("2019-10-01T12:00:01")
  )

  "A doclib doc with a valid mimetype" should "be validated" in {
    assert(ConsumerSpreadsheetConverter.validateMimetype(validDoc).get == true)
  }

  "A doclib doc with an valid mimetype" should "not be validated" in {
    val caught = intercept[Exception] {
      ConsumerSpreadsheetConverter.validateMimetype(invalidDoc)
    }
    assert(caught.getMessage == "Document mimetype is not recognised")
  }
}
