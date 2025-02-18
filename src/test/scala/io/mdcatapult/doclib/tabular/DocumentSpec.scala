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

package io.mdcatapult.doclib.tabular

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.TestKit
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.nio.file.Paths

class DocumentSpec extends TestKit(ActorSystem("SpreadsheetConverterSpec", ConfigFactory.parseString(
  """
  akka.loggers = ["akka.testkit.TestEventListener"]
  """))) with AnyFlatSpecLike with Matchers {

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
      |totsv {
      |  max-timeout: 10000
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

  val expected = "\"Row1Cell1\"\t\"Row1Cell2\"\t\"Row1Cell3\"\n\"Row2Cell1\"\t\t\"Row2Cell3\"\n\n\"FORMULA\"\t3\t2.1\n\"Cell with \\\"quoted\\\" text\"\n"

  val odsSheet0 = "Row1Cell1\tRow1Cell2\tRow1Cell3\nRow2Cell1\t\tRow2Cell3\nFORMULA\t3.0\t2.1\nCell with \"quoted\" text\t\t\n"
  val odsSheet1 = "Cell1Row1\tCell2Row1\tCell3Row1\nCell1Row2\tCell2Row2\tCell3Row2\nFORMULA\t6.0\t\n"

  private def convert(testFileName: String): List[Sheet] = {
    val testFile = getClass.getResource(testFileName)
    val path = Paths.get(testFile.toURI)

    new Document(path).convertTo("tsv").get
  }

  "A Document" should "parse an XSLX document" in {
    val result = convert("/test.xlsx")

    result should have length 2
    result.head.content should be (expected)
  }

  it should "parse an XLS document" in {
    val result = convert("/test.xls")

    result should have length 2
    result.head.content should be (expected)
  }

  it should "parse an ODS document" in {
    val result = convert("/test.ods")

    result should have length 2
    result.head.content should be (odsSheet0)
    result(1).content should be (odsSheet1)
  }

  it should "parse a CSV document" in {
    val result = convert("/test.csv")

    result should have length 1
    result.head.content should be (odsSheet0.replaceAll("\t\t\n$", ""))
  }

  it should "parse an XLSX document that has an .xls extension" in {
    val result = convert("/misnamed-xslx-test.xls")

    result should have length 2
    result.head.content should be (expected)
  }

  it should "parse an XLS document that has an .xlsx extension" in {
    val result = convert("/misnamed-xsl-test.xlsx")

    result should have length 2
    result.head.content should be (expected)
  }

}
