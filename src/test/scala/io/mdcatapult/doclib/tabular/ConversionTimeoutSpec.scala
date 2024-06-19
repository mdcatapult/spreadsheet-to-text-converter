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

class ConversionTimeoutSpec extends TestKit(ActorSystem("SpreadsheetConverterSpec", ConfigFactory.parseString(
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
      |  max-timeout: 1
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

  private def convert(testFileName: String): List[Sheet] = {
    val testFile = getClass.getResource(testFileName)
    val path = Paths.get(testFile.toURI)

    new Document(path).convertTo("tsv").get
  }

  "A Document with a low max-timeout" should "timeout while parsing an XSLX document" in {
    val thrown = intercept[Exception] {
      convert("/test.xlsx")
    }
    assert(thrown.getMessage === "Circuit Breaker Timed out.")
  }

  it should "timeout while parsing an XLS document" in {
    val thrown = intercept[Exception] {
      convert("/test.xls")
    }
    assert(thrown.getMessage === "Circuit Breaker Timed out.")
  }

}
