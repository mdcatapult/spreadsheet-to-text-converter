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

package io.mdcatapult.doclib.consumers

import com.typesafe.config.{Config, ConfigFactory}
import io.mdcatapult.doclib.handlers.ConsumerPaths
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Try

class ConsumerPathsSpec extends AnyFlatSpec with Matchers {

  // Note: we are going to overwrite this in a later test so var not val.
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
      |mongo {
      |  doclib-database: "prefetch-test"
      |  documents-collection: "documents"
      |  connection {
      |    username: "doclib"
      |    password: "doclib"
      |    database: "admin"
      |    hosts: ["localhost"]
      |  }
      |}
      |consumer {
      |  name = spreadsheet-converter
      |}
    """.stripMargin)

  private val paths = new ConsumerPaths()

  private val localTempDir = config.getString("doclib.local.temp-dir")

  "A ConsumerPaths targetPath" should "put source under ingress/derivatives with nesting when taking from local" in {
    val source = "local/resources/test.csv"
    val result = paths.getTargetPath(source, Try(config.getString("consumer.name")).toOption)

    result should be ("ingress/derivatives/resources/spreadsheet-converter-test.csv")
  }

  it should "put target directly under ingress/derivatives when taking from local with no nesting" in {
    val source = "local/test.csv"
    val target = paths.getTargetPath(source, Try(config.getString("consumer.name")).toOption)

    target should be ("ingress/derivatives/spreadsheet-converter-test.csv")
  }

  it should "put target directly under {temp-dir}/derivatives when taking from remote with no nesting" in {
    val source = "remote/test.csv"
    val target = paths.getTargetPath(source, Try(config.getString("consumer.name")).toOption)

    target should be (s"$localTempDir/derivatives/remote/spreadsheet-converter-test.csv")
  }

  it should "de-duplicate derivatives sub-path when derivatives multiply nested" in {
    val source = "local/derivatives/derivatives/derivatives/test.csv"
    val target = paths.getTargetPath(source, Try(config.getString("consumer.name")).toOption)

    target should be ("ingress/derivatives/spreadsheet-converter-test.csv")
  }

  it should "have a target path within doclib.temp-dir when taking an existing source" in {
    val source = "local/derivatives/remote/test.csv"
    val target = paths.getTargetPath(source, Try(config.getString("consumer.name")).toOption)

    target should be (s"$localTempDir/derivatives/remote/spreadsheet-converter-test.csv")
  }
}
