package io.mdcatapult.doclib.consumers

import com.typesafe.config.{Config, ConfigFactory}
import io.mdcatapult.doclib.handlers.ConsumerPaths
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConsumerPathsSpec extends AnyFlatSpec with Matchers {

  // Note: we are going to overwrite this in a later test so var not val.
  implicit var config: Config = ConfigFactory.parseString(
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

  private val paths = new ConsumerPaths()

  "Spreadsheet handler" should "create a target path from a doclib doc source" in {
    val source = "local/resources/test.csv"
    val result = paths.getTargetPath(source, Some("spreadsheet_conv"))
    assert(result == "ingress/derivatives/resources/spreadsheet_conv-test.csv")
  }

  "A derivative" should "be ingested into doclib-root/temp-dir" in {
    val source = "local/test.csv"
    val target = paths.getTargetPath(source, Some("spreadsheet_conv"))
    assert(target == "ingress/derivatives/spreadsheet_conv-test.csv")
  }

  "A spreadsheet source" should "have a target path within doclib.temp-dir" in {
    val source = "remote/test.csv"
    val target = paths.getTargetPath(source, Some("spreadsheet_conv"))
    assert(target == s"${config.getString("doclib.local.temp-dir")}/derivatives/remote/spreadsheet_conv-test.csv")
  }

  "An existing derivative" should "only have derivative once in the path" in {
    val source = "local/derivatives/test.csv"
    val target = paths.getTargetPath(source, Some("spreadsheet_conv"))
    assert(target == "ingress/derivatives/spreadsheet_conv-test.csv")
  }

  "A path with multiple derivative segments" should "only have derivative once in the path" in {
    val source = "local/derivatives/derivatives/derivatives/test.csv"
    val target = paths.getTargetPath(source, Some("spreadsheet_conv"))
    assert(target == "ingress/derivatives/spreadsheet_conv-test.csv")
  }

  "An existing spreadsheet source" should "have a target path within doclib.temp-dir" in {
    val source = "local/derivatives/remote/test.csv"
    val target = paths.getTargetPath(source, Some("spreadsheet_conv"))
    assert(target == s"${config.getString("doclib.local.temp-dir")}/derivatives/remote/spreadsheet_conv-test.csv")
  }
}
