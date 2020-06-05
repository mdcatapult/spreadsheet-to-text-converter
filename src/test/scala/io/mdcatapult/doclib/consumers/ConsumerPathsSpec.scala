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

  private val localTempDir = config.getString("doclib.local.temp-dir")

  "A ConsumerPaths targetPath" should "put source under ingress/derivatives with nesting when taking from local" in {
    val source = "local/resources/test.csv"
    val result = paths.getTargetPath(source, Some("spreadsheet_conv"))

    result should be ("ingress/derivatives/resources/spreadsheet_conv-test.csv")
  }

  it should "put target directly under ingress/derivatives when taking from local with no nesting" in {
    val source = "local/test.csv"
    val target = paths.getTargetPath(source, Some("spreadsheet_conv"))

    target should be ("ingress/derivatives/spreadsheet_conv-test.csv")
  }

  it should "put target directly under {temp-dir}/derivatives when taking from remote with no nesting" in {
    val source = "remote/test.csv"
    val target = paths.getTargetPath(source, Some("spreadsheet_conv"))

    target should be (s"$localTempDir/derivatives/remote/spreadsheet_conv-test.csv")
  }

  it should "de-duplicate derivatives sub-path when derivatives multiply nested" in {
    val source = "local/derivatives/derivatives/derivatives/test.csv"
    val target = paths.getTargetPath(source, Some("spreadsheet_conv"))

    target should be ("ingress/derivatives/spreadsheet_conv-test.csv")
  }

  it should "de-duplicate remote derivatives sub-path when derivatives multiply nested" in {
    val source = "local/derivatives/derivatives/derivatives/remote/test.csv"
    val target = paths.getTargetPath(source, Some("spreadsheet_conv"))

    target should be ("ingress/derivatives/remote/spreadsheet_conv-test.csv")
  }

  it should "have a target path within doclib.temp-dir when taking an existing source" in {
    val source = "local/derivatives/remote/test.csv"
    val target = paths.getTargetPath(source, Some("spreadsheet_conv"))

    target should be (s"$localTempDir/derivatives/remote/spreadsheet_conv-test.csv")
  }
}
