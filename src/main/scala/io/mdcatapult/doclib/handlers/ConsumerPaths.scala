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

package io.mdcatapult.doclib.handlers

import java.nio.file.{Path, Paths}

import com.typesafe.config.Config
import better.files.{File => ScalaFile, _}

import scala.annotation.tailrec

/**
  * Defines the paths that are used by the spreadsheet consumer.
  * @param config path configuration
  */
class ConsumerPaths()(implicit config: Config) {

  private val doclibRoot = config.getString("doclib.root")
  private val tempDir = config.getString("doclib.local.temp-dir")
  private val localTargetDir = config.getString("doclib.local.target-dir")
  private val convertToPath = config.getString("doclib.derivative.target-dir")

  /**
    * The absolute path from file system root through doclib root to the actual file
    * @param path path to resolve
    * @return
    */
  def absolutePath(path: String): Path =
    Paths.get(doclibRoot, path).toAbsolutePath

  val absoluteRootPath: ScalaFile = doclibRoot/""

  /**
    * generate new file path maintaining file path from origin but allowing for intersection of common root paths
    * @param source String
    * @return String full path to new target
    */
  def getTargetPath(source: String, prefix: Option[String] = None): String = {
    val targetRoot = convertToPath.replaceAll("/+$", "")
    val regex = """(.*)/(.*)$""".r

    source match {
      case regex(path, file) =>
        val c = commonPath(List(targetRoot, path))
        val targetPath =
          scrub(
            path
              .replaceAll(s"^$c", "")
              .replaceAll("^/+|/+$", "")
          )

        Paths.get(tempDir, targetRoot, targetPath, s"${prefix.getOrElse("")}-$file").toString
      case _ => source
    }
  }

  @tailrec
  private def scrub(path: String): String =
    path match {
      case path if path.startsWith(localTargetDir) =>
        scrub(
          removePathRoot(path, localTargetDir)
        )
      case path if path.startsWith(convertToPath)  =>
        scrub(
          removePathRoot(path, convertToPath)
        )
      case _ => path
    }

  private def removePathRoot(path: String, root: String): String =
    path.replaceFirst(s"^$root/*", "")

  /**
    * Determines common root paths for two path string
    * @param paths List[String]
    * @return String common path component
    */
  private def commonPath(paths: List[String]): String = {
    val SEP = "/"
    val BOUNDARY_REGEX = s"(?=[$SEP])(?<=[^$SEP])|(?=[^$SEP])(?<=[$SEP])"

    def common(a: List[String], b: List[String]): List[String] =
      (a, b) match {
        case (aa :: as, bb :: bs) if aa equals bb => aa :: common(as, bs)
        case _ => Nil
      }

    paths match {
      case List() =>
        ""
      case List(x) =>
        x
      case _ =>
        paths.map(_.split(BOUNDARY_REGEX).toList).reduceLeft(common).mkString
    }
  }

}
