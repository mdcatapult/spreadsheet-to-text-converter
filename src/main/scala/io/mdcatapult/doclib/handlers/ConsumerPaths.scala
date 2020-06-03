package io.mdcatapult.doclib.handlers

import java.nio.file.{Path, Paths}

import better.files.{File => ScalaFile, _}
import com.typesafe.config.Config
import io.mdcatapult.doclib.util.TargetPath

/**
  * Defines the paths that are used by the spreadsheet consumer.
  * @param doclibConfig path configuration
  */
class ConsumerPaths()(implicit val doclibConfig: Config) extends TargetPath {

  private val doclibRoot = doclibConfig.getString("doclib.root")
  private val tempDir = doclibConfig.getString("doclib.local.temp-dir")
  //private val localTargetDir = doclibConfig.getString("doclib.local.target-dir")
  private val convertToPath = doclibConfig.getString("convert.to.path")

  /**
    * The absolute path from file system root through doclib root to the actual file
    * @param path path to resolve
    * @return
    */
  def absolutePath(path: String): Path =
    Paths.get(doclibRoot, path).toAbsolutePath

  val absoluteRootPath: ScalaFile = doclibRoot/""

  /**
    * Generate new file path maintaining file path from origin but allowing for intersection of common root paths.
    * The target path used is that configured by the property convert.to.path.
    * @param source String
    * @return String full path to new target
    */
  def getTargetPath(source: String, prefix: Option[String]): String =
    tempDir + "/" + getTargetPath(source, convertToPath, prefix.map(_ + "-"))

}
