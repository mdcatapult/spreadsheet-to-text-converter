package io.mdcatapult.doclib.tabular

import java.io.File
import java.nio.file.Path
import io.mdcatapult.doclib.tabular.{Sheet ⇒ TabSheet}
import io.mdcatapult.doclib.tabular.parser._

/**
  * Simple control class to act as an interface between the application and parsers
  * @param path Path
  */
class Document(path: Path) {
  val file = new File(path.toUri)
  val parser: Parser = if (path.endsWith(".csv")) new CSV(file) else new Stream(file)

  def to(format: String): List[TabSheet] = format match {
    case "tsv" ⇒ parser.parse("\t", "\"")
    case "csv" ⇒ parser.parse(",", "\"")
    case _ ⇒ throw new Exception(f"Format $format not currently supported")
  }
}
