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
  val file: File = new File(path.toUri)
  lazy val parser: Parser = getParser

  def getParser: Parser = if (path.toString.toLowerCase.endsWith(".csv")) new CSV(file)
  else if (path.toString.toLowerCase.endsWith(".xls")) new XLS(file)
  else if (path.toString.toLowerCase.endsWith(".xlsx")) new XLSX(file)
  else new Default(file)

  def convertTo(format: String): List[TabSheet] = format match {
    case "tsv" ⇒ parser.parse("\t", "\"")
    case "csv" ⇒ parser.parse(",", "\"")
    case _ ⇒ throw new Exception(f"Format $format not currently supported")
  }
}
