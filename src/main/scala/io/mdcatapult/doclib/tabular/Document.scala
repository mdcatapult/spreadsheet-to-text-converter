package io.mdcatapult.doclib.tabular

import java.io.File
import java.nio.file.Path
import better.files.{File => ScalaFile, _}

import io.mdcatapult.doclib.tabular.{Sheet ⇒ TabSheet}
import io.mdcatapult.doclib.tabular.parser._

/**
  * Simple control class to act as an interface between the application and parsers
  * @param path Path
  */
class Document(path: Path) {
  val file: File = new File(path.toUri)
  lazy val parser: Parser = getParser

  def getParser: Parser = ScalaFile(path.toString).extension match {
    case Some(".csv") ⇒ new CSV(file)
    case Some(".xls") ⇒ new XLS(file)
    case Some(".xlsx") ⇒ new XLSX(file)
    case Some(".ods") ⇒ new ODF(file)
    case _ ⇒ new Default(file)
  }

  def convertTo(format: String): List[TabSheet] = format match {
    case "tsv" ⇒ parser.parse("\t", "\"")
    case "csv" ⇒ parser.parse(",", "\"")
    case _ ⇒ throw new Exception(f"Format $format not currently supported")
  }
}
