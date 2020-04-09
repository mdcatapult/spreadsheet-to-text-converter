package io.mdcatapult.doclib.tabular

import java.io.File
import java.nio.file.Path

import better.files.{File => ScalaFile}
import io.mdcatapult.doclib.tabular.parser._
import io.mdcatapult.doclib.tabular.{Sheet => TabSheet}

/**
  * Simple control class to act as an interface between the application and parsers
  * @param path Path
  */
class Document(path: Path) {
  private val file: File = new File(path.toUri)

  private val extension = ScalaFile(path.toString).extension

  lazy val parser: Parser =
    extension match {
      case Some(".xls") =>
        try {
          new XLS(file)
        } catch {
          case _: Exception =>
            new XLSX(file)
        }
      case Some(".xlsx") => new XLSX(file)
      case Some(".csv") => new CSV(file)
      case Some(".ods") => new ODF(file)
      case _ => new Default(file)
    }

  def convertTo(format: String): List[TabSheet] = format match {
    case "tsv" => parser.parse("\t", "\"")
    case "csv" => parser.parse(",", "\"")
    case _ => throw new Exception(f"Format $format not currently supported")
  }
}
