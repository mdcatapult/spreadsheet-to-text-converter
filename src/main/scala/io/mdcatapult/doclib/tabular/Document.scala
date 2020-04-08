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


  lazy val parser: Parser = {
    val extension = ScalaFile(path.toString).extension

    try {
      extension match {
        case Some(".csv") => new CSV(file)
        case Some(".xls") => new XLS(file)
        case Some(".xlsx") => new XLSX(file)
        case Some(".ods") => new ODF(file)
        case _ => new Default(file)
      }
    } catch {
      // A catch in case it's an Office 2007+ XML with ".xls" extension ie use XSSF.
      // TODO something better
      case x: Exception =>
        extension match {
          case Some(".xls") => new XLSX(file)
          case Some(".xlsx") => new XLS(file)
          case _ => throw x
        }
    }
  }

  def convertTo(format: String): List[TabSheet] = format match {
    case "tsv" => parser.parse("\t", "\"")
    case "csv" => parser.parse(",", "\"")
    case _ => throw new Exception(f"Format $format not currently supported")
  }
}
