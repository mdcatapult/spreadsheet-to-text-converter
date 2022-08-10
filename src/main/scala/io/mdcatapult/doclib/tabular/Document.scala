package io.mdcatapult.doclib.tabular

import akka.actor.ActorSystem
import com.typesafe.config.Config
//import akka.pattern.CircuitBreaker

import java.io.File
import java.nio.file.Path
import better.files.{File => ScalaFile}
import io.mdcatapult.doclib.tabular.parser._
import io.mdcatapult.doclib.tabular.{Sheet => TabSheet}

//import scala.concurrent.duration.DurationInt

/**
  * Simple control class to act as an interface between the application and parsers
  * @param path Path
  */
class Document(path: Path)(implicit system: ActorSystem, config: Config) {
  private val file: File = new File(path.toUri)

  private val extension = ScalaFile(path.toString).extension

  private val expectedParser =
    extension match {
      case Some(".xls") =>
        try {
          new XLS(file)
        } catch {
          case _: Exception => new XLSX(file)
        }
      case Some(".xlsx") => new XLSX(file)
      case Some(".csv") => new CSV(file)
      case Some(".ods") => new ODF(file)
      case _ => new Default(file)
    }

  private def misnamedParser =
    extension match {
      case Some(".xls") => new XLSX(file)
      case Some(".xlsx") => new XLS(file)
      case _ => new CSV(file)
    }

  private val nestedParser =
    new Parser {
      override def parse(
                          fieldDelimiter: String,
                          stringDelimiter: String,
                          lineDelimiter: Option[String])(implicit system: ActorSystem, config: Config): Option[List[TabSheet]] =
        try {
          expectedParser.parse(fieldDelimiter, stringDelimiter, lineDelimiter)
        } catch {
          case x: Exception =>
            try {
              misnamedParser.parse(fieldDelimiter, stringDelimiter, lineDelimiter)
            } catch {
              case _: Exception => throw x
            }
        }
    }

  def convertTo(format: String): Option[List[TabSheet]] = format match {
      case "tsv" => nestedParser.parse(fieldDelimiter = "\t", stringDelimiter = "\"")
      case "csv" => nestedParser.parse(fieldDelimiter = ",", stringDelimiter = "\"")
      case _ => throw new Exception(f"Format $format not currently supported")
    }
}
