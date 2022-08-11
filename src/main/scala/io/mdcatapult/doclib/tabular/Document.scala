package io.mdcatapult.doclib.tabular

import akka.actor.ActorSystem
import com.typesafe.config.Config

import scala.util.{Failure, Success, Try}
import java.io.File
import java.nio.file.Path
import better.files.{File => ScalaFile}
import io.mdcatapult.doclib.tabular.parser._
import io.mdcatapult.doclib.tabular.{Sheet => TabSheet}
import org.apache.poi.openxml4j.exceptions.OLE2NotOfficeXmlFileException
import org.apache.poi.poifs.filesystem.OfficeXmlFileException

/**
  * Simple control class to act as an interface between the application and parsers
  * @param path Path
  */
class Document(path: Path)(implicit system: ActorSystem, config: Config) {
  private val file: File = new File(path.toUri)

  private val extension = ScalaFile(path.toString).extension

  private val expectedParser =
    extension match {
      case Some(".xls") => new XLS(file)
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

  def convertTo(format: String): Try[List[TabSheet]] = {
    val stringDelimiter = "\""
    val fieldDelimiter = format match {
      case "tsv" => "\t"
      case "csv" => ","
      case _ => throw new Exception(f"Format $format not currently supported")
    }
    expectedParser.parse(fieldDelimiter, stringDelimiter) match {
      // The circuit breaker throws an anonymous exception with a message
      case Failure(e: Exception) if (e.getMessage == "Circuit Breaker Timed out.") => Failure(e)
      case Failure(_: OLE2NotOfficeXmlFileException) => misnamedParser.parse(fieldDelimiter, stringDelimiter)
      case Failure(_: OfficeXmlFileException) => misnamedParser.parse(fieldDelimiter, stringDelimiter)
      // Every other Exception
      case Failure(e: Exception) if (e.getMessage != "Circuit Breaker Timed out.") => Failure(e)
      case Success(value) => Try(value)
    }
  }
}
