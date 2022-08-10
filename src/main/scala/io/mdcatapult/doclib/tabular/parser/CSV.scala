package io.mdcatapult.doclib.tabular.parser

import akka.actor.ActorSystem

import java.io.File
import java.nio.charset.Charset
import io.mdcatapult.doclib.tabular.{Sheet => TabSheet}
import org.apache.commons.csv.{CSVFormat, CSVParser}

import scala.jdk.CollectionConverters._

/**
  * Parser for loading CSV file and reformatting with set delimiters
  * @param file File
  */
class CSV(file: File) extends Parser {

  def parse(fieldDelimiter: String, stringDelimiter: String, lineDelimiter: Option[String] = Some("\n"))(implicit system: ActorSystem): Option[List[TabSheet]] = {
    val p = CSVParser.parse(file, Charset.defaultCharset(), CSVFormat.DEFAULT)
    println("Parser created")
    val result = p.iterator.asScala.map(row => {
      println("iterating")
      row.iterator().asScala.mkString(fieldDelimiter)
    }).mkString(lineDelimiter.get)
    // Can we just call p.close() during the process to stop it?
    p.close()
    Some(List(TabSheet(0, "sheet", result)))
  }
}
