package io.mdcatapult.doclib.tabular.parser

import java.io.File
import java.nio.charset.Charset
import io.mdcatapult.doclib.tabular.{Sheet ⇒ TabSheet}
import org.apache.commons.csv.{CSVFormat, CSVParser}

import scala.collection.JavaConverters._

/**
  * Parser for loading CSV file and reformatting with set delimiters
  * @param file
  */
class CSV(file: File) extends Parser {

  def parse(fieldDelimiter: String, stringDelimiter: String, lineDelimiter: Option[String] = Some("\n")): List[TabSheet] = {
    val p = CSVParser.parse(file, Charset.defaultCharset(), CSVFormat.DEFAULT)
    val result = p.iterator.asScala.map(row ⇒ {
      row.iterator().asScala.mkString(fieldDelimiter)
    }).mkString(lineDelimiter.get)
    p.close()
    List(TabSheet(1, "1", result))
  }
}
