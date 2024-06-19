package io.mdcatapult.doclib.tabular.parser

import org.apache.pekko.actor.ActorSystem
import com.typesafe.config.Config

import java.io.File
import java.nio.charset.Charset
import io.mdcatapult.doclib.tabular.{Sheet => TabSheet}
import org.apache.commons.csv.{CSVFormat, CSVParser}

import scala.jdk.CollectionConverters._
import scala.util.Try

/**
  * Parser for loading CSV file and reformatting with set delimiters
  * @param file File
  */
class CSV(file: File) extends Parser {

  def parse(fieldDelimiter: String, stringDelimiter: String, lineDelimiter: Option[String] = Some("\n"))(implicit system: ActorSystem, config: Config): Try[List[TabSheet]] = {
    Try {
      val p = CSVParser.parse(file, Charset.defaultCharset(), CSVFormat.DEFAULT)
      val result = p.iterator.asScala.map(row => {
        row.iterator().asScala.mkString(fieldDelimiter)
      }).mkString(lineDelimiter.get)
      p.close()
      List(TabSheet(0, "sheet", result))
    }
  }
}
