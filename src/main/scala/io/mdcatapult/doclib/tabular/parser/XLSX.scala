package io.mdcatapult.doclib.tabular.parser

import akka.actor.ActorSystem
import akka.pattern.CircuitBreaker
import com.typesafe.config.Config

import java.io.File
import io.mdcatapult.doclib.tabular.Sheet
import io.mdcatapult.doclib.tabular.handlers.XlsxSheetHandler
import org.apache.poi.openxml4j.opc._
import org.apache.poi.ss.usermodel.DataFormatter
import org.apache.poi.util.XMLHelper
import org.apache.poi.xssf.eventusermodel.XSSFReader.SheetIterator
import org.apache.poi.xssf.eventusermodel._
import org.apache.poi.xssf.model.StylesTable
import org.xml.sax.InputSource

import scala.jdk.CollectionConverters._

class XLSX(file: File) extends Parser {

  def parse(fieldDelimiter: String, stringDelimiter: String, lineDelimiter:Option[String] = Some("\n"))(implicit system: ActorSystem, config: Config): Option[List[Sheet]] = {

    val pkg: OPCPackage = OPCPackage.open(file, PackageAccess.READ)
    val breaker = createCircuitBreaker()
        .onOpen({
          pkg.close()
        })
    try {
      breaker.withSyncCircuitBreaker({
        val reader: XSSFReader = new XSSFReader(pkg)
        val sharedStrings = new ReadOnlySharedStringsTable(pkg)
        val styles: StylesTable = reader.getStylesTable

        val it: SheetIterator = reader.getSheetsData.asInstanceOf[SheetIterator]

        val sheets = it.asScala.zipWithIndex.map(sh => {

          val contents = new StringBuilder()
          val p = XMLHelper.newXMLReader()
          p.setContentHandler(new XSSFSheetXMLHandler(
            styles,
            null,
            sharedStrings,
            new XlsxSheetHandler(contents, fieldDelimiter, stringDelimiter),
            new DataFormatter(),
            false))
          val sheetSource: InputSource = new InputSource(sh._1)

          p.parse(sheetSource)

          Sheet(
            sh._2,
            it.getSheetName,
            contents.toString()
          )
        }).toList

        Some(sheets)
      })
    } catch {
      case e: Throwable => {
        throw e
      }

    } finally {
      pkg.close()
    }
  }
}
