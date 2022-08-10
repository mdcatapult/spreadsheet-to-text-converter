package io.mdcatapult.doclib.tabular.parser

import akka.actor.ActorSystem
import akka.pattern.CircuitBreaker

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

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

class XLSX(file: File) extends Parser {

  def parse(fieldDelimiter: String, stringDelimiter: String, lineDelimiter:Option[String] = Some("\n"))(implicit system: ActorSystem): Option[List[Sheet]] = {

    println("Parsing xlsx")
    val pkg: OPCPackage = OPCPackage.open(file, PackageAccess.READ)

    try {
      val reader: XSSFReader = new XSSFReader(pkg)
      val sharedStrings = new ReadOnlySharedStringsTable(pkg)
      val styles: StylesTable = reader.getStylesTable
      println("Parsing xlsx 1")

      val it: SheetIterator = reader.getSheetsData.asInstanceOf[SheetIterator]
      println("Parsing xlsx 2")

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
        println("Parsing xlsx 3")

        val breaker =
          CircuitBreaker(system.scheduler, maxFailures = 1, callTimeout = 10.seconds, resetTimeout = 1.minute)
            .onOpen(throw new Exception("Taking totally way too long"))
        println("Parsing xlsx 4")

        breaker.withSyncCircuitBreaker(p.parse(sheetSource))

        Sheet(
          sh._2,
          it.getSheetName,
          contents.toString()
        )
      }).toList
      println("Parsing xlsx 5")

      Some(sheets)
    } catch {
      case _: Throwable => {
        println("It's an exception")
        None
      }

    } finally {
      pkg.close()
    }
  }
}
