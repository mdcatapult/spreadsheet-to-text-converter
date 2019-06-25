package io.mdcatapult.doclib.tabular.parser

import java.io.File

import io.mdcatapult.doclib.tabular.Sheet
import io.mdcatapult.doclib.tabular.handlers.XlsxSheetHandler
import org.apache.poi.ooxml.util.SAXHelper
import org.apache.poi.openxml4j.opc._
import org.apache.poi.ss.usermodel.DataFormatter
import org.apache.poi.xssf.eventusermodel.XSSFReader.SheetIterator
import org.apache.poi.xssf.eventusermodel._
import org.apache.poi.xssf.model.StylesTable
import org.xml.sax.InputSource

import scala.collection.JavaConverters._

class XLSX(file: File) extends Parser {

  protected val pkg: OPCPackage = OPCPackage.open(file, PackageAccess.READ)
  protected val reader: XSSFReader = new XSSFReader(pkg)
  protected val sharedStrings = new ReadOnlySharedStringsTable(pkg)
  protected val styles: StylesTable = reader.getStylesTable

  def parse(fieldDelimiter: String, stringDelimiter: String, lineDelimiter:Option[String] = Some("\n")): List[Sheet] = {
    val it: SheetIterator = reader.getSheetsData.asInstanceOf[SheetIterator]
    it.asScala.zipWithIndex.map(sh ⇒ {

      val contents = new StringBuilder()
      val p = SAXHelper.newXMLReader()
      p.setContentHandler(new XSSFSheetXMLHandler(
        styles,
        null,
        sharedStrings,
        new XlsxSheetHandler(contents, fieldDelimiter, stringDelimiter),
        new DataFormatter(),
        false))
      val sheetSource: InputSource = new InputSource(sh._1)
      p.parse(sheetSource)
      pkg.close()

      Sheet(
        sh._2,
        it.getSheetName,
        contents.toString()
      )
    }).toList
  }
}
