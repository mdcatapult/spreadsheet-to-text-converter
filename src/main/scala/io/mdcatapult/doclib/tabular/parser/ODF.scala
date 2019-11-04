package io.mdcatapult.doclib.tabular.parser

import java.io.File

import io.mdcatapult.doclib.tabular.Sheet
import org.jopendocument.dom.spreadsheet.SpreadSheet
import org.jopendocument.dom.spreadsheet.Range
import org.jopendocument.dom.spreadsheet.{Sheet ⇒ JDocSheet}

class ODF(file: File) extends Parser {
  /**
   * Abstract definition to take appropriate delimiters and convert the supplied document into a list of "sheets"
   *
   * @param fieldDelimiter  String
   * @param stringDelimiter String
   * @param lineDelimiter   Option[String]
   * @return List[Sheet]
   */
  override def parse(fieldDelimiter: String, stringDelimiter: String, lineDelimiter: Option[String]): List[Sheet] = {
    val spreadsheet = SpreadSheet.createFromFile(file)
    for {
      sheetCount <-( 0 until spreadsheet.getSheetCount).toList
    } yield createSheet(sheetCount, spreadsheet, fieldDelimiter, lineDelimiter.getOrElse("\n"))
  }

  def createSheet(sheetCount: Int, spreadsheet: SpreadSheet, fieldDelimiter: String, lineDelimiter: String): Sheet = {
    val sheet = spreadsheet.getSheet(sheetCount)
    val contents = new StringBuilder()
    // This range count is not fast but it's all we've got.
    val range = sheet.getUsedRange
    for {
      i <- 0 to range.getEndPoint.x
      j ← 0 to range.getEndPoint.y
    } {
      contents.append(sheet.getCellAt(j, i).getValue)
      if (j != range.getEndPoint.y) contents.append(fieldDelimiter)
      if(j == range.getEndPoint.y) contents.append(lineDelimiter)
    }
    Sheet(
      index = sheetCount,
      name = sheet.getName,
      content = contents.toString()
    )
  }

}
