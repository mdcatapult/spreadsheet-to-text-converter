package io.mdcatapult.doclib.tabular.parser

import java.io.File

import io.mdcatapult.doclib.tabular.Sheet
import org.jopendocument.dom.spreadsheet.SpreadSheet

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
    for (sheetCount <- 0 until spreadsheet.getSheetCount) {
      val sheet = spreadsheet.getSheet(sheetCount)
      val contents = new StringBuilder()
      val range = sheet.getUsedRange
      for (i <- 0 to range.getEndPoint.x) {
        for (j ← 0 to range.getEndPoint.y) {
          contents.append(sheet.getCellAt(j, i).getTextValue)
          if (j != range.getEndPoint.x) contents.append(fieldDelimiter)
        }
        contents.append(lineDelimiter)
      }
    }
    return List[Sheet]()
  }

}
