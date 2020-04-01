package io.mdcatapult.doclib.tabular.parser

import java.io.{File, FileInputStream}

import io.mdcatapult.doclib.tabular.Sheet
import org.apache.poi.hssf.eventusermodel._
import org.apache.poi.hssf.eventusermodel.dummyrecord.{LastCellOfRowDummyRecord, MissingCellDummyRecord, MissingRowDummyRecord}
import org.apache.poi.hssf.record.{SSTRecord, _}
import org.apache.poi.poifs.filesystem.{DocumentInputStream, POIFSFileSystem}

import scala.collection.mutable

class XLS(file: File) extends Parser with HSSFListener {

  val poifs = new POIFSFileSystem(new FileInputStream(file))
  val din: DocumentInputStream = poifs.createDocumentInputStream("Workbook")
  var sheetIndex: Int = -1
  var columnIndex: Int = 0
  var rowIndex: Int = 0
  var sheetContents: mutable.StringBuilder = new StringBuilder
  var boundRecords: mutable.ListBuffer[BoundSheetRecord] = mutable.ListBuffer[BoundSheetRecord]()
  var sstRecord: Option[SSTRecord] = None
  var fieldDelimiter: String = ""
  var stringDelimiter: String = ""
  var lineDelimiter:Option[String] = Some("\n")
  val minColumns: Option[Int] = Some(0)
  val output: mutable.ListBuffer[Sheet] = mutable.ListBuffer[Sheet]()
  val formatListener = new FormatTrackingHSSFListener(
    new MissingRecordAwareHSSFListener(this)
  )
  val workbookBuildingListener = new EventWorkbookBuilder.SheetRecordCollectingListener(formatListener)


  def parse(fieldDel: String, stringDel: String, lineDel:Option[String] = Some("\n")): List[Sheet] = {
    fieldDelimiter = fieldDel
    stringDelimiter = stringDel
    lineDelimiter = lineDel
    val factory = new HSSFEventFactory
    val request = new HSSFRequest
    request.addListenerForAllRecords(formatListener)
//    request.addListenerForAllRecords(workbookBuildingListener)
    factory.processWorkbookEvents(request, poifs)
    poifs.close()
    output.toList
  }

  /**
    * Function used as callback for handling record definitions in workbook
    * @param record XLS Record
    */
  def processRecord(record: Record): Unit = {
    val (rowNum, colNum): (Int, Int) = record match {
      case _: LastCellOfRowDummyRecord =>
        if (minColumns.getOrElse(0) > 0) {
          for (_ <- columnIndex until minColumns.get) {
            sheetContents.append(fieldDelimiter)
          }
        } else {
          val r = sheetContents.toString().replaceAll(s"$fieldDelimiter+$$", "")
          sheetContents = new StringBuilder().append(r)
        }


        if (lineDelimiter.isDefined) sheetContents.append(lineDelimiter.get)
        (rowIndex+1,0)

      case _: MissingCellDummyRecord =>
        sheetContents.append(fieldDelimiter)
        (rowIndex,columnIndex+1)

      case _: MissingRowDummyRecord =>
        for (_ <- 0 until minColumns.get) {
          sheetContents.append(fieldDelimiter)
        }
        if (lineDelimiter.isDefined) sheetContents.append(lineDelimiter.get)
        (rowIndex+1,0)
      case r: BoundSheetRecord =>
        boundRecords += r
        (rowIndex,columnIndex)

      case r: SSTRecord =>
        sstRecord = Some(r)
        (rowIndex, columnIndex)

      case r: BOFRecord =>
        if (r.getType == BOFRecord.TYPE_WORKSHEET) {
          sheetIndex = sheetIndex + 1
          sheetContents = new StringBuilder()
          (0,0)
        } else {
          (rowIndex, columnIndex)
        }

      case _: EOFRecord =>
        if (sheetIndex >= 0)
          output += Sheet(sheetIndex, boundRecords(sheetIndex).getSheetname, sheetContents.toString())
        (0,0)

      case r: BlankRecord =>
        if (r.getColumn > 0) sheetContents.append(fieldDelimiter)
        (r.getRow, r.getColumn)

      case r: BoolErrRecord =>
        (r.getRow, r.getColumn)
      case r: FormulaRecord =>
        if (r.getColumn > 0) sheetContents.append(fieldDelimiter)
        if (!r.hasCachedResultString) {
          sheetContents.append(formatListener.formatNumberDateCell(r))
        }
        (r.getRow, r.getColumn)
      case r: StringRecord =>
        // follow on from FormulaRecord
        sheetContents.append(stringDelimiter)
        sheetContents.append(escape(r.getString))
        sheetContents.append(stringDelimiter)
        (rowIndex, columnIndex)

      case r: LabelRecord =>
        if (r.getColumn > 0 ) sheetContents.append(fieldDelimiter)
        sheetContents.append(stringDelimiter)
        sheetContents.append(escape(r.getValue))
        sheetContents.append(stringDelimiter)
        (r.getRow, r.getColumn)

      case r: LabelSSTRecord =>
        if (r.getColumn > 0 ) sheetContents.append(fieldDelimiter)
        if (sstRecord.isDefined) {
          sheetContents.append(stringDelimiter)
          sheetContents.append(escape(sstRecord.get.getString(r.getSSTIndex).toString))
          sheetContents.append(stringDelimiter)
        }
        (r.getRow, r.getColumn)

      case r: NoteRecord =>
        sheetContents.append(fieldDelimiter)
        (r.getRow, r.getColumn)

      case r: NumberRecord =>
        if (r.getColumn > 0 ) sheetContents.append(fieldDelimiter)
        sheetContents.append(formatListener.formatNumberDateCell(r))
        (r.getRow, r.getColumn)

      case r: RKRecord =>
        sheetContents.append(fieldDelimiter)
        (r.getRow, r.getColumn)
      //          case RowRecord.sid =>
      case _ => (rowIndex, columnIndex)

    }
    columnIndex = colNum
    rowIndex = rowNum


  }

}
