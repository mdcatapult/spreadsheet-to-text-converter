/*
 * Copyright 2024 Medicines Discovery Catapult
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mdcatapult.doclib.tabular.parser

import org.apache.pekko.actor.ActorSystem
import com.typesafe.config.Config
import io.mdcatapult.doclib.tabular.Sheet
import org.apache.poi.hssf.eventusermodel._
import org.apache.poi.hssf.eventusermodel.dummyrecord.{LastCellOfRowDummyRecord, MissingCellDummyRecord, MissingRowDummyRecord}
import org.apache.poi.hssf.record._
import org.apache.poi.poifs.filesystem.POIFSFileSystem

import java.io.{File, FileInputStream}
import scala.collection.mutable
import scala.util.Try

class XLS(file: File) extends Parser with HSSFListener {

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


  def parse(fieldDel: String, stringDel: String, lineDel:Option[String] = Some("\n"))(implicit system: ActorSystem, config: Config): Try[List[Sheet]] = {
    Try {
      val poifs: POIFSFileSystem = new POIFSFileSystem(new FileInputStream(file))
      val breaker = createCircuitBreaker()
          .onOpen({
            // The conversion has timed out so close it. The circuit breaker will throw an exception
            // It should be a CircuitBreaker with "Circuit Breaker Timed out" message
            poifs.close()
          })
      breaker.withSyncCircuitBreaker({
        fieldDelimiter = fieldDel
        stringDelimiter = stringDel
        lineDelimiter = lineDel
        val factory = new HSSFEventFactory
        val request = new HSSFRequest

        request.addListenerForAllRecords(formatListener)

        factory.processWorkbookEvents(request, poifs)
        poifs.close()
        output.toList
      })
    }
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
