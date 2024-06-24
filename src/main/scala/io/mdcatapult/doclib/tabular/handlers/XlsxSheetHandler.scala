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

package io.mdcatapult.doclib.tabular.handlers

import io.mdcatapult.doclib.tabular.parser.escape
import org.apache.poi.ss.util.{CellAddress, CellReference}
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler.SheetContentsHandler
import org.apache.poi.xssf.usermodel.XSSFComment

import scala.util.{Failure, Success, Try}

class XlsxSheetHandler(output: StringBuilder,
                       fieldDelimiter: String,
                       stringDelimiter: String,
                       minColumns: Option[Int] = Some(0),
                       lineDelimiter:Option[String] = Some("\n")
                  ) extends SheetContentsHandler {

  private var isFirst: Boolean = false
  private var currentRow: Int = 0
  private var currentCol: Int = 0


  def startRow(rowNum: Int): Unit = {
    outputMissingRows(rowNum - currentRow - 1)
    isFirst = true
    currentRow = rowNum
    currentCol = 0
  }

  private def outputMissingRows(number: Int): Unit =
    for (_ <- 0 until number) {
      for (_ <- 0 until minColumns.get) {
        output.append(fieldDelimiter)
      }
      output.append(lineDelimiter.get)
    }

  def endRow(rowNum: Int): Unit = {
    for (_ <- currentCol until minColumns.get) {
      output.append(fieldDelimiter)
    }
    output.append(lineDelimiter.get)
  }


  def cell(cellReference: String, formattedValue: String, comment: XSSFComment): Unit = {
    if (isFirst) isFirst = false
    else output.append(fieldDelimiter)
    val thisCol = new CellReference(
      if (cellReference == null)
        new CellAddress(currentRow, currentCol).formatAsString
      else
        cellReference
    ).getCol
    val missedCols = thisCol - currentCol - 1
    for (_ <- 0 until missedCols) {
      output.append(fieldDelimiter)
    }
    currentCol = thisCol
    val isInteger = """([0-9]+)""".r
    val isDouble = """([0-9]+.[0-9]*)""".r

    Try(formattedValue match {
      case isInteger(_) =>  output.append(formattedValue.toInt)
      case isDouble(_) => output.append(formattedValue.toDouble)
      case _ =>
        appendText(formattedValue)
    }) match {
      case Success(_) => // do nothing
      case Failure(ex) => ex match {
        case _: NumberFormatException =>
          appendText(formattedValue)
        case e => throw e
      }
    }
  }

  private def appendText(formattedValue: String): Unit = {
    output.append(stringDelimiter)
    output.append(escape(formattedValue))
    output.append(stringDelimiter)
  }

}
