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

import java.io.File
import io.mdcatapult.doclib.tabular.{Sheet => TabSheet}
import org.apache.poi.ss.usermodel.{CellType, Workbook, WorkbookFactory}
import org.apache.poi.xssf.usermodel.XSSFWorkbook

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/**
  * Stream Parser for loading POI workbooks and processing as a streamed interface
  * @param file File
  * @param windowSize Option[Int] default window to use with streamed SXSSFWorkbook interface
  */
class Default(file: File, windowSize: Option[Int] = Some(100)) extends Parser {

  /**
    * create workbook.
    * @todo Review use of SXSSF Workbook and build compatible row/cell iteration around it
    * @return
    */
  def getWorkbook: Workbook = {
    Try(WorkbookFactory.create(file)) match {
      case Success(value) => value match {
        case x: XSSFWorkbook => x //new SXSSFWorkbook(x, windowSize.get)
        case other => other
      }
      case Failure(e) => throw e
    }
  }

  def parse(fieldDelimiter: String, stringDelimiter: String, lineDelimiter: Option[String] = Some("\n"))(implicit system: ActorSystem, config: Config): Try[List[TabSheet]] = {
    Try {
      val wb = getWorkbook
      val result: List[TabSheet] = wb.sheetIterator().asScala.zipWithIndex.map(sheet => {
        TabSheet(
          sheet._2,
          sheet._1.getSheetName,
          wb.getSheetAt(sheet._2).rowIterator().asScala.map(
            _.cellIterator().asScala.map(
              cell => cell.getCellType match {
                case CellType.NUMERIC => cell.getNumericCellValue
                case CellType.BOOLEAN => cell.getBooleanCellValue
                case CellType.FORMULA => cell.getCellFormula
                case CellType.STRING => cell.getStringCellValue
                case CellType.ERROR => cell.getErrorCellValue
                case _ => cell.getStringCellValue
              }
            ).mkString(fieldDelimiter)
          ).mkString(lineDelimiter.get)
        )
      }).toList
      wb.close()
      result
    }
  }
}
