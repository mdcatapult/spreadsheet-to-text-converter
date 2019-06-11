package io.mdcatapult.doclib.tabular.parser

import java.io.File

import io.mdcatapult.doclib.tabular.{Sheet ⇒ TabSheet}
import org.apache.poi.ss.usermodel.{Cell, CellType, Workbook, WorkbookFactory}
import org.apache.poi.xssf.streaming.SXSSFWorkbook
import org.apache.poi.xssf.usermodel.XSSFWorkbook

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * Stream Parser for loading POI workbooks and processing as a streamed interface
  * @param file File
  * @param windowSize Option[Int] default window to use with streamed SXSSFWorkbook interface
  */
class Stream(file: File, windowSize: Option[Int] = Some(100)) extends Parser {

  def getWorkbook: Workbook = {
    Try(WorkbookFactory.create(file)) match {
      case Success(value) ⇒ value match {
        case x: XSSFWorkbook ⇒ new SXSSFWorkbook(x, windowSize.get)
        case other ⇒ other
      }
      case Failure(e) ⇒ throw e
    }
  }

  def parse(fieldDelimiter: String, stringDelimiter: String, lineDelimiter: Option[String] = Some("\n")): List[TabSheet] = {
    val wb = getWorkbook
    val result: List[TabSheet] = wb.sheetIterator().asScala.zipWithIndex.map(sheet ⇒ {
      TabSheet(
        sheet._2,
        sheet._1.getSheetName,
        wb.getSheet(sheet._1.getSheetName).rowIterator().asScala.map(row ⇒ {
          row.cellIterator().asScala.map((cell: Cell) ⇒ cell.getCellType match {
            case CellType.NUMERIC ⇒ cell.getNumericCellValue
            case CellType.BOOLEAN ⇒ cell.getBooleanCellValue
            case CellType.FORMULA ⇒ cell.getCellFormula
            case CellType.STRING ⇒ cell.getStringCellValue
            case CellType.ERROR ⇒ cell.getErrorCellValue
            case _ ⇒ cell.getStringCellValue
          }).mkString(fieldDelimiter)
        }).mkString(lineDelimiter.get)
      )
    }).toList
    wb.close()
    result
  }

}
