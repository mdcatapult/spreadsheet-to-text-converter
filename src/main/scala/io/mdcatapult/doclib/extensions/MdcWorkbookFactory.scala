package io.mdcatapult.doclib.extensions

import java.io.{File, InputStream}

import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.ss.usermodel.{Workbook, WorkbookFactory}
import org.apache.poi.xssf.streaming.SXSSFWorkbook
import org.apache.poi.xssf.usermodel.XSSFWorkbook

class MdcWorkbookFactory extends WorkbookFactory {
}

object MdcWorkbookFactory {

  def create(file: File, windowSize: Int): Workbook = {
    val wb = WorkbookFactory.create(file)

    try {
      wb match {
        case x: XSSFWorkbook => return new SXSSFWorkbook(x, windowSize)
        case h: HSSFWorkbook => return h
      }
    } catch {
      case e: Exception => return wb
    }
  }

  def create(input: InputStream, windowSize: Int): Workbook = {
    val wb = WorkbookFactory.create(input)

    try {

      wb match {
        case x: XSSFWorkbook => return new SXSSFWorkbook(x, windowSize)
        case h: HSSFWorkbook => return h
      }
    } catch {
      case e: Exception => return wb
    }
  }
}
