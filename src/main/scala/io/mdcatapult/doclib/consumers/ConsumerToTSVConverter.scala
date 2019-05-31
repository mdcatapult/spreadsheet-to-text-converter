package io.mdcatapult.doclib.consumers

import java.io._
import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.{Path, Paths}
import java.util.stream.Collectors

import scala.collection.JavaConverters._
import akka.actor.ActorSystem
import cats.data.OptionT
import com.spingo.op_rabbit.SubscriptionRef
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import io.mdcatapult.doclib.messages.{DoclibMsg, PrefetchMsg}
import io.mdcatapult.klein.mongo.Mongo
import io.mdcatapult.klein.queue.{Exchange, Queue}
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.poi.ss.usermodel.{CellType, DataFormatter, Workbook, WorkbookFactory}
import org.apache.tika.Tika
import org.apache.tika.io.TikaInputStream
import org.apache.tika.metadata.Metadata
import org.apache.tika.parser.ParseContext
import org.apache.tika.sax.BodyContentHandler
import org.bson.codecs.configuration.CodecRegistries.fromRegistries
import org.bson.codecs.configuration.{CodecRegistries, CodecRegistry}
import org.bson.codecs.jsr310.LocalDateTimeCodec
import org.bson.types.ObjectId
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.model.Filters.equal
import org.mongodb.scala.model.Updates.{addEachToSet, addToSet, combine, set}
import org.mongodb.scala.result.UpdateResult
import org.mongodb.scala.{Document, MongoCollection, SingleObservable, result}
import cats.implicits._
import cats.data._
import org.mongodb.scala.bson.{BsonArray, BsonDocument, BsonString, BsonValue}
import org.apache.commons.io.FilenameUtils
import org.apache.poi.EncryptedDocumentException
import org.bson.conversions.Bson
import org.mongodb.scala.bson.conversions.Bson

import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.xml.InputSource
import java.nio.file._
import java.nio.file.attribute._
import java.util.Set

import org.apache.poi.xssf.streaming
import org.apache.poi.xssf.streaming.SXSSFWorkbook
import org.apache.poi.ss.usermodel

object ConsumerToTSVConverter extends App with LazyLogging {

  implicit val system: ActorSystem = ActorSystem("consumer-totsvconverter")
  implicit val executor: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  implicit val config: Config = ConfigFactory.load()


  /** Initialise Mongo **/
  implicit val mongoCodecs: CodecRegistry = fromRegistries(
    CodecRegistries.fromCodecs(
      new LocalDateTimeCodec),
    DEFAULT_CODEC_REGISTRY)


  val queue: Queue[DoclibMsg] = new Queue[DoclibMsg](
    config.getString("upstream.queue"),
    Some(config.getString("upstream.topics")))
  val subscription: SubscriptionRef = queue.subscribe(handle, config.getInt("upstream.concurrent"))
  val collection: MongoCollection[Document] = new Mongo().getCollection()
  val outputBaseDirectory = config.getString("output.baseDirectory")
  val downstream: Queue[PrefetchMsg] = new Queue[PrefetchMsg](config.getString("downstream.queue"))
  var updateResult: Future[Option[UpdateResult]] = _


  def handle(msg: DoclibMsg, key: String): Future[Option[org.mongodb.scala.Document]] =
    try {

      msg.id match {
        case id: String ⇒
          logger.info(f"STARTING: $id")
          val query = equal("_id", new ObjectId(id))
          (for {
            doc ← OptionT(collection.find(query).first.toFutureOption())
            _ ← OptionT(feedParser(id, doc))
          } yield doc).value

        case _ ⇒
          logger.error("No id supplied")
          Future.failed(new Exception("No ID Supplied"))
      }
    }
    catch {
      case ex: Throwable ⇒
        logger.error(ex.toString)
        // set klein.totsv = false providing not a mongo error
        // add exclusion for monho exception
        collection.updateOne(equal("_id", msg.id), combine(
          set("klein.totsv", false)
        )).toFutureOption()
        Future.failed(ex)
    }

  def enqueue(prefetchMsg: PrefetchMsg): Unit = {
    downstream.send(prefetchMsg)
  }

  def feedParser(id: String, document: Document): Future[Option[UpdateResult]] = {
    val inputFilepath = document("source").asString.getValue
    logger.debug(inputFilepath)

    try {
      val sheetMap = parseDocument(inputFilepath)
      val newFiles = mutable.ListBuffer[String]()
      var count = 0
      for (sheetItem ← sheetMap) {
        val pmcNumber: String = getPMCNumber(inputFilepath)
        val (outputDirectoryPart2: String, outputFilename: String) = getOutputFilepathParts(inputFilepath, sheetItem._1)
        val outputDirectory = s"$outputBaseDirectory/$pmcNumber"

        val fullOutputDirectory = outputDirectory + "/" + outputDirectoryPart2
        if (createOutputDirectory(fullOutputDirectory)) {
          if (sheetItem._2 != "") {
            count += 1
            val newFile = writeTSV(sheetItem._2, fullOutputDirectory, count.toString, outputFilename).toString
            newFiles += newFile

            enqueue(new PrefetchMsg(newFile,
              id,
              document("tags").asArray().getValues.asScala.map(tag => tag.asString().getValue).toList
            ))
          }
        }
      }

      // refactor - code works but needs refactoring into better code - not batching calls
      val length = newFiles.length - 1

      if (length < 0) {
        val update = addToSet("derivatives", "")
        updateResult = collection.updateOne(equal("_id", document("_id")), update).toFutureOption()
        return updateResult
      } else {
        for (i <- 0 to length) {
          val update = addToSet("derivatives", newFiles(i))
          updateResult = collection.updateOne(equal("_id", document("_id")), update).toFutureOption()
        }
        updateResult
      }

    } catch {
      case e: Exception => println("Exception: feedParser() id:" + id + " document:" + document)
        throw e
      // nack here
    }
  }

  def writeTSV(content: String, outputDirectory: String, filenamePrefix: String, outputFilename: String): Path = {

    require(content != "")
    require(outputDirectory != "")
    require(filenamePrefix != "")
    require(outputFilename != "")
    require(outputDirectory != "")

    val filename = Paths.get(outputDirectory, filenamePrefix + "_" + outputFilename + ".tsv")
    val outputFile = new File(filename.toString)


    val bw = new BufferedWriter(new FileWriter(outputFile))
    bw.write(content)
    bw.close()

    filename
  }


  //  def writeTSV(content: String, outputDirectory: String, filenamePrefix: String, outputFilename: String): Path = {
  //    require(content != "")
  //    require(outputFilename != "")
  //    require(outputDirectory != "")
  //
  //    var fw = None : Option[FileWriter]
  //    var bw = None : Option[BufferedWriter]
  //    var filename = None : Option[Path]
  //
  //    try {
  //
  //      filename = Some(Paths.get(outputDirectory, filenamePrefix + "_" + outputFilename + ".tsv"))
  //      val outputFile = (new File(filename.get.toString))
  //
  //      fw = Some(new FileWriter(outputFile))
  //      bw = Some(new BufferedWriter(fw.get))
  //
  //      bw.get.write(content)
  //
  //    } catch {
  //      case e : Exception => {
  //        println("Exception: writeTSV() content.length: " + content.length + " outputDirectory: " + outputDirectory + " filenamePrefix: " + filenamePrefix + " outputFilename: " + outputFilename + " " + e.toString)
  //        throw e
  //      }
  //    } finally {
  //      if (fw.isDefined) fw.get.close
  //      if (bw.isDefined) bw.get.close
  //    }
  //
  //    filename.get
  //  }

  def createOutputDirectory(outputDirectory: String): Boolean = {
    require(outputDirectory != "")

    val outputDirectoryFile = new File(outputDirectory)

    val outputDirectoryPath = Paths.get(outputDirectory)
    val permissions = PosixFilePermissions.fromString("rwxrwxrwx");
    val fileAttributes = PosixFilePermissions.asFileAttribute(permissions);
    Files.createDirectories(outputDirectoryPath, fileAttributes);

    Files.exists(outputDirectoryPath)
  }

  def getOutputFilepathParts(inputFilepath: String, sheetName: String): (String, String) = {
    require(inputFilepath != "")
    require(sheetName != "")

    val inputFilename = new File(inputFilepath).getName
    val outputDirectory = FilenameUtils.removeExtension(inputFilename.trim().replace(" ", "_"))
    val outputFilename = sheetName.trim().replace(" ", "_")
    (outputDirectory, outputFilename)
  }

  def getPMCNumber(inputFilepath: String): String = {
    require(inputFilepath != "")

    val parts = inputFilepath.split("/")
    val pmcNumber = parts.filter(_.startsWith("PMC")).head
    pmcNumber
  }

  def parseCSV(filepath: String): (String, String) = {

    var csvParser = None: Option[CSVParser]
    var file = None: Option[File]
    var contentBuilder = None: Option[StringBuilder]

    try {
      file = Some(new File(filepath))
      csvParser = Some(CSVParser.parse(file.get, Charset.defaultCharset(), CSVFormat.DEFAULT))
      contentBuilder = Some(new StringBuilder())

      val rowIterator = csvParser.get.iterator

      while (rowIterator.hasNext) {
        val row = rowIterator.next()
        val fieldIterator = row.iterator

        while (fieldIterator.hasNext) {
          val fieldValue = fieldIterator.next()

          val outputFieldValue = fieldValue + "\t"
          contentBuilder.get.append(outputFieldValue)
        }
        contentBuilder.get.append("\n")
      }
      csvParser.get.close()
    } catch {
      case e: Exception => {
        println("Exception: parseCSV() filepath: " + filepath + " " + e.toString)
        throw e
      }
    } finally {
      if (csvParser.isDefined) csvParser.get.close()
    }
    (file.get.getName, contentBuilder.toString)
  }

  def parseByStreaming(filepath: String): Map[String, String] = {
    val xlsxFile = new File(filepath);
    val result: mutable.Map[String, String] = mutable.Map.empty[String, String]
    val contentBuilder = new StringBuilder

      val inp = new FileInputStream(filepath);
      val wb = WorkbookFactory.create(inp);
      val sheetCount = wb.getNumberOfSheets
      val sheetIterator = wb.sheetIterator()

      System.out.println("Retrieving Sheets using Iterator")
      while ({sheetIterator.hasNext}) {
        val sheetName = sheetIterator.next().getSheetName
        val sheet = wb.getSheet(sheetName);
        if (sheet != null) {

          val rowIterator = sheet.rowIterator()

          while({rowIterator.hasNext}) {
            val row = rowIterator.next()

            val cellIterator = row.cellIterator

            while({cellIterator.hasNext}) {
              val cell = cellIterator.next()

              val cellType = cell.getCellType
              System.out.println(cellType)

              cellType match {
                case CellType.NUMERIC => {
                  val cellValue = cell.getNumericCellValue
//                  System.out.println(cellValue)
                  contentBuilder.append(cellValue + "\t")
                }
                case CellType.BLANK => {
                  contentBuilder.append("\t")
                }
                case CellType.BOOLEAN => {
                  val cellValue = cell.getBooleanCellValue
//                  System.out.println(cellValue)
                  contentBuilder.append(cellValue + "\t")
                }
                case CellType.FORMULA => {
                  val cellValue = cell.getCellFormula
//                  System.out.println(cellValue)
                  contentBuilder.append(cellValue + "\t")
                }
                case CellType.STRING => {
                  val cellValue = cell.getStringCellValue
//                  System.out.println(cellValue)
                  contentBuilder.append(cellValue + "\t")
                }
                case CellType.ERROR => {
                  val cellValue = cell.getErrorCellValue
//                  System.out.println(cellValue)
                  contentBuilder.append(cellValue + "\t")
                }
                case CellType._NONE => {
                  contentBuilder.append("\t")
                }
              }
            }
            contentBuilder.append("\n")
          }
        } else {
          // continue
        }

        val sheetContent = contentBuilder.toString()
//        System.out.println(sheetContent)
        result(sheetName) = sheetContent
      }
//      } else {
        // continue
//      }

    result.toMap
  }

  def parseDocument(filepath: String): Map[String, String]  = {
    val file = new File(filepath)
    if (file.getName.contains(".csv")) {
      List(parseCSV(filepath)).toMap
    } else {
      return parseByStreaming(filepath)
    }
  }

  def parseByLoadUpfront(file: File): Map[String, String] = {
    val result: mutable.Map[String, String] = mutable.Map.empty[String, String]

    // Creating a Workbook from an Excel file (.xls or .xlsx)
    var workbook = None: Option[org.apache.poi.ss.usermodel.Workbook]
    try {
      workbook = Some(WorkbookFactory.create(file))

      // Retrieving the number of sheets in the Workbook
      System.out.println("Workbook has " + workbook.get.getNumberOfSheets + " Sheets : ")

      val sheetIterator = workbook.get.sheetIterator
      System.out.println("Retrieving Sheets using Iterator")
      while ( {
        sheetIterator.hasNext
      }) {
        val sheet = sheetIterator.next
        val sheetName = sheet.getSheetName
        System.out.println("=> " + sheetName)

        val dataFormatter = new DataFormatter()

        System.out.println("\n\nIterating over Rows and Columns using Iterator\n")
        val rowIterator = sheet.rowIterator()

        var lastRow = 0

        val contentBuilder = new StringBuilder()

        while (rowIterator.hasNext) {

          val row = rowIterator.next()

          val cellIterator = row.cellIterator()
          val rowNumber = row.getRowNum //getRowNumber(row, "application/vnd.ms-excel")

          if (rowNumber > lastRow + 1) {
            // required to generate internal blank line
            //System.out.println()

            for (_ ← 1 to rowNumber - (lastRow + 1)) {
              contentBuilder.append("\n")
            }
          }

          var sourceCellColumn = 0

          while (cellIterator.hasNext) {

            val cell = cellIterator.next

            // validates cell value is put in the correct column else add tab until it matches
            // map cell value to reference on first line
            val targetCellColumn = cell.getColumnIndex
            while (targetCellColumn > sourceCellColumn) {
              contentBuilder.append("\t")
              sourceCellColumn += 1
            }

            val cellValue = dataFormatter.formatCellValue(cell)

            if (cellValue.contains("\n") || cellValue.contains("\t")) {
              val quotedCellValue = "\"" + cellValue + "\""
              // System.out.print(quotedCellValue + "\t")
              contentBuilder.append(quotedCellValue + "\t")
            } else {
              // System.out.print(cellValue + "\t")
              contentBuilder.append(cellValue + "\t")
            }
            sourceCellColumn += 1
          }

          //            System.out.println()
          contentBuilder.append("\n")
          lastRow = rowNumber
        }
        result(sheetName) = contentBuilder.toString
      }
      workbook.get.close

    } catch {
      case ioe: IOException => {
        println("IO Exception: parseDocument() filepath: " + file.getPath + " " + ioe)
        throw ioe
      }
      case ede: EncryptedDocumentException => {
        println("Encrypted Document Exception: parseDocument() filepath:" + file.getPath + " " + ede)
        throw ede
      }

      case e: Exception => {
        println("Exception: filepath: " + file.getPath + " " + e)
        throw e;
      }

    } finally {
      if (workbook.isDefined) workbook.get.close
    }

    result.toMap
  }

  def getNextSheet(tika: Tika, input: BufferedInputStream): immutable.Seq[BufferedInputStream] = {

    val handler = getHTMLForFile(tika, input)

    val htmlToProcess = new InputSource(new StringReader(handler.toString))

    val parserFactory = new org.ccil.cowan.tagsoup.jaxp.SAXFactoryImpl
    val parser = parserFactory.newSAXParser()

    val adapter = new scala.xml.parsing.NoBindingFactoryAdapter
    val node = adapter.loadXML(htmlToProcess, parser)

    println(node \\ "div")

    // each div is a sheet
    val nodes = node \\ "div"

    for (item ← nodes)
      yield new BufferedInputStream(new ByteArrayInputStream(item.toString().getBytes(StandardCharsets.UTF_8))) {
      println(s"item: $item")
    }
  }

  def getHTMLForFile(tika: Tika, input: BufferedInputStream): BodyContentHandler = {

    val handler = new BodyContentHandler()
    var stream = None: Option[TikaInputStream]

    try {
      val parser = tika.getParser
      stream = Some(TikaInputStream.get(input))

      parser.parse(
        TikaInputStream.get(input),
        handler,
        new Metadata,
        new ParseContext)

    } catch {
      case e: Exception => {
        println("Exception: getFileContent input: " + input)
        throw e
      }

    } finally {
      if (stream.isDefined) stream.get.close()
    }
    handler
  }

  def printDebugStatements(input: _root_.java.io.BufferedInputStream, content: _root_.java.lang.String): Unit = {

    println(f"RAW Content length = ${scala.io.Source.fromInputStream(input).getLines().mkString("").length} characters")
    println()
    println("Content Loaded as string with Apache Tika")
    println(content)
  }
}
