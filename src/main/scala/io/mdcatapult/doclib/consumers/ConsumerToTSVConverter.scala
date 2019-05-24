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
import org.apache.poi.ss.usermodel.{DataFormatter, WorkbookFactory}
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
import org.mongodb.scala.{Document, MongoCollection}
import cats.implicits._
import cats.data._
import org.mongodb.scala.bson.{BsonArray, BsonString, BsonValue}
import org.apache.commons.io.FilenameUtils;

import scala.collection.{immutable, mutable}
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.xml.InputSource

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


  def handle(msg: DoclibMsg, key: String): Future[Option[org.mongodb.scala.Document]] =
    try {

      msg.id match {
        case id: String ⇒
          logger.info(f"STARTING: $id")
          val query = equal("_id", new ObjectId(id))
          (for {
            doc ← OptionT(collection.find(query).first.toFutureOption())
            _ ← OptionT(feedParser(doc))
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

  def feedParser(document: Document): Future[Option[UpdateResult]] = {
    val inputFilepath = document("source").asString.getValue
    logger.debug(inputFilepath)
    val sheetMap = parseDocument(inputFilepath)


    val newFiles = mutable.ListBuffer[String]()
    var count = 1
    for (sheetItem ← sheetMap) {
      val pmcNumber: String = getPMCNumber(inputFilepath)
      val (outputDirectoryPart2: String, outputFilename: String) = getOutputFilepathParts(inputFilepath, sheetItem._1)
      val outputDirectory = s"$outputBaseDirectory/$pmcNumber"

      createOutputDirectory(outputDirectory + "/" + outputDirectoryPart2)
      if (sheetItem._2 != "") {
        val newFile = writeTSV(sheetItem._2, outputDirectory, count.toString, outputFilename).toString
        newFiles += newFile

        enqueue(new PrefetchMsg(newFile,
          document("origin").asString.getValue,
          document("tags").asArray().getValues.asScala.map(tag => tag.asString().getValue).toList
        ))
      }
    }

//    val allFiles: List[String] = (document.get("derivatives").getOrElse(BsonArray()).asArray().getValues.asScala.flatMap({
//      case d: BsonString ⇒ Some(d.getValue)
//      case _ ⇒ None
//    }).toList ::: newFiles.toList).distinct

    var derivatives = combine()
    for (f <- newFiles.toList) {
      derivatives = combine(derivatives, addToSet("derivatives", f))
    }

    val update =  combine(
      derivatives,
      set(config.getString("upstream.queue"), true)
    )

    collection.updateOne(equal("_id", document("_id")),update).toFutureOption()

  }

  def writeTSV(content: String, outputDirectory: String, filenamePrefix: String, outputFilename: String): Path = {
    require(content != "")
    require(outputFilename != "")
    require(outputDirectory != "")

    val filename: Path = Paths.get(outputDirectory, filenamePrefix + "_" + outputFilename + ".tsv")
    val outputFile = new File(filename.toString)
    val bw = new BufferedWriter(new FileWriter(outputFile))
    bw.write(content)
    bw.close()
    filename
  }

  def createOutputDirectory(outputDirectory: String): Boolean = {
    require(outputDirectory != "")

    val outputDirectoryFile = new File(outputDirectory)
    outputDirectoryFile.mkdirs()
  }

  def getOutputFilepathParts(inputFilepath: String, sheetName: String): (String, String) = {
    require(inputFilepath != "")
    require(sheetName != "")

    val inputFilename = new File(inputFilepath).getName
    val outputFilenamePart1 = FilenameUtils.removeExtension(inputFilename.trim().replace(" ", "_"))
    val outputFilenamePart2 = sheetName.trim().replace(" ", "_")
    (outputFilenamePart1, outputFilenamePart2)
  }

  def getPMCNumber(inputFilepath: String): String = {
    require(inputFilepath != "")

    val parts = inputFilepath.split("/")
    val pmcNumber = parts.filter(_.startsWith("PMC")).head
    pmcNumber
  }

  def getFileContent(tika: Tika, input: BufferedInputStream) = {
    val handler = new BodyContentHandler()
    tika.getParser.parse(
      TikaInputStream.get(input),
      handler,
      new Metadata,
      new ParseContext)
    handler.toString
  }

  def parseCSV(filepath: String): (String, String) = {

    val file = new File(filepath)
    val csvParser = CSVParser.parse(file, Charset.defaultCharset(), CSVFormat.DEFAULT)
    val contentBuilder = new StringBuilder()

    val rowIterator = csvParser.iterator

    while (rowIterator.hasNext) {
      val row = rowIterator.next()
      val fieldIterator = row.iterator

      while (fieldIterator.hasNext) {
        val fieldValue = fieldIterator.next()

        val outputFieldValue = fieldValue + "\t"
        contentBuilder.append(outputFieldValue)
      }
      contentBuilder.append("\n")
    }
    (file.getName, contentBuilder.toString)
  }

  def parseDocument(filepath: String): Map[String, String]  = {
    val file = new File(filepath)
    if (file.getName.contains(".csv")) {
      List(parseCSV(filepath)).toMap
    } else {

      val result: mutable.Map[String, String] = mutable.Map.empty[String, String]

      // Creating a Workbook from an Excel file (.xls or .xlsx)
      val workbook = WorkbookFactory.create(file)

      // Retrieving the number of sheets in the Workbook
      System.out.println("Workbook has " + workbook.getNumberOfSheets + " Sheets : ")

      val sheetIterator = workbook.sheetIterator
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
            System.out.println()

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
              System.out.print(quotedCellValue + "\t")
              contentBuilder.append(quotedCellValue + "\t")
            } else {
              System.out.print(cellValue + "\t")
              contentBuilder.append(cellValue + "\t")
            }
            sourceCellColumn += 1
          }

          System.out.println()
          contentBuilder.append("\n")
          lastRow = rowNumber
        }
        result(sheetName) = contentBuilder.toString
      }
      result.toMap
    }
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

//    val handler = new PreserveCoordinatesContentHandler(new BodyContentHandler, new Metadata)
    val handler = new BodyContentHandler()
    tika.getParser.parse(
      TikaInputStream.get(input),
      handler,
      new Metadata,
      new ParseContext)
    handler
  }

  def printDebugStatements(input: _root_.java.io.BufferedInputStream, content: _root_.java.lang.String): Unit = {

    println(f"RAW Content length = ${scala.io.Source.fromInputStream(input).getLines().mkString("").length} characters")
    println()
    println("Content Loaded as string with Apache Tika")
    println(content)
  }
}
