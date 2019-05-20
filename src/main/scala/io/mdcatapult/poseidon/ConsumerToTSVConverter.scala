package io.mdcatapult.poseidon

import java.io.{File, _}
import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file._
import java.time.LocalDateTime

import akka.actor.ActorSystem
import cats.data._
import cats.instances.future._
import com.mongodb.client.model.Projections
import com.spingo.op_rabbit.SubscriptionRef
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import io.mdcatapult.doclib.messages.DoclibMsg
import io.mdcatapult.doclib.models.PrefetchOrigin
import io.mdcatapult.doclib.util.{LemonLabsAbsoluteUrlCodec, LemonLabsRelativeUrlCodec, LemonLabsUrlCodec, LocalDateTimeCodec}
import io.mdcatapult.klein.leadmine.LeadMiner
import io.mdcatapult.klein.mongo.Mongo
import io.mdcatapult.klein.queue.{Error, Exchange, Queue}
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.poi.ss.usermodel._
import org.apache.tika.Tika
import org.apache.tika.metadata.Metadata
import org.apache.tika.parser.ParseContext
import org.apache.tika.sax.BodyContentHandler
import org.bson.BsonValue
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.{CodecRegistries, CodecRegistry}
import org.bson.types.ObjectId
import org.mongodb.scala._
import org.mongodb.scala.bson.{BsonDateTime, BsonElement, BsonString}
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Updates._

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContextExecutor, Future, _}
import scala.util.control.Breaks._
import scala.util.{Failure, Success}
import scala.xml.InputSource
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY

object ConsumerToTSVConverter extends App with LazyLogging {

  implicit val system: ActorSystem = ActorSystem("consumer-totsvconverter")
  implicit val executor: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  implicit val config: Config = ConfigFactory.load()
  /** Initialise Mongo **/
  implicit val mongoCodecs: CodecRegistry = fromRegistries(
//    fromProviders(classOf[PrefetchOrigin]),
    CodecRegistries.fromCodecs(
      new LocalDateTimeCodec,
      new LemonLabsAbsoluteUrlCodec,
      new LemonLabsRelativeUrlCodec,
      new LemonLabsUrlCodec),
    DEFAULT_CODEC_REGISTRY)


  val queue: Queue[PoseidonMsg] = new Queue[PoseidonMsg](config.getString("consumer.queue"))
  val subscription: SubscriptionRef = queue.subscribe(handle, config.getInt("consumer.concurrent"))
  val errors: Queue[Error] = new Queue[Error](config.getString("error.queue"))
  val collection: MongoCollection[Document] = new Mongo().getCollection()
  val outputBaseDirectory = config.getString("output.baseDirectory")
  // TODO setup downstream queue - queue for prefetch
  val downstream: Exchange[DoclibMsg] = new Exchange[DoclibMsg](config.getString("downstream.queue"))



  var lmf: Future[LeadMiner] = Future(if (config.hasPath("leadmine.config"))
    LeadMiner(config.getString("leadmine.config"))
  else LeadMiner())

  def handle(msg: PoseidonMsg, key: String): Future[Option[org.mongodb.scala.Document]] =

    try {

      msg.id match {
        case id: String ⇒
          println(f"STARTING: $id")
          val query = equal("_id", new ObjectId(id))

          (for {
            doc ← OptionT({
              val d = Await.ready(collection.find(query)
                .projection(Projections.fields(Projections.include("source", "tags", "origin", "id")))
                .first().toFutureOption(), Duration.Inf)
              feedParser(d)
              d
            })
          } yield doc).value

        case _ ⇒
          logger.error("No id supplied")
          Future.failed(new Exception("No ID Supplied"))
      }
    }
    catch {
      case ex: Throwable ⇒ logger.error(ex.toString)
        Future.failed(ex)
    }

  def feedParser(d: Future[Option[Document]]): Unit = {

    val source = d.map(r ⇒ r.get("source"))

    d.onComplete({
      case Success(value) ⇒
        val document = value.getOrElse(throw new Exception("failed to get document"))

        val source = document("source")

        val inputFilepath = document("source").asString.getValue

        println(inputFilepath)

        val sheetMap = parseDocument(inputFilepath)

        for (sheetItem ← sheetMap) {
          breakable {

            val pmcNumber: String = getPMCNumber(inputFilepath)
            val (outputFilenamePart1: String, outputFilenamePart2: String) = getOutputFilepathParts(inputFilepath, sheetItem._1)
            val outputDirectory = s"$outputBaseDirectory/$pmcNumber"

            createOutputDirectory(outputDirectory)

            if (sheetItem._2 != "") {
              writeTSV(sheetItem._2, outputFilenamePart1, outputFilenamePart2, outputDirectory)

              val tags = document("tags")
              val origin = document("origin")
              val id = document("_id")

              val update = combine(
                addToSet("tags", tags), //.getOrElse(List[String]()).distinct),
                set("origin", origin), //.getOrElse(Map[String, Any]())),
                set("source", inputFilepath),
                set("updated", LocalDateTime.now())
              )

              val kleinUpdate = combine(addToSet("klein", "totsv"))

              // update the klein value on the source document
              collection.updateOne(equal("_id", id), kleinUpdate).toFutureOption().andThen({

                case Success(_) ⇒ {

                  // create the doclib entry for the new tsv document
                  val doc: Document = Document(
                    "tags" -> tags, //.getOrElse(List[String]()).distinct,
                    "origin" -> origin,
                  "source" -> inputFilepath,
//                  "updated" -> LocalDateTime.now()
                  )

                  // push onto queue
                  // push document into mongo
                  collection.insertOne(doc).toFutureOption().andThen({
                    case Success(_) ⇒ downstream.send(DoclibMsg(id = id.toString()))
                    case Failure(e) => throw e
                  })
                }
                case Failure(e) => throw e
              })
            }
          }
        }


      case Failure(e) ⇒ throw e
    })

    //    source.onComplete({
    //      case Success(value) ⇒

    //        val inputFilepath = "/efs/dev/source/PMC999/Scenario1.xlsx"
    //        val inputFilepath = "/efs/ebi/supplementary_data/PMC3446998/pone.0044872.s008.xls"


    //      case Failure(e) ⇒ println(e)
    //    })
  }

  def writeTSV(content: String, outputFilenamePart1: String, outputFilenamePart2: String, outputDirectory: String) = {

    require(content != "")
    require(outputFilenamePart1 != "")
    require(outputFilenamePart2 != "")
    require(outputDirectory != "")

    val filename = Paths.get(outputDirectory, outputFilenamePart1 + "_" + outputFilenamePart2 + ".tsv")
    val outputFile = new File(filename.toString)
    val bw = new BufferedWriter(new FileWriter(outputFile))
    bw.write(content)
    bw.close()
  }

  def createOutputDirectory(outputDirectory: String) = {

    require(outputDirectory != "")

    val outputDirectoryFile = new File(outputDirectory)
    outputDirectoryFile.mkdirs()
  }

  def getOutputFilepathParts(inputFilepath: String, sheetName: String) = {

    require(inputFilepath != "")
    require(sheetName != "")

    val inputFilename = new File(inputFilepath).getName

    val outputFilenamePart1 = inputFilename.trim().replace(" ", "_")
    val outputFilenamePart2 = sheetName.trim().replace(" ", "_")
    (outputFilenamePart1, outputFilenamePart2)
  }

  def getPMCNumber(inputFilepath: String) = {

    require(inputFilepath != "")

    val parts = inputFilepath.split("/")
    val pmcNumber = parts.filter(_.startsWith("PMC")).head
    pmcNumber
  }

  def getFileContent(tika: Tika, input: BufferedInputStream) = {

    val handler = new BodyContentHandler()
    tika.getParser.parse(
      input,
      handler,
      new Metadata,
      new ParseContext)
    handler.toString
  }

  def parseCSV(filepath: String): scala.collection.mutable.Map[String, String] = {

    val file = new File(filepath)
    val reader = Files.newBufferedReader(Paths.get(filepath))
    val csvParser = CSVParser.parse(file, Charset.defaultCharset(), CSVFormat.DEFAULT)
    val contentBuilder = new StringBuilder()
    var result: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map.empty[String, String]

    val rowIterator = csvParser.iterator

    while (rowIterator.hasNext) {

      val row = rowIterator.next()

      var fieldIterator = row.iterator

      while (fieldIterator.hasNext) {
        val fieldValue = fieldIterator.next()

        val outputFieldValue = fieldValue + "\t"
        contentBuilder.append(outputFieldValue)
      }
      contentBuilder.append("\n")
    }
    result(file.getName) = contentBuilder.toString
    result
  }

  def parseDocument(filepath: String): scala.collection.mutable.Map[String, String]  = {

    var result: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map.empty[String, String]

    val file = new File(filepath)

    if (file.getName.contains(".csv")) {
      parseCSV(filepath)
    }
    else {

      // Creating a Workbook from an Excel file (.xls or .xlsx)
      val workbook = WorkbookFactory.create(file);

      // Retrieving the number of sheets in the Workbook
      System.out.println("Workbook has " + workbook.getNumberOfSheets() + " Sheets : ");

      val sheetIterator = workbook.sheetIterator
      System.out.println("Retrieving Sheets using Iterator")
      while ( {
        sheetIterator.hasNext
      }) {
        val sheet = sheetIterator.next
        val sheetName = sheet.getSheetName
        System.out.println("=> " + sheetName)

        val dataFormatter = new DataFormatter();

        System.out.println("\n\nIterating over Rows and Columns using Iterator\n");
        val rowIterator = sheet.rowIterator();

        var lastRow = 0;

        val contentBuilder = new StringBuilder()

        while (rowIterator.hasNext()) {

          val row = rowIterator.next();

          val cellIterator = row.cellIterator();

          val rowNumber = row.getRowNum //getRowNumber(row, "application/vnd.ms-excel")

          if (rowNumber > lastRow + 1) {
            System.out.println()

            for (i ← 1 to rowNumber - (lastRow + 1)) {
              contentBuilder.append("\n")
            }
          }

          var sourceCellColumn = 0

          while (cellIterator.hasNext()) {

            val cell = cellIterator.next();

            // validates cell value is put in the correct column else add tab until it matches
            // map cell value to reference on first line
            val targetCellColumn = cell.getColumnIndex
            while (targetCellColumn > sourceCellColumn) {
              contentBuilder.append("\t")
              sourceCellColumn += 1
            }

            val cellValue = dataFormatter.formatCellValue(cell);

            if (cellValue.contains("\n") || cellValue.contains("\t")) {
              val quotedCellValue = "\"" + cellValue + "\""
              System.out.print(quotedCellValue + "\t");
              contentBuilder.append(quotedCellValue + "\t")
            } else {
              System.out.print(cellValue + "\t");
              contentBuilder.append(cellValue + "\t")
            }
            sourceCellColumn += 1
          }

          System.out.println();
          contentBuilder.append("\n")
          lastRow = rowNumber
        }
        result(sheetName) = contentBuilder.toString
      }
      result
    }
  }

  def getNextSheet(tika: Tika, input: BufferedInputStream) = {

    val handler = getHTMLForFile(tika, input)

    val htmlToProcess = new InputSource(new StringReader(handler.toString()))

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

  def getHTMLForFile(tika: Tika, input: BufferedInputStream) = {

//    val handler = new PreserveCoordinatesContentHandler(new BodyContentHandler, new Metadata)
    val handler = new BodyContentHandler()
    tika.getParser.parse(
      input,
      handler,
      new Metadata,
      new ParseContext)
    handler
  }

  def printDebugStatements(input: _root_.java.io.BufferedInputStream, content: _root_.java.lang.String) = {

    println(f"RAW Content length = ${scala.io.Source.fromInputStream(input).getLines().mkString("").length} characters")
    println()
    println("Content Loaded as string with Apache Tika")
    println(content)
  }
}