package com.fjeandel.challenge.results

import java.io.{BufferedWriter, File, FileWriter}
import java.time.LocalDate
import java.util.UUID
import com.fjeandel.challenge.files.{CA, CAFile, Sale, SaleFile}
import com.fjeandel.challenge.files.FileUtils._
import com.fjeandel.challenge.services.LoadFile
import com.typesafe.scalalogging.StrictLogging


// Methods for Writing CA & Sale Result files or Calculate CA or Sale Result


case class Result(txId: Int, date: LocalDate, storeId: UUID, artId: Int, qty: Int, price: Double, ca: Double)
object Result extends StrictLogging {

  def writeCAByDayAndStore(data: Stream[CA], resultPath: String, dateArg: LocalDate): Unit = {

    var nbCAFile: Int = 0
    data
      .filter(caResult => caResult.date.equals(dateArg))
      .groupBy(caResult => (caResult.date, caResult.storeId))
      .foreach { case ((date, storeId), caResults) =>

        val caFileName = "top_100_ca_" + storeId + "_" + transformDate(date)
        nbCAFile += 1

        val caFile = new File(resultPath, caFileName)
        val caBw = new BufferedWriter(new FileWriter(caFile))
        val firstLine: Unit = caBw.write("product|ca" + "\n")
        caResults
          .groupBy(_.artId).mapValues(x => roundDouble(x.map(_.ca).sum)).take(100).toSeq.sortWith(_._2 > _._2).toStream
          .foreach{result =>
            val line =  parseTupleToLine(result)
            caBw.write(line)
          }
          caBw.close()
      }
      logger.info(s"Nb Distinct Stores: $nbCAFile")
      logger.info(s"Total files exported for both KPI & Period: ${nbCAFile*4+4}")
  }

  def writeSaleByDayAndStore(data: Stream[Sale], resultPath: String, dateArg: LocalDate): Unit = {

    data
      .filter(saleLine => saleLine.date.equals(dateArg))
      .groupBy(saleLine => (saleLine.date, saleLine.storeId))
      .foreach { case ((date, storeId), saleResult) =>

        val saleFileName = "top_100_ventes_" + storeId + "_" + transformDate(date)

        val saleFile = new File(resultPath, saleFileName)
        val saleBw = new BufferedWriter(new FileWriter(saleFile))
        val firstLine: Unit = saleBw.write("product|qty" + "\n")
        saleResult
          .groupBy(_.artId).mapValues(_.map(_.qty).sum).take(100).toSeq.sortWith(_._2 > _._2).toStream
          .foreach{result =>
            val line =  parseTupleToLine(result)
            saleBw.write(line)
          }
        saleBw.close()
      }
  }

  def writeCAByStoreOverPeriod(caFiles: Stream[CAFile], resourcesIntermediatePath: String, resultPath: String, dateArg: LocalDate): Unit = {

    val beginDate = LocalDate.of(dateArg.getYear,dateArg.getMonth,dateArg.getDayOfMonth-7)
    caFiles
      .filter(caFile => caFile.date.isAfter(beginDate) || caFile.date.equals(dateArg))
      .flatMap(caFile => LoadFile.fileToCAResult(resourcesIntermediatePath, caFile))
      .groupBy(_.storeId)
      .mapValues(caStream => caStream.map(caLine => CA(caLine.txId, caLine.date, caLine.storeId, caLine.artId, caLine.ca)))
      .mapValues(_.groupBy(_.artId).mapValues(x => roundDouble(x.map(_.ca).sum)).take(100).toSeq.sortWith(_._2 > _._2).toStream)
      .foreach{ case (store, stream) =>
        val caFileName = "top_100_ca_" + store + "_" + transformDate(dateArg) + "-J7"

        val caFile = new File(resultPath, caFileName)
        val caBw = new BufferedWriter(new FileWriter(caFile))
        val firstLine: Unit = caBw.write("product|ca" + "\n")
        stream
          .foreach { result =>
            val line = parseTupleToLine(result)
            caBw.write(line)
          }
        caBw.close()
      }
  }

  def writeSaleByStoreOverPeriod(saleFiles: Stream[SaleFile], resourcesIntermediatePath: String, resultPath: String, dateArg: LocalDate): Unit = {

    val beginDate = LocalDate.of(dateArg.getYear,dateArg.getMonth,dateArg.getDayOfMonth-7)
    saleFiles
      .filter(saleFile => saleFile.date.isAfter(beginDate) || saleFile.date.equals(dateArg))
      .flatMap(saleFile => LoadFile.fileToSaleResult(resourcesIntermediatePath, saleFile))
      .groupBy(_.storeId)
      .mapValues(saleStream => saleStream.map(saleLine => Sale(saleLine.txId, saleLine.date, saleLine.storeId, saleLine.artId, saleLine.qty)))
      .mapValues(_.groupBy(_.artId).mapValues(_.map(_.qty).sum).take(100).toSeq.sortWith(_._2 > _._2).toStream)
      .foreach{ case (store, stream) =>
        val saleFileName = "top_100_ventes_" + store + "_" + transformDate(dateArg) + "-J7"

        val saleFile = new File(resultPath, saleFileName)
        val saleBw = new BufferedWriter(new FileWriter(saleFile))
        val firstLine: Unit = saleBw.write("product|qty" + "\n")
        stream
          .foreach { result =>
            val line = parseTupleToLine(result)
            saleBw.write(line)
          }
        saleBw.close()
      }
  }

  def getGlobalCAByDay(data: Stream[CA]): Stream[(Int, Double)] = {
    data.groupBy(_.artId).mapValues(x => roundDouble(x.map(_.ca).sum)).take(100).toSeq.sortWith(_._2 > _._2).toStream
  }

  def getGlobalSaleByDay(data: Stream[Sale]): Stream[(Int, Int)] = {
    data.groupBy(_.artId).mapValues(_.map(_.qty).sum).take(100).toSeq.sortWith(_._2 > _._2).toStream
  }
}





