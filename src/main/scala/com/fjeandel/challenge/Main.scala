package com.fjeandel.challenge

import java.time.LocalDate
import com.fjeandel.challenge.files.FileUtils._
import com.fjeandel.challenge.services._
import com.fjeandel.challenge.files._
import com.fjeandel.challenge.intermediateResult.IntermediateResult._
import com.fjeandel.challenge.results.{AggregateResult, Result}
import com.fjeandel.challenge.results.AggregateResult._
import com.fjeandel.challenge.results.Result._
import com.fjeandel.challenge.services.LoadFile.{fileToArticles, fileToCAResult, fileToSaleResult, fileToTransaction}
import com.typesafe.scalalogging.StrictLogging

class Main extends StrictLogging {

  def main(args: Array[String]): Unit = {

    logger.info("Hello Phenix Challenge !")

    if (args.isEmpty){
      logger.error("Program argument is empty!!")
    } else {
      args.foreach(arg => logger.debug(arg))
    }

    // Program arguments
    val dateArg: LocalDate = LocalDate.parse(args(0), yyyyMMdd)
    val resourcesPath: String = args(1)

    // Define path for data accessibility
    val transactionsPath: String = resourcesPath + "transactions/"
    val articlesPath: String = resourcesPath + "articles/"
    val resourcesIntermediatePath: String = resourcesPath + "intermediateResults/"
    val resultPath: String = resourcesPath + "results/"


    // Importing & Mapping all articles & transactions files
    val transactionFiles: Stream[TransactionFile] = getListOfFiles(transactionsPath).map(e => TransactionFile.apply(e))
    val articleFiles: Stream[ArticleFile] = getListOfFiles(articlesPath).map(e => ArticleFile.apply(e))
    logger.info(s"Nb imported transaction's files: ${transactionFiles.length} for this directory: "+transactionsPath)
    logger.info(s"Nb imported article's files: ${articleFiles.length} for this directory: "+articlesPath)


    // Transforming & Persisting all lines from articles & transactions files
    val transactions: Stream[Stream[Transaction]] = transactionFiles.map(transactionFile => fileToTransaction(transactionsPath, transactionFile))
    val articles: Stream[Stream[Article]] = articleFiles.map(articleFile => fileToArticles(articlesPath, articleFile))

    // Join between transactions files & articles files
    val intermediateResult: Stream[Result] = transactions.flatMap(transaction => Join.joinStream(articleFiles, transaction, articlesPath))
    logger.info(s"Total Result lines from join: ${intermediateResult.length}")

    // Writing intermediate Files
    writeIntermediateFiles(intermediateResult, resourcesIntermediatePath)

    // Importing & Mapping all intermediate CA & Sale files
    val caFiles: Stream[CAFile] = getListOfFiles(resourcesIntermediatePath, "ca_").map(e => CAFile(e))
    val saleFiles: Stream[SaleFile] = getListOfFiles(resourcesIntermediatePath, "ventes_").map(e => SaleFile(e))

    // Transforming & Persisting all lines from CA & Sale files
    val caLines: Stream[CA] = caFiles.flatMap(caFile => fileToCAResult(resourcesIntermediatePath, caFile))
    val saleLines: Stream[Sale] = saleFiles.flatMap(saleFile => fileToSaleResult(resourcesIntermediatePath, saleFile))
    logger.info(s"Importing lines from intermediates files for both KPI: ${caLines.length}")
    logger.info("Importing lines from intermediates files finish...")
    logger.info("Calculating Result Files...")

    // Top 100 CA by StoreId & Date
    writeCAByDayAndStore(caLines, resultPath, dateArg)

    // Top 100 CA by StoreId & Date Over 7 days
    writeCAByStoreOverPeriod(caFiles, resourcesIntermediatePath, resultPath, dateArg)

    // Top 100 Ventes by StoreId & Date
    writeSaleByDayAndStore(saleLines, resultPath, dateArg)

    // Top 100 Sale by StoreId & Date Over 7 days
    writeSaleByStoreOverPeriod(saleFiles, resourcesIntermediatePath, resultPath, dateArg)

    // Top 100 CA global by Date
    val caByDay = getGlobalCAByDay(aggregateCAByDate(caFiles, caLines, dateArg, resourcesIntermediatePath))
    fileWriter(resultPath, "top_100_ca_GLOBAL_"+transformDate(dateArg),caByDay)

    // Top 100 CA Global by Date Over 7 days
    val CaOverPeriod = getGlobalCAByDay(aggregateCAOverPeriod(caFiles, caLines, dateArg, resourcesIntermediatePath))
    fileWriter(resultPath, "top_100_ca_GLOBAL_"+transformDate(dateArg)+"-J7",CaOverPeriod)

    // Top 100 Ventes global by Date
    val saleByDay = getGlobalSaleByDay(aggregateSaleByDate(saleFiles, saleLines, dateArg, resourcesIntermediatePath))
    fileSaleWriter(resultPath, "top_100_ventes_GLOBAL_"+transformDate(dateArg), saleByDay)

    // Top 100 Ventes Global by Date Over 7 days
    val saleOverPeriod = getGlobalSaleByDay(aggregateSaleOverPeriod(saleFiles, saleLines, dateArg, resourcesIntermediatePath))
    fileSaleWriter(resultPath, "top_100_ventes_GLOBAL_"+transformDate(dateArg)+"-J7",saleOverPeriod)
    logger.info("Writing Result Write finish....")

    logger.info("closing program....")
  }
}
