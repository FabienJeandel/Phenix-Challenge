package com.fjeandel.challenge.results

import java.time.LocalDate
import com.fjeandel.challenge.files.FileUtils.roundDouble
import com.fjeandel.challenge.files.{CA, CAFile, Sale, SaleFile}
import com.fjeandel.challenge.services.LoadFile

// Aggregate data for calculate Result, outputs: Stream[CA], Stream[Sale]

object AggregateResult {

  // Aggregate CA by Date
  def aggregateCAByDate(caFile: Stream[CAFile], caResult: Stream[CA], date: LocalDate, resourcesIntermediatePath: String): Stream[CA] = {

    caFile
      .filter(caFile => caFile.date.equals(date))
      .flatMap(caFile => LoadFile.fileToCAResult(resourcesIntermediatePath, caFile))
      .map(caResult => CA(caResult.txId, caResult.date, caResult.storeId, caResult.artId, roundDouble(caResult.ca)))
  }

  // Aggregate Sales by Date
  def aggregateSaleByDate(saleFile: Stream[SaleFile], saleResult: Stream[Sale], date: LocalDate, resourcesIntermediatePath: String): Stream[Sale] = {

    saleFile
      .filter(saleFile => saleFile.date.equals(date))
      .flatMap(saleFile => LoadFile.fileToSaleResult(resourcesIntermediatePath, saleFile))
      .map(saleResult => Sale(saleResult.txId, saleResult.date, saleResult.storeId, saleResult.artId, saleResult.qty))
  }

  // Aggregate CA over 7 days
  def aggregateCAOverPeriod(caFile: Stream[CAFile], caResult: Stream[CA], date: LocalDate, resourcesIntermediatePath: String): Stream[CA] = {

    val beginDate = LocalDate.of(date.getYear,date.getMonth,date.getDayOfMonth-7)
    caFile
      .filter(caFile => caFile.date.isAfter(beginDate) || caFile.date.equals(date))
      .flatMap(caFile => LoadFile.fileToCAResult(resourcesIntermediatePath, caFile))
      .map(caResult => CA(caResult.txId, caResult.date, caResult.storeId, caResult.artId, roundDouble(caResult.ca)))
  }

  // Aggregate Sale over 7 days
  def aggregateSaleOverPeriod(saleFile: Stream[SaleFile], caResult: Stream[Sale], date: LocalDate, resourcesIntermediatePath: String): Stream[Sale] = {

    val beginDate = LocalDate.of(date.getYear,date.getMonth,date.getDayOfMonth-7)
    saleFile
      .filter(saleFile => saleFile.date.isAfter(beginDate) || saleFile.date.equals(date))
      .flatMap(saleFile => LoadFile.fileToSaleResult(resourcesIntermediatePath, saleFile))
      .map(saleResult => Sale(saleResult.txId, saleResult.date, saleResult.storeId, saleResult.artId,saleResult.qty))
  }
}
