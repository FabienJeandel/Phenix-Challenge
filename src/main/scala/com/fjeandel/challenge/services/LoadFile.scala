package com.fjeandel.challenge.services

import java.time.LocalDate
import java.util.UUID
import com.fjeandel.challenge.files._
import scala.io.Source


// Transform File to Objects

object LoadFile {

  //File to Article
  def fileToArticles(articlesPath: String, articleFile: ArticleFile): Stream[Article] = {
    val uuid: UUID = articleFile.uuid
    val date: LocalDate = articleFile.date
    Source
      .fromFile(articlesPath + articleFile.fileName).getLines().toStream
      .map(line => ArticleParser.parse(line, uuid, date))
  }

  // File to Transaction
  def fileToTransaction(transactionsPath: String, transactionFiles: TransactionFile): Stream[Transaction] = {
    Source
      .fromFile(transactionsPath + transactionFiles.fileName).getLines().toStream
      .map(line => TransactionParser.parse(line))
  }

  // File to CA
  def fileToCAResult(intermediatePath: String, cafile: CAFile): Stream[CA] = {
    Source
      .fromFile(intermediatePath + cafile.fileName).getLines().toStream
      .map(line => CAParser.parse(line))
  }

  // File to Sale
  def fileToSaleResult(intermediatePath: String, saleFile: SaleFile): Stream[Sale] = {
      Source
        .fromFile(intermediatePath + saleFile.fileName).getLines().toStream
        .map(line => SaleParser.parse(line))
  }
}
