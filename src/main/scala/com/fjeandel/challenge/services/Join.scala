package com.fjeandel.challenge.services

import java.time.LocalDate
import java.util.UUID
import com.fjeandel.challenge.files.{ArticleFile, FileUtils, Transaction}
import com.fjeandel.challenge.results.Result
import com.typesafe.scalalogging.StrictLogging


object Join extends StrictLogging{

  // Join two Streams & calculate CA by line
  def joinStream(articleFiles: Stream[ArticleFile], transactions: Stream[Transaction], articlesPath: String): Stream[Result] = {

    logger.info(s"Nb article's files for Join: ${articleFiles.length}")
    logger.info(s"Nb transaction's lines for Join: ${transactions.length}")
    val result = transactions.flatMap { transaction =>
      val storeId: UUID = transaction.storeId
      val date: LocalDate = transaction.date
      val artId: Int = transaction.artId
      articleFiles
        .filter(articleFile => articleFile.uuid.equals(storeId) && articleFile.date.equals(date))
        .flatMap { articleFile =>
          LoadFile.fileToArticles(articlesPath, articleFile)
            .filter(article => article.artId.equals(artId))
            .map(article => Result(transaction.txId, transaction.date, transaction.storeId, transaction.artId, transaction.qty, article.price, FileUtils.roundDouble(article.price * transaction.qty)))
        }
    }
    logger.info(s"Join for Transaction File...")
    result
  }
}
