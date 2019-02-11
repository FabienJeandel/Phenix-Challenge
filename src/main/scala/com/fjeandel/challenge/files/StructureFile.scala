package com.fjeandel.challenge.files

import java.time.LocalDate
import java.util.UUID

import com.fjeandel.challenge.files
import com.fjeandel.challenge.files.FileUtils.{extension, extractDate, extractResultDate}
import com.fjeandel.challenge.results.Result


// File structure for imported files:  (Transaction, Article, IntermediateCA, IntermediateSale, CA, Sale)


case class TransactionFile(fileName: String, extension:String, date: LocalDate)

object TransactionFile {

    def apply(fileName: String): TransactionFile = {
      val name = fileName
      val ext = extension(fileName)
      val date = extractDate(fileName)
      files.TransactionFile(name, ext, date)
    }
}



case class ArticleFile(fileName: String, extension: String, uuid: UUID, date: LocalDate)
object ArticleFile {

  def apply(fileName: String): ArticleFile = {
    val name = fileName
    val ext = extension(fileName)
    val uuid = UUID.fromString(fileName.substring(fileName.indexOf('-')+1, fileName.indexOf('-') + 37))
    val date = extractDate(fileName)
    files.ArticleFile(name, ext, uuid, date)
  }
}


case class IntermediateSales(txId: Int, date: LocalDate, storeId: UUID, artId: Int, qty: Int)
object IntermediateSales {

  def apply(result: Result): IntermediateSales = {
    IntermediateSales(result.txId, result.date, result.storeId, result.artId, result.qty)
  }
}


case class IntermediateCA(txId: Int, date: LocalDate, storeId: UUID, artId: Int, ca: Double)
object IntermediateCA {

  def apply(result: Result): IntermediateCA = {
    IntermediateCA(result.txId, result.date, result.storeId, result.artId, result.ca)
  }
}


case class CAFile(fileName: String, date: LocalDate, uuid: UUID)
object CAFile {

  def apply(fileName: String): CAFile = {
    val name = fileName
    val date = extractResultDate(fileName)
    val uuid = UUID.fromString(FileUtils.uuid(fileName))
    CAFile(name, date, uuid)
  }
}


case class SaleFile(fileName: String, date: LocalDate, uuid: UUID)
object SaleFile {

  def apply(fileName: String): SaleFile = {
    val name = fileName
    val date = extractResultDate(fileName)
    val uuid = UUID.fromString(FileUtils.uuid(fileName))
    SaleFile(name, date, uuid)
  }
}
