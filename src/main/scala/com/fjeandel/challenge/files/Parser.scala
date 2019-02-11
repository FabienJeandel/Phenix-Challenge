package com.fjeandel.challenge.files

import java.time.LocalDate
import java.util.UUID


// Mapping & Parsing imported files : (Transaction, Article, CA, Sale)


case class Article(artId: Int, price: Double, storeId: UUID, date: LocalDate)
object ArticleParser {

   def parse(line: String, uuid: UUID, date: LocalDate): Article = {
    val arrayArticle = line.split('|')
    val artId = arrayArticle(0).toInt
    val price = arrayArticle(1).toDouble
    val storeId = UUID.fromString(uuid.toString)
    val dateFile = date
    Article(artId, price, storeId, dateFile)
  }
}


case class Transaction(txId: Int, date: LocalDate, storeId: UUID, artId: Int, qty: Int)
object TransactionParser  {

   def parse(line: String): Transaction = {
    val arrayTransac = line.split('|')
    val txId = arrayTransac(0).toInt
    val date = FileUtils.convertDate(arrayTransac(1).substring(0,8))
    val storeId = UUID.fromString(arrayTransac(2))
    val artId = arrayTransac(3).toInt
    val qty = arrayTransac(4).toInt
    Transaction(txId, date, storeId, artId, qty)
  }
}


case class CA(txId: Int, date: LocalDate, storeId: UUID, artId: Int, ca: Double)
object CAParser  {

  def parse(line: String): CA = {
    val arrayCA = line.split('|')
    val txId = arrayCA(0).toInt
    val date = FileUtils.convertDate(arrayCA(1).replace("-",""))
    val storeId = UUID.fromString(arrayCA(2))
    val artId = arrayCA(3).toInt
    val ca = arrayCA(4).toDouble
    CA(txId, date, storeId, artId, ca)
  }
}


case class Sale(txId: Int, date: LocalDate, storeId: UUID, artId: Int, qty: Int)
object SaleParser  {

  def parse(line: String): Sale = {
    val arraySale = line.split('|')
    val txId = arraySale(0).toInt
    val date = FileUtils.convertDate(arraySale(1).replace("-",""))
    val storeId = UUID.fromString(arraySale(2))
    val artId = arraySale(3).toInt
    val qty = arraySale(4).toInt
    Sale(txId, date, storeId, artId, qty)
  }
}