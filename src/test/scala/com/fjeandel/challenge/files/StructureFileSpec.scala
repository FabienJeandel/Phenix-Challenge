package com.fjeandel.challenge.files

import java.time.LocalDate
import java.util.UUID
import org.scalatest.{Matchers, WordSpec}

class StructureFileSpec extends WordSpec with Matchers {


    "TransactionFileSpec" should {
      "return a " in {
        val date = LocalDate.of(2019, 2, 10)
        val expected = TransactionFile("transactions_20190210.data", ".data", date)
        val result = TransactionFile.apply("transactions_20190210.data")
        result shouldEqual expected
      }
    }


  "ArticleFileSpec" should {
    "return a " in {
      val date = LocalDate.of(2019, 2, 10)
      val uuid = UUID.fromString("2a4b6b81-5aa2-4ad8-8ba9-ae1a006e7d71")
      val expected = ArticleFile("reference_prod-2a4b6b81-5aa2-4ad8-8ba9-ae1a006e7d71_20190210.data",".data",uuid, date)
      val result = ArticleFile.apply("reference_prod-2a4b6b81-5aa2-4ad8-8ba9-ae1a006e7d71_20190210.data")
      result shouldEqual expected
    }
  }


  "CAFileSpec" should {
    "return a " in {
      val date = LocalDate.of(2019, 2, 10)
      val uuid = UUID.fromString("2a4b6b81-5aa2-4ad8-8ba9-ae1a006e7d71")
      val expected = CAFile("ca_20190210-2a4b6b81-5aa2-4ad8-8ba9-ae1a006e7d71",date, uuid)
      val result = CAFile.apply("ca_20190210-2a4b6b81-5aa2-4ad8-8ba9-ae1a006e7d71")
      result shouldEqual expected
    }
  }

  "SaleFileSpec" should {
    "return a " in {
      val date = LocalDate.of(2019, 2, 10)
      val uuid = UUID.fromString("2a4b6b81-5aa2-4ad8-8ba9-ae1a006e7d71")
      val expected = SaleFile("ventes_20190210-2a4b6b81-5aa2-4ad8-8ba9-ae1a006e7d71",date, uuid)
      val result = SaleFile.apply("ventes_20190210-2a4b6b81-5aa2-4ad8-8ba9-ae1a006e7d71")
      result shouldEqual expected
    }
  }

}
