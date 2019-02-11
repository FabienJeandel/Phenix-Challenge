package com.fjeandel.challenge.files

import java.time.LocalDate
import org.scalatest.{Matchers, WordSpec}


class FileUtilsSpec extends WordSpec with Matchers {


  "convertDate" should {
    "return a " in {
      val dateString: String = "20190212"
      val expected = LocalDate.of(2019, 2, 12)
      val result = FileUtils.convertDate(dateString)
      result shouldEqual expected
    }
  }

  "transformDate" should {
    "return a " in {
      val date = LocalDate.of(2019, 2, 12)
      val expected: String = "20190212"
      val result = FileUtils.transformDate(date)
      result shouldEqual expected
    }
  }


}
