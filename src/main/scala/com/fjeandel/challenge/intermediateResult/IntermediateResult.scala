package com.fjeandel.challenge.intermediateResult

import java.io.{BufferedWriter, File, FileWriter}
import com.fjeandel.challenge.files.FileUtils.transformDate
import com.fjeandel.challenge.files.{IntermediateCA, IntermediateSales}
import com.fjeandel.challenge.results.Result
import com.typesafe.scalalogging.StrictLogging



// Writing & Parsing IntermediateResult

object IntermediateResult extends StrictLogging {

  // Parse lines of intermediateSales File
  def SalesResultToString (result: IntermediateSales): String = {
    result.toString.replace("IntermediateSales(","").replace(")","\n").replace(',','|')
  }

  // Parse lines of intermediateCA File
  def CAResultToString (result: IntermediateCA): String = {
    result.toString.replace("IntermediateCA(","").replace(")","\n").replace(',','|')
  }

  // Write Intermediate results split by kpi, date & storeId
  def writeIntermediateFiles(intermediateFile: Stream[Result], resourcesIntermediatePath: String): Unit = {

    var nbFiles = 0
    logger.info(s"Writing Files in directory: $resourcesIntermediatePath...")
    intermediateFile.groupBy(result => (result.date, result.storeId))
      .foreach { case ((date, storeId), results) =>

        val caFileName: String = "ca_" + transformDate(date) + "-" + storeId
        val saleFileName: String = "ventes_" + transformDate(date) + "-" + storeId

        val caFile = new File(resourcesIntermediatePath, caFileName)
        val saleFile = new File(resourcesIntermediatePath, saleFileName)
        val caBw = new BufferedWriter(new FileWriter(caFile))
        val saleBw = new BufferedWriter(new FileWriter(saleFile))
        results
          .foreach {result =>
            val ca =  CAResultToString(IntermediateCA(result))
            val sale = SalesResultToString(IntermediateSales(result))
            caBw.write(ca)
            saleBw.write(sale)
          }
          caBw.close()
          saleBw.close()
          nbFiles+= 1
      }
      logger.info(s"Total Intermediate Files exported: ${nbFiles*2}")
      logger.info("Intermediate File Writing is finish...")
  }
}
