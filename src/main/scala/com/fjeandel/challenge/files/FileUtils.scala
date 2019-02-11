package com.fjeandel.challenge.files

import java.io.{BufferedWriter, File, FileWriter}
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object FileUtils {

  val resourcesPathResults = "/home/pdemanget/adneom/phenix_stream/data/results"
  val yyyyMMdd: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

  // String to LocalDate
  def convertDate(str: String): LocalDate = {
    LocalDate.parse(str, yyyyMMdd)
  }

  // String to LocalDate
  def transformDate(localDate: LocalDate): String = {
    localDate.toString.replace("-","")
  }

  // Extract LocalDate from FileName
  def extractDate(fileName:String): LocalDate = {
    val ext = extension(fileName)
    LocalDate.parse(fileName.substring(fileName.length()-ext.length()-8, fileName.length()-ext.length), yyyyMMdd)
  }

  def extractResultDate(fileName:String): LocalDate = {
    LocalDate.parse(fileName.substring(fileName.indexOf('_')+1, fileName.indexOf('_')+9), yyyyMMdd)
  }

  // Define extension of file
  def extension(fileName:String): String = {
    fileName.substring(fileName.indexOf('.'))
  }

  // Define uuid of article file
  def uuid(fileName: String): String = {
    fileName.substring(fileName.indexOf('-')+1, fileName.indexOf('-') + 37)
  }

  // Transformation List[File] to List[String]
  def getName(file: File): String = {
    file.getName
  }

  def parseTupleToLine(tuple: (Int, Any)): String = {
    tuple.toString().replace(",","|").replace("(","").replace(")","\n")
  }

  def roundDouble(double: Double): Double = {
    math.BigDecimal(double).setScale(2,BigDecimal.RoundingMode.HALF_UP).toDouble
  }


  // Load Files from Directory
  def getListOfFiles(dir: String): Stream[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(file => file.isFile).map(file => getName(file)).toStream
    } else {
      Stream.empty
    }
  }

  // Load Files from Directory
  def getListOfFiles(dir: String, kpi: String): Stream[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(file => file.isFile && file.getName.startsWith(kpi)).map(file => getName(file)).toStream
    } else {
      Stream.empty
    }
  }


  // Ecriture des résultats dans un fichier
  def fileWriter(path: String, fileName: String, text: Stream[(Int, Double)]): Unit = {
    val file = new File(path,fileName)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("product|ca" + "\n")
    text.foreach{result =>
      val line = parseTupleToLine(result)
      bw.write(line)
    }
    bw.close()
  }

  // Ecriture des résultats dans un fichier
  def fileSaleWriter(path: String, fileName: String, text: Stream[(Int, Int)]): Unit = {
    val file = new File(path,fileName)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("product|qty" + "\n")
    text.foreach{result =>
      val line = parseTupleToLine(result)
      bw.write(line)
    }
    bw.close()
  }


}
