package vn.mobifone.bigdata.ut988predict.util

import java.io.File

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object Utilities {
    def roundAt(n: Double, p: Int): Double = {
        val s = math pow (10, p);
        return (math round n * s) / s
    }

    def division(a: Int, b: Int): Double = {
        if(a == 0 || b == 0){
            0
        }else{
            roundAt(a.toDouble/b, 2)
        }
    }

    def sum = (a: Int, b: Int) => {
        a + b
    }

    def sum3 = (a: Int, b: Int, c: Int) => {
        a + b + c
    }

    def average(a: Int, b: Int): Double = {
        val sum = a + b
        roundAt(sum.toDouble/2, 2)
    }

    def isBiggerZero(a: Int): Int = {
        if (a > 0){
            1
        }else{
            0
        }
    }

    def saveCSV (df: DataFrame, savePath: String, newName: String, sc: SparkContext, logger: Logger) = {
        try{
            df.coalesce(1)
              .write.mode("overwrite")
              .format("csv")
              .option("header", "true")
              .save(savePath)

            val fs = FileSystem.get(sc.hadoopConfiguration)
            val filePath = savePath + "/"
            val fileName = fs.globStatus(new Path(filePath+"part*"))(0).getPath.getName

            fs.rename(new Path(filePath+fileName), new Path(filePath+newName))
        }catch{
            case e: Exception => logger.error(e.getMessage)
        }
    }

    def readFolder(spark: SparkSession, path: String): DataFrame ={
        spark.read
          .option("sep", ",")
          .option("inferSchema", "true")
          .option("header", "true")
          .csv(path)
          .toDF().persist()
    }

    def readFolderWithJoin(spark: SparkSession, path: String, joinColumn : String, joinType: String, dropColumn: Array[String]): DataFrame = {
        val files = this.getListOfFiles(path)
        var df : DataFrame = null
        for (f <- files){
            if (df == null) {
                var df1 =  spark.read
                  .option("sep", ",")
                  .option("inferSchema", "true")
                  .option("header", "true")
                  .csv(f.getPath)
                  .toDF().persist()
                df1 = df1.drop(dropColumn.toSeq: _*)
                val newColumnList = this.reColumnName(df1.columns, f.getName, joinColumn).toSeq
                df = df1.toDF(newColumnList: _*)
            }else{
                var df1 =  spark.read
                  .option("sep", ",")
                  .option("inferSchema", "true")
                  .option("header", "true")
                  .csv(f.getPath)
                  .toDF().persist()
                df1 = df1.drop(dropColumn.toSeq: _*)
                val newColumnList = this.reColumnName(df1.columns, f.getName, joinColumn).toSeq
                val df2 = df1.toDF(newColumnList: _*)
                df = df.join(df2,Seq(joinColumn), joinType)
            }
        }
        return df
    }

    def reColumnName(columns : Array[String], fileName: String, joinColumn : String ): Array[String] = {
        val prefixs = fileName.split("\\_", -1)
        var last = prefixs(prefixs.length - 1)
        last = last.split("\\.", -1)(0)
        val rs : Array[String] = new Array[String](columns.length)
        var i = 0
        for (c <- columns){
            if (c.equals(joinColumn)){
                rs(i) = c
            }else {
                rs(i) = c + "_" + last
            }
            i += 1
        }
        return rs
    }

    def getListOfFiles(dir: String):List[File] = {
        val d = new File(dir)
        if (d.exists && d.isDirectory) {
            d.listFiles.filter(_.isFile).filter(_.getName.endsWith(".csv")).toList
        } else {
            List[File]()
        }
    }

}
