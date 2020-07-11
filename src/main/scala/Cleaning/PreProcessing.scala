package Cleaning

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.{min, max, count}
import org.apache.spark.sql.Row

object PreProcessing {
  def saveCSV (df: DataFrame, savePath: String, newName: String, sc: SparkContext) = {
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
      case e: Exception => e.printStackTrace
    }
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("PreProcessing").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("PreProcessing")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    val file_1_path = "out/UT/extract_feature/newmodel/ut_feature/ut_feature_05_2020/ut_feature_05_2020.csv"

    val file_2_path = "out/UT/EDA/eda_1/eda_1.csv"

    val df1 = spark.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(file_1_path)
//      .toDF("ISDN","TOTAL_SCRATCH","TOTAL_SCRATCH_VALUE","DATE_ID")
      .persist()

    val df2 = spark.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(file_2_path)
//      .toDF("ISDN","LABEL")
      .persist()

    //df1.printSchema()
    //df2.printSchema()

    //print(df1.count() + " - " + df2.count)

    //print(df2.count)

    //print(df2.select("ISDN").distinct().count())

//    val df = df1.join(df2,"ISDN").select("ISDN", "TOTAL_SCRATCH_VALUE", "TOTAL_SCRATCH_VALUE_SUM")

//    print(df1.count() + " - " + df2.count + " - " + df.count())

    var df = df1.select("ISDN", "TOTAL_SCRATCH_VALUE", "TOTAL_SCRATCH_VALUE_SUM", "AMT").filter(df1("LOAN_SUM") === 0)
      .filter(df1("LABEL") === 0)
//      .filter(df1("TOTAL_SCRATCH_VALUE") === 0 || df1("TOTAL_SCRATCH_VALUE_SUM") === 0)
      .filter(df1("TOTAL_SCRATCH_VALUE_SUM") === 0)
//      .filter(df1("AMT") === 5.0)
        .groupBy("AMT")
        .agg(count("ISDN"))

//    df = df.select("ISDN", "AMT").filter(df("AMT") === 6.0)
//    df.agg(max("AMT"), count("ISDN")).show()
//    df.agg(max("TOTAL_SCRATCH_VALUE"), max("TOTAL_SCRATCH_VALUE_SUM"), min("TOTAL_SCRATCH_VALUE"), min("TOTAL_SCRATCH_VALUE_SUM")).show()

    val saveFile_dir = "out/UT/EDA/eda_3"
    val saveFile_name =  "eda_3.csv"
    df1.filter(df("AMT") === 0).show()
    saveCSV(df, saveFile_dir, saveFile_name, sc)

//    df.show(10)

    //print(df.count)
  }
}
