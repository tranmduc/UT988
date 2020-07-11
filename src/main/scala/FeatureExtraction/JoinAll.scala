package FeatureExtraction

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object JoinAll {
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
    val conf = new SparkConf().setAppName("JoinAll").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("JoinAll")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    val file_1_path = "out/0806/call_04/call_04_other.csv"

    val file_2_path = "out/0806/loan_refund_04/loan_refund_04_other.csv"

    val file_3_path = "out/0806/revenue_04/revenue_04_other.csv"

    val file_4_path = "out/0806/scratch_04/scratch_04_other.csv"

    val file_5_path = "in/20200518_v3_other/ut_label.csv"

    val file_6_path = "in/20200518_v3_other/ut_subscriber_info_v3.csv"

    val df1 = spark.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(file_1_path)
      .persist()

    val df2 = spark.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(file_2_path)
      .persist()

    val df3 = spark.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(file_3_path)
      .persist()

    val df4 = spark.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(file_4_path)
      .persist()

    val df5 = spark.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(file_5_path)
      .toDF("ISDN","AMOUNT","STATUS")
      .persist()

    val df6 = spark.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(file_6_path)
      .persist()

    val df = df1.join(df2,Seq("ISDN"), "outer")
      .join(df3,Seq("ISDN"), "outer")
      .join(df4,Seq("ISDN"), "outer")
      .join(df5,Seq("ISDN"), "outer")
      .na.fill(0)
      .join(df6,Seq("ISDN"), "outer")

    print(df1.count() + " - " + df2.count + " - " + df3.count + " - " + df4.count + " - " + df5.count + " - " + df6.count + " - " + df.count())

    val saveFile_dir = "out/0806/raw_04"
    val saveFile_name =  "raw_04_other.csv"

    saveCSV(df, saveFile_dir, saveFile_name, sc)
  }
}

