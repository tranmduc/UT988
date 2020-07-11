package FeatureExtraction

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object JoinFeature {
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
    val conf = new SparkConf().setAppName("JoinFeature").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("JoinFeature")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    val file_1_path = "out/UT/extract_feature/newmodel/loan_refund_scratch_sms/loan_refund_scratch_sms_05_2020/loan_refund_scratch_sms_05_2020.csv"

    val file_2_path = "in/20200508_v1/label.csv"

    val df1 = spark.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(file_1_path).persist()
//      .toDF("ISDN","TOTAL_SCRATCH","TOTAL_SCRATCH_VALUE","DATE_ID")

    val df2 = spark.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(file_2_path)
      .toDF("INDEX","ISDN","LABEL","AMT")
      .drop("INDEX")
      .persist()

//    df1.printSchema()
//    df2.printSchema()

//    val df = df1.join(df2,"ISDN")

    val df = df1.join(df2,Seq("ISDN"), "outer")
      .na.fill(0)
//    df.printSchema()

    print(df1.count() + " - " + df2.count + " - " + df.count())

    val saveFile_dir = "out/UT/extract_feature/newmodel/ut_feature/ut_feature_05_2020"
    val saveFile_name =  "ut_feature_05_2020.csv"

    saveCSV(df, saveFile_dir, saveFile_name, sc)

//    df.show(10)
  }
}
