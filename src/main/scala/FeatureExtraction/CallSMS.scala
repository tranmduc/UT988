package FeatureExtraction

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object CallSMS {
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

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("CallSMS").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("CallSMS")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    val file_1_path = "in/20200518_v3_other/ut_call_sms_201911_v3.csv"

    val file_2_path = "in/20200518_v3_other/ut_call_sms_201912_v3.csv"

    val file_3_path = "in/20200518_v3_other/ut_call_sms_202001_v3.csv"

    val df1 = spark.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(file_1_path)
      .select("MSISDN", "TOTAL_CALL_OG", "TOTAL_DURATION_OG")
      .toDF("ISDN","TOTAL_CALL_OG_03","TOTAL_DURATION_OG_03").persist()

    val df2 = spark.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(file_2_path)
      .select("MSISDN", "TOTAL_CALL_OG", "TOTAL_DURATION_OG")
      .toDF("ISDN", "TOTAL_CALL_OG_02", "TOTAL_DURATION_OG_02").persist()

    val df3 = spark.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(file_3_path)
      .select("MSISDN", "TOTAL_CALL_OG", "TOTAL_DURATION_OG")
      .toDF("ISDN", "TOTAL_CALL_OG_01", "TOTAL_DURATION_OG_01").persist()

//            df1.printSchema()
//            df2.printSchema()

    val df = df1.join(df2,Seq("ISDN"), "outer").join(df3,Seq("ISDN"), "outer").persist()
//        df.printSchema()
    val callsms_df = df.na.fill(0).persist()
//    callsms_df.show(10)

        print(df1.count() + " - " + df2.count + " - " + df3.count + " - " + df.count())

    val divisionUDF = udf(division _)
    val isBiggerZeroUDF = udf(isBiggerZero _)
    val sumUDF = udf(sum)
    val averageUDF = udf(average _)

//    val callsms_extract = callsms_df
//      .withColumn("TOTAL_CALL_OG_SUM", sumUDF(col("TOTAL_CALL_OG_01"), col("TOTAL_CALL_OG")))
//      .withColumn("TOTAL_DURATION_OG_SUM", sumUDF(col("TOTAL_DURATION_OG_01"), col("TOTAL_DURATION_OG")))
//
//      .withColumn("TOTAL_CALL_OG_AVG", averageUDF(col("TOTAL_CALL_OG_01"), col("TOTAL_CALL_OG")))
//      .withColumn("TOTAL_DURATION_OG_AVG", averageUDF(col("TOTAL_DURATION_OG_01"), col("TOTAL_DURATION_OG")))
//
//      .drop("TOTAL_CALL_OG_01", "TOTAL_DURATION_OG_01")

    //    callsms_extract.select("*").filter(col("is_SCRATCH_SUM") === 0).show(10)

//    callsms_extract.printSchema()

//    callsms_extract.show(10)

    val saveFile_dir = "out/0806/call_04"
    val saveFile_name =  "call_04.csv"

    saveCSV(callsms_df, saveFile_dir, saveFile_name, sc)

  }
}
