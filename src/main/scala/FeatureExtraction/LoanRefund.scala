package FeatureExtraction

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object LoanRefund {
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
    val conf = new SparkConf().setAppName("LoanRefund").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("PreProcessing")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    val file_1_path = "in/20200518_v3_other/ut_loan_refund_v3.csv"

    val file_2_path = "in/20200508_v1/ut_loan_refund_202002_v1.csv"

    val df = spark.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(file_1_path)
      .toDF("ISDN","DATE_TRUNC","LOAN","LOAN_MONEY","REFUND","REFUND_MONEY").persist()

//    val df2 = spark.read
//      .option("sep", ",")
//      .option("inferSchema", "true")
//      .option("header", "true")
//      .csv(file_2_path)
//      .toDF("ISDN","DATE_TRUNC_02","LOAN","LOAN_MONEY","REFUND","REFUND_MONEY").persist()

//    df1.printSchema()
//    df2.printSchema()

//    val df = df1.join(df2,Seq("ISDN"), "outer").drop("DATE_TRUNC_01", "DATE_TRUNC_02")

//    val loan_refund_df = df.na.fill(0)
    //loan_refund_df.show(10)

//    print(df1.count() + " - " + df2.count + " - " + df.count())
    print(df.count())

    val divisionUDF = udf(division _)
    val isBiggerZeroUDF = udf(isBiggerZero _)
    val sumUDF = udf(sum)
    val averageUDF = udf(average _)

    val df1 = df.toDF("ISDN","DATE_TRUNC_03","LOAN_03","LOAN_MONEY_03","REFUND_03","REFUND_MONEY_03").filter(col("DATE_TRUNC_03") === "2019-11-01 00:00:00").persist()

    val df2 = df.toDF("ISDN","DATE_TRUNC_02","LOAN_02","LOAN_MONEY_02","REFUND_02","REFUND_MONEY_02").filter(col("DATE_TRUNC_02") === "2019-12-01 00:00:00").persist()

    val df3 = df.toDF("ISDN","DATE_TRUNC_01","LOAN_01","LOAN_MONEY_01","REFUND_01","REFUND_MONEY_01").filter(col("DATE_TRUNC_01") === "2020-01-01 00:00:00").persist()

    val joindf = df1.join(df2,Seq("ISDN"), "outer").join(df3,Seq("ISDN"), "outer")
      .drop("DATE_TRUNC_03", "DATE_TRUNC_02", "DATE_TRUNC_01")
      .persist()

    val loanrefund_df = joindf.na.fill(0).persist()

    print(df1.count() + " - " + df2.count + " - " + df3.count + " - " + loanrefund_df.count())
//    val loan_refund_extract = loan_refund_df
//      .withColumn("LOAN_MONEY/LOAN", divisionUDF(col("LOAN_MONEY"), col("LOAN")))
//      .withColumn("REFUND_MONEY/REFUND", divisionUDF(col("REFUND_MONEY"), col("REFUND")))
//      .withColumn("REFUND_MONEY/LOAN_MONEY", divisionUDF(col("REFUND_MONEY"), col("LOAN_MONEY")))
//      .withColumn("REFUND/LOAN", divisionUDF(col("REFUND"), col("LOAN")))
//
//      .withColumn("is_REFUND", isBiggerZeroUDF(col("REFUND")))
//      .withColumn("is_LOAN", isBiggerZeroUDF(col("LOAN")))
//
//      .withColumn("LOAN_SUM", sumUDF(col("LOAN_01"), col("LOAN")))
//      .withColumn("LOAN_MONEY_SUM", sumUDF(col("LOAN_MONEY_01"), col("LOAN_MONEY")))
//      .withColumn("REFUND_SUM", sumUDF(col("REFUND_01"), col("REFUND")))
//      .withColumn("REFUND_MONEY_SUM", sumUDF(col("REFUND_MONEY_01"), col("REFUND_MONEY")))
//
//      .withColumn("LOAN_MONEY_SUM/LOAN_SUM", divisionUDF(col("LOAN_MONEY_SUM"), col("LOAN_SUM")))
//      .withColumn("REFUND_MONEY_SUM/REFUND_SUM", divisionUDF(col("REFUND_MONEY_SUM"), col("REFUND_SUM")))
//      .withColumn("REFUND_MONEY_SUM/LOAN_MONEY_SUM", divisionUDF(col("REFUND_MONEY_SUM"), col("LOAN_MONEY_SUM")))
//      .withColumn("REFUND_SUM/LOAN_SUM", divisionUDF(col("REFUND_SUM"), col("LOAN_SUM")))
//
//      .withColumn("is_REFUND_SUM", isBiggerZeroUDF(col("REFUND_SUM")))
//      .withColumn("is_LOAN_SUM", isBiggerZeroUDF(col("LOAN_SUM")))
//
//      .withColumn("LOAN_AVG", averageUDF(col("LOAN_01"), col("LOAN")))
//      .withColumn("LOAN_MONEY_AVG", averageUDF(col("LOAN_MONEY_01"), col("LOAN_MONEY")))
//      .withColumn("REFUND_AVG", averageUDF(col("REFUND_01"), col("REFUND")))
//      .withColumn("REFUND_MONEY_AVG", averageUDF(col("REFUND_MONEY_01"), col("REFUND_MONEY")))
//
//      .drop("LOAN_01", "LOAN_MONEY_01", "REFUND_01", "REFUND_MONEY_01")



//    loan_refund_extract.select("*").filter(col("is_LOAN_SUM") === 0).show(10)

//    loan_refund_extract.printSchema()

//    loan_refund_extract.show(10)

    val saveFile_dir = "out/0806/loan_refund_04"
    val saveFile_name =  "loan_refund_04_other.csv"

    saveCSV(loanrefund_df, saveFile_dir, saveFile_name, sc)

  }
}
