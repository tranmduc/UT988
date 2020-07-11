package EDA

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object Explore {
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

  def revenueCheck(a: Int, b: Int, c: Int): Int = {
    val check = 55000
    if (a >= check || b >= check || c >= check){
      1
    }else{
      0
    }
  }

  def scratchAvgAmountCheck(a: Int, b: Int, c: Int, d: Int): Int = {
    val avg = (a+b+c)/3
    val total = d + (d/10)
    if (avg >= total){
      1
    }else{
      0
    }
  }

  val m = 2

  def scratchAllAmountCheck(a: Int, b: Int, c: Int, d: Int): Int = {
//    val total = d + (d/10)
    val total = d*m*1.1
    if (a >= total && b >= total && c >= total){
      1
    }else{
      0
    }
  }

  val n = 1

  def revenueAllAmountCheck(a: Int, b: Int, c: Int, d: Int): Int = {
    //    val total = d + (d/10)
    val total = d*n*1.1
    if (a >= total && b >= total && c >= total){
      1
    }else{
      0
    }
  }

  def scratchAllAmountCheck2(a: Int, b: Int, c: Int, d: Int): Int = {
    //    val total = d + (d/10)
    val total = d*m*1.1
    if ((a >= total && b >= total) || (b >= total && c >= total) || (c >= total && a >= total)){
      1
    }else{
      0
    }
  }

  def scratchAllAmountCheckSmall(a: Int, b: Int, c: Int, d: Int): Int = {
    val total = d*1.1*7
    if (a < total && b < total && c < total){
      1
    }else{
      0
    }
  }

  def isRefundBigger(a: Int, b: Int, c:Int, d:Int, e:Int, f:Int): Int = {
    if (a <= b && c <= d && e <= f){
      1
    }else{
      0
    }
  }

  def isContinuous(a: Int, b: Int): Int = {
    if (a > 0 && b > 0) 1
    else 0
  }

  val thres1 = 1.5
  val thres2 = 2
  val thres3 = 0.8

  def revenueThreshold(a: Int, b: Int, c: Int, d: Int): Int = {
    val avg = (a + b +c)/3
    if (avg >= d * thres1) 1
    else 0
  }

  def scratchThreshold(a: Int, b: Int, c: Int, d: Int): Int = {
    val avg = (a + b +c)/3
    if (avg >= d * thres2) 1
    else 0
  }

  def ratioVariance(a: Int, b: Int, c: Int): Int = {
    val avg = (a + b + c)/3
    if (c >= avg * thres3) 1
    else 0
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("Explore").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Explore")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    val path_1 = "out/0806/everLoan_04/everLoan_04_other.csv"
    val path_2 = "out/0806/everLoan_04_ut/everLoan_04_ut.csv"

    val df1 = spark.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(path_1)
//      .toDF()
      .persist()

    val df2 = spark.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(path_2)
      //      .toDF()
      .persist()

    val revenueCheckUDF = udf(revenueCheck _)
    val scratchAvgAmountCheckUDF = udf(scratchAvgAmountCheck _)
    val scratchAllAmountCheckUDF = udf(scratchAllAmountCheck _)
    val scratchAllAmountCheckSmallUDF = udf(scratchAllAmountCheckSmall _)
    val scratchAllAmountCheck2UDF = udf(scratchAllAmountCheck2 _)
    val revenueAllAmountCheckUDF = udf(revenueAllAmountCheck _)
    val isRefundBiggerUDF = udf(isRefundBigger _)
    val isContinuousUDF = udf(isContinuous _)
    val revenueThresholdUDF = udf(revenueThreshold _)
    val scratchThresholdUDF = udf(scratchThreshold _)
    val ratioVarianceUDF = udf(ratioVariance _)

//    val eda2 = df2
//      .persist()
//
//    print(eda2.count())

    val no_ut = df1
//      .withColumn("REVENUE_CHECK", revenueCheckUDF(col("REVENUE_01"),col("REVENUE_02"),col("REVENUE_03")))
//      .withColumn("ScratchAvgAmt", scratchAvgAmountCheckUDF(col("TOTAL_SCRATCH_VALUE_03"), col("TOTAL_SCRATCH_VALUE_02"), col("TOTAL_SCRATCH_VALUE_01"), col("AMOUNT")))
//      .withColumn("ScratchAllAmt", scratchAllAmountCheckUDF(col("TOTAL_SCRATCH_VALUE_03"), col("TOTAL_SCRATCH_VALUE_02"), col("TOTAL_SCRATCH_VALUE_01"), col("AMOUNT")))
//      .withColumn("ScratchAllAmtSmall", scratchAllAmountCheckSmallUDF(col("TOTAL_SCRATCH_VALUE_03"), col("TOTAL_SCRATCH_VALUE_02"), col("TOTAL_SCRATCH_VALUE_01"), col("AMOUNT")))
//      .withColumn("ScratchAllAmt2", scratchAllAmountCheck2UDF(col("TOTAL_SCRATCH_VALUE_03"), col("TOTAL_SCRATCH_VALUE_02"), col("TOTAL_SCRATCH_VALUE_01"), col("AMOUNT")))
//      .withColumn("RevenueAllAmt", revenueAllAmountCheckUDF(col("REVENUE_03"), col("REVENUE_02"), col("REVENUE_01"), col("AMOUNT")))
//        .withColumn("isRefundBigger", isRefundBiggerUDF(col("LOAN_01"), col("REFUND_01"), col("LOAN_02"), col("REFUND_02"), col("LOAN_03"), col("REFUND_03")))
        .withColumn("revenueContinuous", isContinuousUDF(col("REVENUE_01"),col("REVENUE_02")))
        .withColumn("scratchContinuous", isContinuousUDF(col("TOTAL_SCRATCH_01"), col("TOTAL_SCRATCH_02")))
        .withColumn("revenueThreshold", revenueThresholdUDF(col("REVENUE_03"), col("REVENUE_02"), col("REVENUE_01"), col("AMOUNT")))
        .withColumn("scratchThreshold", scratchThresholdUDF(col("TOTAL_SCRATCH_VALUE_03"), col("TOTAL_SCRATCH_VALUE_02"), col("TOTAL_SCRATCH_VALUE_01"), col("AMOUNT")))
        .withColumn("ratioScratchVariance", ratioVarianceUDF(col("TOTAL_SCRATCH_VALUE_03"), col("TOTAL_SCRATCH_VALUE_02"), col("TOTAL_SCRATCH_VALUE_01")))
        .withColumn("ratioRevenueVariance", ratioVarianceUDF(col("REVENUE_03"), col("REVENUE_02"), col("REVENUE_01")))
        .persist()
//    val saveFile_dir = "out/0806/everLoan_04_ut"
//    val saveFile_name =  "everLoan_04_ut.csv"

    val ut = df2
//      .withColumn("ScratchAllAmtSmall", scratchAllAmountCheckSmallUDF(col("TOTAL_SCRATCH_VALUE_03"), col("TOTAL_SCRATCH_VALUE_02"), col("TOTAL_SCRATCH_VALUE_01"), col("AMOUNT")))
//      .withColumn("isRefundBigger", isRefundBiggerUDF(col("LOAN_01"), col("REFUND_01"), col("LOAN_02"), col("REFUND_02"), col("LOAN_03"), col("REFUND_03")))
        .withColumn("revenueContinuous", isContinuousUDF(col("REVENUE_01"),col("REVENUE_02")))
        .withColumn("scratchContinuous", isContinuousUDF(col("TOTAL_SCRATCH_01"), col("TOTAL_SCRATCH_02")))
        .withColumn("revenueThreshold", revenueThresholdUDF(col("REVENUE_03"), col("REVENUE_02"), col("REVENUE_01"), col("AMOUNT")))
        .withColumn("scratchThreshold", scratchThresholdUDF(col("TOTAL_SCRATCH_VALUE_03"), col("TOTAL_SCRATCH_VALUE_02"), col("TOTAL_SCRATCH_VALUE_01"), col("AMOUNT")))
        .withColumn("ratioScratchVariance", ratioVarianceUDF(col("TOTAL_SCRATCH_VALUE_03"), col("TOTAL_SCRATCH_VALUE_02"), col("TOTAL_SCRATCH_VALUE_01")))
        .withColumn("ratioRevenueVariance", ratioVarianceUDF(col("REVENUE_03"), col("REVENUE_02"), col("REVENUE_01")))
        .persist()
    val cnt1 = no_ut
//      .filter(col("ScratchAvgAmt") === 1)
//      .filter(col("ScratchAllAmt") === 1)
//      .filter(col("RevenueAllAmt") === 0)
//      .filter(col("ScratchAllAmtSmall") === 1)
//      .filter(col("ScratchAllAmt2") === 1)
//        .filter(col("isRefundBigger") === 0)
//      .filter(col("revenueContinuous") === 1)
//      .filter(col("scratchContinuous") === 1)
//      .filter(col("revenueThreshold") === 1)
//      .filter(col("scratchThreshold") === 1)
//      .filter(col("ratioScratchVariance") === 1)
//      .filter(col("ratioRevenueVariance") === 1)
//      .filter(col("STATUS") === 1)
        .filter(col("revenueContinuous") === 1 || col("scratchContinuous") === 1)
        .filter(col("revenueThreshold") === 1 || col("scratchThreshold") === 1)
        .filter(col("ratioScratchVariance") === 1 || col("ratioRevenueVariance") === 1)
//      .count()
      .persist()

    val cnt2 = ut
      //      .filter(col("ScratchAvgAmt") === 1)
      //      .filter(col("ScratchAllAmt") === 1)
      //      .filter(col("RevenueAllAmt") === 0)
//      .filter(col("ScratchAllAmtSmall") === 1)
      //      .filter(col("ScratchAllAmt2") === 1)
//      .filter(col("isRefundBigger") === 0)
//      .filter(col("revenueContinuous") === 1)
//      .filter(col("scratchContinuous") === 1)
//      .filter(col("revenueThreshold") === 1)
//      .filter(col("scratchThreshold") === 1)
//      .filter(col("ratioScratchVariance") === 1)
//      .filter(col("ratioRevenueVariance") === 1)
//      .filter(col("STATUS") === 1)
    //      .count()
      .filter(col("revenueContinuous") === 1 || col("scratchContinuous") === 1)
      .filter(col("revenueThreshold") === 1 || col("scratchThreshold") === 1)
      .filter(col("ratioScratchVariance") === 1 || col("ratioRevenueVariance") === 1)
      .persist()

    print(cnt1.count() + "-" + cnt1.filter(col("STATUS") === 1).count())
    print("\n")
    print(cnt2.count() + "-" + cnt2.filter(col("STATUS") === 1).count())

//    saveCSV(eda, saveFile_dir, saveFile_name, sc)
  }
}

