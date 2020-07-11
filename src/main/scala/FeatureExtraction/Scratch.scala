package FeatureExtraction

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object Scratch {
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
    val conf = new SparkConf().setAppName("Scratch").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Scratch")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    val file_1_path = "in/20200518_v3_other/ut_scratch_201911_v3.csv"

    val file_2_path = "in/20200518_v3_other/ut_scratch_201912_v3.csv"

    val file_3_path = "in/20200518_v3_other/ut_scratch_202001_v3.csv"

    val df1 = spark.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(file_1_path)
      .toDF("ISDN","TOTAL_SCRATCH_03","TOTAL_SCRATCH_VALUE_03","DATE_ID_03")
      .drop("DATE_ID_03").persist()

    val df2 = spark.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(file_2_path)
      .toDF("ISDN","TOTAL_SCRATCH_02","TOTAL_SCRATCH_VALUE_02","DATE_ID_02")
      .drop("DATE_ID_02").persist()

    val df3 = spark.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(file_3_path)
      .toDF("ISDN","TOTAL_SCRATCH_01","TOTAL_SCRATCH_VALUE_01","DATE_ID_01")
      .drop("DATE_ID_01").persist()

//        df1.printSchema()
//        df2.printSchema()

    val df = df1.join(df2,Seq("ISDN"), "outer").join(df3,Seq("ISDN"), "outer")
//    df.printSchema()
    val scratch_df = df.na.fill(0)
//    scratch_df.show(10)

    print(df1.count() + " - " + df2.count + " - " + df3.count + " - " + df.count())

    val divisionUDF = udf(division _)
    val isBiggerZeroUDF = udf(isBiggerZero _)
    val sumUDF = udf(sum)
    val averageUDF = udf(average _)

//    val scratch_extract = scratch_df
//      .withColumn("TOTAL_SCRATCH_VALUE/TOTAL_SCRATCH", divisionUDF(col("TOTAL_SCRATCH_VALUE"), col("TOTAL_SCRATCH")))
//      .withColumn("is_SCRATCH", isBiggerZeroUDF(col("TOTAL_SCRATCH")))
//
//      .withColumn("TOTAL_SCRATCH_SUM", sumUDF(col("TOTAL_SCRATCH_01"), col("TOTAL_SCRATCH")))
//      .withColumn("TOTAL_SCRATCH_VALUE_SUM", sumUDF(col("TOTAL_SCRATCH_VALUE_01"), col("TOTAL_SCRATCH_VALUE")))
//
//      .withColumn("TOTAL_SCRATCH_VALUE_SUM/TOTAL_SCRATCH_SUM", divisionUDF(col("TOTAL_SCRATCH_VALUE_SUM"), col("TOTAL_SCRATCH_SUM")))
//      .withColumn("is_SCRATCH_SUM", isBiggerZeroUDF(col("TOTAL_SCRATCH_SUM")))
//
//      .withColumn("TOTAL_SCRATCH_AVG", averageUDF(col("TOTAL_SCRATCH_01"), col("TOTAL_SCRATCH")))
//      .withColumn("TOTAL_SCRATCH_VALUE_AVG", averageUDF(col("TOTAL_SCRATCH_VALUE_01"), col("TOTAL_SCRATCH_VALUE")))
//
//      .drop("TOTAL_SCRATCH_01", "TOTAL_SCRATCH_VALUE_01")

//    scratch_extract.select("*").filter(col("is_SCRATCH_SUM") === 0).show(10)

//    scratch_extract.printSchema()

//    scratch_extract.show(10)

    val saveFile_dir = "out/0806/scratch_04"
    val saveFile_name =  "scratch_04_other.csv"

    saveCSV(scratch_df, saveFile_dir, saveFile_name, sc)

  }
}
