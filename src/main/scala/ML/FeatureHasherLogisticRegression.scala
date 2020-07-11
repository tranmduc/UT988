package ML

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.FeatureHasher
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType

object FeatureHasherLogisticRegression {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("FeatureHasherLR")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    val df = spark.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("in/ut_sample_20200312.txt")
      .toDF("ISDN","AMOUNT","MONTH_ACTIVE","AMOUNT_TOPUP_AVG_MONTH","AMOUNT_TOPUP_MONTH_CHECK","ARPU_EACH_MONTH","ARPU_AVG","STATUS")


    val data = df.withColumn("label", df("STATUS").cast(DoubleType))
      .drop("STATUS")

    val seed = 11L

    val Array(training, test) = data.randomSplit(Array(0.7, 0.3), seed)
    training.cache()

    //Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    val hasher = new FeatureHasher()
      .setInputCols("AMOUNT","MONTH_ACTIVE","AMOUNT_TOPUP_AVG_MONTH","AMOUNT_TOPUP_MONTH_CHECK","ARPU_EACH_MONTH","ARPU_AVG")
      .setOutputCol("features")

    val lr = new LogisticRegression()
      .setMaxIter(10)

    val pipeline = new Pipeline()
      .setStages(Array(hasher, lr))

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
    val paramGrid = new ParamGridBuilder()
      .addGrid(hasher.numFeatures, Array(10, 100, 1000))
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .build()

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)  // Use 3+ in practice
      .setParallelism(3)  // Evaluate up to 2 parameter settings in parallel

    // Run cross-validation, and choose the best set of parameters.
    val model = cv.fit(training)

    // Now we can optionally save the fitted pipeline to disk
    //model.write.overwrite().save("/tmp/spark-logistic-regression-model")

    // We can also save this unfit pipeline to disk
    //pipeline.write.overwrite().save("/tmp/unfit-lr-model")

    // And load it back in during production
    //val sameModel = PipelineModel.load("/tmp/spark-logistic-regression-model")

    val results = model.transform(test)

    val predictions = results.select("prediction").rdd.map(_.getDouble(0))
    val labels = results.select("label").rdd.map(_.getDouble(0))

    val mMetrics = new MulticlassMetrics(predictions.zip(labels))
    val bMetrics = new BinaryClassificationMetrics(predictions.zip(labels))

    // Confusion matrix
    println("Confusion matrix:")
    println(mMetrics.confusionMatrix)

    // Overall Statistics
    val accuracy = mMetrics.accuracy
    println("Summary Statistics")
    println(s"Accuracy = $accuracy")

    // Precision by label
    val label = mMetrics.labels
    label.foreach { l =>
      println(s"Precision($l) = " + mMetrics.precision(l))
    }

    // Recall by label
    label.foreach { l =>
      println(s"Recall($l) = " + mMetrics.recall(l))
    }

    // False positive rate by label
    label.foreach { l =>
      println(s"FPR($l) = " + mMetrics.falsePositiveRate(l))
    }

    // F-measure by label
    label.foreach { l =>
      println(s"F1-Score($l) = " + mMetrics.fMeasure(l))
    }

    // AUPRC
    val auPRC = bMetrics.areaUnderPR
    println("Area under precision-recall curve = " + auPRC)

    // ROC Curve
    val roc = bMetrics.roc

    // AUROC
    val auROC = bMetrics.areaUnderROC
    println("Area under ROC = " + auROC)
  }
}
