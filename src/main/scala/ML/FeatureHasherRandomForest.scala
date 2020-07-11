package ML

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.FeatureHasher
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType

object FeatureHasherRandomForest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("FeatureHasherRF")
      .config("spark.sql.warehouse.dir", ".")
      .getOrCreate()

    val df = spark.read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("in/ut_fillna.csv")
      .toDF("ISDN","SEX","SUB_AGE","AMOUNT","MONTH_ACTIVE","AMOUNT_TOPUP_AVG_MONTH","AMOUNT_TOPUP_MONTH_CHECK","ARPU_EACH_MONTH","ARPU_AVG","STATUS")

    val data = df.withColumn("label", df("STATUS").cast(DoubleType))
      .drop("STATUS")

    val seed = 5043

    val Array(training, test) = data.randomSplit(Array(0.7, 0.3), seed)
    training.cache()

    //Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    val hasher = new FeatureHasher()
      .setNumFeatures(100)
      .setInputCols("SEX","SUB_AGE","AMOUNT","MONTH_ACTIVE","AMOUNT_TOPUP_AVG_MONTH","AMOUNT_TOPUP_MONTH_CHECK","ARPU_EACH_MONTH","ARPU_AVG")
      .setOutputCol("features")

    val rf = new RandomForestClassifier()
      //.setNumTrees(1000)
      .setFeatureSubsetStrategy("auto")
      .setSeed(seed)

    val pipeline = new Pipeline()
      .setStages(Array(hasher, rf))

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    val paramGrid = new ParamGridBuilder()
      //.addGrid(hasher.numFeatures, Array(10, 100, 1000))
      //.addGrid(rf.maxBins, Array(25, 28, 31))
      .addGrid(rf.numTrees, Array(1000, 1200, 1400, 1600, 1800, 2000))
      .addGrid(rf.maxDepth, Array(10, 20, 30, 40, 50))
      .addGrid(rf.impurity, Array("entropy", "gini"))
      .build()

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)  // Use 3+ in practice
      .setParallelism(3)

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

     //Confusion matrix
    println("Confusion matrix:")
    println(mMetrics.confusionMatrix)

     //Overall Statistics
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
