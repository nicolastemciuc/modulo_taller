// scalastyle:off println
package org.apache.spark.examples.mllib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

object SVMWithSGDExample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SVMWithSGDExample")
    val sc   = new SparkContext(conf)

    // === Adjust these if needed ===
    val csvPath    = sys.env.getOrElse("CSV_PATH", "file:///mnt/datasets/random_submission/random_submission.csv")
    val hasHeader  = true
    val delimiter  = ","        // String, not Char, so split(delimiter, -1) compiles on 2.12
    val labelIndex = 0          // change if label is in another column
    // ==============================

    val lines = sc.textFile(csvPath)

    val dataNoHeader =
      if (hasHeader) {
        val header = lines.first()
        lines.filter(_ != header)
      } else lines

    def toDoubleSafe(s: String): Double =
      try s.toDouble catch { case _: Throwable => 0.0 }

    val labeled = dataNoHeader.flatMap { line =>
      val parts = line.split(delimiter, -1).map(_.trim)
      if (parts.length <= labelIndex) None
      else {
        val labelStr = parts(labelIndex)
        val labelOpt =
          try Some(labelStr.toDouble)
          catch { case _: Throwable => None }

        labelOpt.map { label =>
          val feats = parts.indices.collect {
            case i if i != labelIndex => toDoubleSafe(parts(i))
          }.toArray
          LabeledPoint(label, Vectors.dense(feats))
        }
      }
    }.cache()

    // Train/test split (avoid pattern binding to keep type inference simple)
    val splits   = labeled.randomSplit(Array(0.8, 0.2), seed = 11L)
    val training = splits(0).cache()
    val test     = splits(1)

    val numIterations = 270000
    val stepSize      = 10.0
    val regParam      = 0.01
    val model         = MySVMWithSGD.train(training, numIterations, stepSize, regParam)

    // Use raw margins for ROC
    model.clearThreshold()

    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC   = metrics.areaUnderROC()
    println(s"Area under ROC = $auROC")

    sc.stop()
  }
}
