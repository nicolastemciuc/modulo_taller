package org.apache.spark.examples.mllib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.storage.StorageLevel

object SVMWithSGDExample {
  // args: [inputPath] [format=auto|libsvm|csv] [numIterations] [stepSize] [miniBatchFrac] [repartitions] [delimiter]
  def main(args: Array[String]): Unit = {
    val inputPath     = if (args.length >= 1) args(0) else "/mnt/datasets/criteo/test.txt"
    val fmtArg        = if (args.length >= 2) args(1) else "auto"
    val numIterations = if (args.length >= 3) args(2).toInt else 5000
    val stepSize      = if (args.length >= 4) args(3).toDouble else 0.1
    val miniBatchFrac = if (args.length >= 5) args(4).toDouble else 1.0
    val repartitions  = if (args.length >= 6) args(5).toInt else 0
    val delimiter     = if (args.length >= 7) args(6) else ","  // usa "\t" para TSV

    val conf = new SparkConf().setAppName("SVMWithSGDExample")
    val sc   = new SparkContext(conf)

    def detectFormat(): String = {
      if (fmtArg != "auto") return fmtArg
      val sample = sc.textFile(inputPath, 1).filter(_.trim.nonEmpty).take(5).mkString("\n")
      // Heurística simple: LibSVM suele tener "label index:value"
      if (sample.contains(":")) "libsvm" else "csv"
    }

    val format = detectFormat()
    val raw = format match {
      case "libsvm" =>
        MLUtils.loadLibSVMFile(sc, inputPath)
      case "csv" =>
        val rdd = sc.textFile(inputPath)
          .map(_.trim).filter(_.nonEmpty)
          .map { line =>
            val parts = line.split(delimiter).map(_.trim)
            // label en la 1ra columna, resto features
            val label = parts.head.toDouble
            val feats = parts.tail.map(_.toDouble)
            LabeledPoint(label, Vectors.dense(feats))
          }
        rdd
      case other =>
        throw new IllegalArgumentException(s"Unsupported format: $other")
    }

    val data = if (repartitions > 0) raw.repartition(repartitions) else raw
    data.persist(StorageLevel.MEMORY_AND_DISK)

    val algo = new SVMWithSGD()
    algo.setIntercept(true)
    algo.optimizer
      .setNumIterations(numIterations)
      .setStepSize(stepSize)
      .setMiniBatchFraction(miniBatchFrac)

    val model = algo.run(data)
    model.clearThreshold()

    val scoreAndLabels = data.map(p => (model.predict(p.features), p.label))
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC   = metrics.areaUnderROC()
    val auPR    = metrics.areaUnderPR()

    println(s"[SVM] Path=$inputPath format=$format iters=$numIterations step=$stepSize mb=$miniBatchFrac")
    println(s"[SVM] areaUnderROC=$auROC areaUnderPR=$auPR")

    sc.stop()
  }
}
