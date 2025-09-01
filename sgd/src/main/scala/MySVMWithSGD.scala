package org.apache.spark.examples.mllib

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.impl.GLMClassificationModel
import org.apache.spark.mllib.classification.ClassificationModel
import org.apache.spark.mllib.linalg.{BLAS, Vector}
import org.apache.spark.mllib.optimization._
import org.apache.spark.mllib.pmml.PMMLExportable
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.util.{DataValidators, Loader, Saveable}
import org.apache.spark.rdd.RDD

class MySVMModel (
  override val weights: Vector,
  override val intercept: Double)
  extends GeneralizedLinearModel(weights, intercept) with ClassificationModel with Serializable
  with PMMLExportable {

  private var threshold: Option[Double] = Some(0.0)

  def setThreshold(threshold: Double): this.type = {
    this.threshold = Some(threshold)
    this
  }

  def getThreshold: Option[Double] = threshold

  def clearThreshold(): this.type = {
    threshold = None
    this
  }

  override protected def predictPoint(
      dataMatrix: Vector,
      weightMatrix: Vector,
      intercept: Double) = {
    val margin = BLAS.dot(weightMatrix, dataMatrix) + intercept
    threshold match {
      case Some(t) => if (margin > t) 1.0 else 0.0
      case None => margin
    }
  }

  override def toString: String = {
    s"${super.toString}, numClasses = 2, threshold = ${threshold.getOrElse("None")}"
  }
}

class MySVMWithSGD private (
    private var stepSize: Double,
    private var numIterations: Int,
    private var regParam: Double,
    private var miniBatchFraction: Double)
  extends GeneralizedLinearAlgorithm[MySVMModel] with Serializable {

  private val gradient = new HingeGradient()
  private val updater = new SquaredL2Updater()
  override val optimizer = new GradientDescent(gradient, updater)
    .setStepSize(stepSize)
    .setNumIterations(numIterations)
    .setRegParam(regParam)
    .setMiniBatchFraction(miniBatchFraction)
    .setConvergenceTol(0.000000000001)
  override protected val validators = List(DataValidators.binaryLabelValidator)

  def this() = this(1.0, 100, 0.01, 1.0)

  override protected def createModel(weights: Vector, intercept: Double) = {
    new MySVMModel(weights, intercept)
  }
}

object MySVMWithSGD {

  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      regParam: Double,
      miniBatchFraction: Double,
      initialWeights: Vector): MySVMModel = {
    new MySVMWithSGD(stepSize, numIterations, regParam, miniBatchFraction)
      .run(input, initialWeights)
  }

  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      regParam: Double,
      miniBatchFraction: Double): MySVMModel = {
    new MySVMWithSGD(stepSize, numIterations, regParam, miniBatchFraction).run(input)
  }

  def train(
      input: RDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      regParam: Double): MySVMModel = {
    train(input, numIterations, stepSize, regParam, 1.0)
  }

  def train(input: RDD[LabeledPoint], numIterations: Int): MySVMModel = {
    train(input, numIterations, 1.0, 0.01, 1.0)
  }
}
