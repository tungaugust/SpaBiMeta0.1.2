package binning.tools

import Utils._
import Clusters._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object Metrics {
	private val confusionMatrixCol = "predict_label"
	case class EvaluationMetrics(val precision: Double,val recall: Double,val fMeasure: Double) {
		override def toString: String = s"[Evaluation Metrics]:[Precision=${this.precision} Recall=${this.recall} F-measure=${this.fMeasure}]"
	}
	def evaluation(labelRDD: RDD[(ReadIndex,String,Int)], params: InputParams,
								 spark: SparkSession, sc: SparkContext, usingSQL: Boolean = true): EvaluationMetrics = {
		val res = {
			if (usingSQL)
				evaluateMetricsUsingSQL(labelRDD, params, spark)
			else
				evaluateMetricsUsingRDD(labelRDD, params, sc)
		}
		res
	}

	private def evaluateMetricsUsingRDD(labelRDD: RDD[(ReadIndex,String,Int)], params: InputParams,
																			sc: SparkContext): EvaluationMetrics = {
		val labelPredictRDD = labelRDD.map(read => ((read._2,read._3), 1)).reduceByKey(_+_)
		labelPredictRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
		//labelPredictRDD.foreach(e=>println(s"\t${e._1._1}\t${e._1._2}:\t${e._2}"))
		val sumFull: Long = labelPredictRDD.map(_._2).treeReduce(_+_).toLong
		var sumMaxLabel: Long = 0L
		var sumMaxPredict: Long = 0L
		var precision: Double = 0.0
		var recall: Double = 0.0
		if (params.isDiscard) {
			val bcDiscardLabel = sc.broadcast(DISCARD_LABEL)
			val predictRDD = labelPredictRDD.filter(elems => elems._1._2 != bcDiscardLabel.value)
			predictRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
			val sumPredict: Long = predictRDD.map(_._2).treeReduce(_+_).toLong
			sumMaxLabel = predictRDD.map(elems => (elems._1._1,elems._2)).reduceByKey(_ max _).map(_._2).treeReduce(_+_).toLong
			sumMaxPredict = predictRDD.map(elems => (elems._1._2,elems._2)).reduceByKey(_ max _).map(_._2).treeReduce(_+_).toLong
			predictRDD.unpersist(false)
			precision = sumMaxLabel.toDouble / sumPredict
			recall = sumMaxPredict.toDouble / sumFull
		} else {
			sumMaxLabel = labelPredictRDD.map(elems => (elems._1._1,elems._2)).reduceByKey(_ max _).map(_._2).treeReduce(_+_).toLong
			sumMaxPredict = labelPredictRDD.map(elems => (elems._1._2,elems._2)).reduceByKey(_ max _).map(_._2).treeReduce(_+_).toLong
			precision = sumMaxLabel.toDouble / sumFull
			recall = sumMaxPredict.toDouble / sumFull
		}
		labelPredictRDD.unpersist(false)
		val fMeasure: Double = 2.0 / (1.0/precision + 1.0/recall)
		EvaluationMetrics(precision,recall,fMeasure)
	}

	private def evaluateMetricsUsingSQL(labelRDD: RDD[(ReadIndex,String,Int)], params: InputParams,
																			spark: SparkSession): EvaluationMetrics = {
		import spark.implicits._
		val confusionMatrix = buildConfusionMatrix(labelRDD,spark) // row: predict _ col: label
		confusionMatrix.persist(StorageLevel.MEMORY_AND_DISK_SER)
		//confusionMatrix.show()
		val sumFull: Long = labelRDD.count.toLong
		var sumMaxLabel: Long = 0L
		var sumMaxPredict: Long = 0L
		var precision: Double = 0.0
		var recall: Double = 0.0
		if (params.isDiscard) {
			val predictDF = confusionMatrix.where(col(confusionMatrixCol) =!= DISCARD_LABEL)
			predictDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
			val sumPredict: Long = sumFull - sumNonPredict(confusionMatrix)
			sumMaxLabel = sumMaxRow(predictDF)
			sumMaxPredict = sumMaxCol(predictDF)
			predictDF.unpersist(false)
			precision = sumMaxLabel.toDouble / sumPredict
			recall = sumMaxPredict.toDouble / sumFull
		} else {
			sumMaxLabel = sumMaxRow(confusionMatrix)
			sumMaxPredict = sumMaxCol(confusionMatrix)
			precision = sumMaxLabel.toDouble / sumFull
			recall = sumMaxPredict.toDouble / sumFull
		}
		confusionMatrix.unpersist(false)
		val fMeasure: Double = 2.0 / (1.0/precision + 1.0/recall)
		EvaluationMetrics(precision,recall,fMeasure)
	}
	private def buildConfusionMatrix(labelRDD: RDD[(ReadIndex,String,Int)], spark: SparkSession): DataFrame = {

		val labelPredictDF = spark.createDataFrame(labelRDD).toDF("id","label","predict")
		val confusionMatrixDF = labelPredictDF.stat.crosstab("predict","label")
		confusionMatrixDF
	}
	private def sumNonPredict(conMatrix: DataFrame): Long = {
		val nonPredictDF = conMatrix.where(col(confusionMatrixCol) === DISCARD_LABEL)
		if (nonPredictDF.count==0) return 0L
		val columns = nonPredictDF.columns.tail
		val sumNonPredict = nonPredictDF.select(columns.map(cols=>col(cols)).reduce(_+_).as("Total")).first.getLong(0)
		return sumNonPredict
	}
	private def sumMaxRow(conMatrix: DataFrame): Long = {
		// find max
		val columns = conMatrix.columns.tail
		val maxRows = conMatrix.withColumn("maxOfRow",greatest(columns.head,columns.tail:_*))
		// sum max
		val sumMaxRows = maxRows.agg("maxOfRow" -> "sum").first().getLong(0)
		sumMaxRows
	}
	private def sumMaxCol(conMatrix: DataFrame): Long = {
		// find max
		val columns = conMatrix.columns.tail
		val exprMax = columns.map((_ -> "max")).toMap
		val maxCols = conMatrix.agg(exprMax)
		// sum max
		val columnsMax = maxCols.columns
		val sumMaxCols = maxCols.select(columnsMax.map(cols => col(cols)).reduce(_+_).as("Total")).first().getLong(0)
		sumMaxCols
	}
}