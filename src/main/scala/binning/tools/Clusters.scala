package binning.tools

import Utils._
import Groups._

import org.apache.spark.mllib.clustering.{KMeans => KMeansMllib, KMeansModel}
import org.apache.spark.mllib.linalg.{Vectors => VectorMllib}
import org.apache.spark.ml.linalg.{Vectors => VectorMl}
import org.apache.spark.ml.clustering.{KMeans => KMeansMl}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.broadcast.Broadcast

object Clusters {
	private val MAX_ITERATIONS: Int = 100

	def clusteringMllib(vectorRDD: RDD[(GroupId,DenseVectorType)],params: InputParams): RDD[(GroupId,Int)] = {
		val pointRDD = vectorRDD.map(vector => vector._1 -> VectorMllib.dense(vector._2))
			.persist(StorageLevel.MEMORY_AND_DISK_SER)
		val numClusters = params.getSpeciesNum
		val numIterations = MAX_ITERATIONS
		val model = KMeansMllib.train(pointRDD.map(_._2),numClusters,numIterations)
		// Make predictions
		val predictRDD = model.predict(pointRDD.map(_._2))
		val labeledClusterRDD = pointRDD.map(_._1).zip(predictRDD).map(cluster => (cluster._1,cluster._2))
		pointRDD.unpersist(false)
		labeledClusterRDD
	}
	def clusteringMl(vectorRDD: RDD[(GroupId,DenseVectorType)],params: InputParams,spark: SparkSession): RDD[(GroupId,Int)] ={
		import spark.implicits._
		val pointRDD = vectorRDD.map(vector => (vector._1,VectorMl.dense(vector._2)))
		val pointDF = spark.createDataFrame(pointRDD).toDF("groupId","features").persist(StorageLevel.MEMORY_AND_DISK_SER)
		val numClusters = params.getSpeciesNum
		val numIterations = MAX_ITERATIONS
		val kmeans = new KMeansMl().setK(numClusters).setMaxIter(numIterations)
		val model = kmeans.fit(pointDF)
		val predictions = model.transform(pointDF) // prediction
		pointDF.unpersist(false)
		predictions.rdd.map(row => (row.getAs[String]("groupId"), row.getAs[Int]("prediction")))
	}

	def clusteringMlFromFile(params: InputParams,spark: SparkSession): RDD[(GroupId,Int)] ={
		import spark.implicits._
		val vectorRDD = IOProcess.loadVectorsFile(params, spark)
		val pointRDD = vectorRDD.map(vector => (vector._1,VectorMl.dense(vector._2)))
		val pointDF = spark.createDataFrame(pointRDD).toDF("groupId","features").persist(StorageLevel.MEMORY_AND_DISK_SER)
		val numClusters = params.getSpeciesNum
		val numIterations = MAX_ITERATIONS
		val kmeans = new KMeansMl().setK(numClusters).setMaxIter(numIterations)
		val model = kmeans.fit(pointDF)
		val predictions = model.transform(pointDF) // prediction
		val res = predictions.rdd.map(row => (row.getAs[String]("groupId"), row.getAs[Int]("prediction")))
		pointDF.unpersist(false)
		res
	}

	def labelingRDD(clusterRDD: RDD[(GroupId,Int)], groupRDD: RDD[GroupClass], readRDD: RDD[ReadType],
									sc: SparkContext): RDD[(ReadIndex, String, Int)] = {
		val bcDiscardLabel = sc.broadcast(DISCARD_LABEL)
		val left = readRDD.map(read => (read._1,read._2)) // label
		val right = labelClusterIdForRead(clusterRDD,groupRDD,bcDiscardLabel) // predict
		val labeled = mergeLabel(left,right,bcDiscardLabel)
		labeled
	}
	private def labelClusterIdForRead(clusters: RDD[(GroupId,Int)], groups: RDD[GroupClass],
																		discardLabel: Broadcast[Int]): RDD[(ReadIndex,Int)] = {
		val labeledGroups = groups.flatMap(labelGroupIdxForRead).distinct
		val labeledClusters = clusters.map(cluster=>(cluster._1,cluster._2))
		val labeledReads = labeledGroups.leftOuterJoin(labeledClusters).map(predictedRead => {
			val (groupId, prop) = predictedRead
			val readId = prop._1
			val clusterId: Int = prop._2.getOrElse(discardLabel.value)
			(readId,clusterId)
		})
		labeledReads
	}
	private def labelGroupIdxForRead(group: GroupClass): Array[(GroupId,ReadIndex)] = {
		val id = group.id
		val seed = group.seed.map(read => (id,read._1))
		val nonSeed = group.nonSeed.map(read =>(id,read))
		val res = seed ++ nonSeed
		res.toArray
	}
	private def mergeLabel(left: RDD[(ReadIndex,String)], right: RDD[(ReadIndex,Int)],
												 discardLabel: Broadcast[Int]): RDD[(ReadIndex,String,Int)] = {
		val res = left.leftOuterJoin(right).map(read => {
			val (id, prop) = read
			val label = prop._1
			val predict = prop._2.getOrElse(discardLabel.value)
			(id,label,predict)
		})
		res
	}
}
