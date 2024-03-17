package binning.tools

import Utils._
import IOProcess._

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.graphx._
import org.graphframes.GraphFrame
import org.graphframes.GraphFrame._

object OverlapGraph {
	case class NodeClass(val id: ReadIndex, val minCCId: ReadIndex,
											 val seqs: Array[DNASeq], val neighbors: Array[ReadIndex],
											 private var visited: Boolean = false
											){
		def setVisited(value: Boolean): Boolean = { this.visited = value; this.visited }
		def isVisited: Boolean = this.visited
		override def toString: String = s"[Node]:id<$id>-cc<$minCCId>-visit<$visited>-nbrs[${neighbors.length}]-seqs[${seqs.length}]"
	}

	// Combinating read pairs
	def generateRDDEdges(kmerRDD: RDD[(KmerHashCode, Array[ReadIndex])],params: InputParams,
										spark: SparkSession,sc: SparkContext): RDD[((ReadIndex, ReadIndex), Int)] = {
		val bcThreshold = sc.broadcast(params.getThreshold)
		val edgeRDD = kmerRDD.flatMap(kmer => Utils.getCanonicalCombinations(kmer._2))
			.map(pair => (pair, 1)).reduceByKey(_+_)
			.filter(_._2 >= bcThreshold.value)
		edgeRDD
	}
	def generateDFEdges(kmerRDD: RDD[(KmerHashCode, Array[ReadIndex])],
											params: InputParams,spark: SparkSession): RDD[((ReadIndex, ReadIndex), Int)] = {
		import spark.implicits._
		val adjacencyList = "adjList"
		val kmerDF = spark.createDataFrame(kmerRDD).toDF("kmer",adjacencyList)
		val combinationExpr = s"filter(transform(flatten(transform($adjacencyList" +
			s", x -> arrays_zip(array_repeat(x,size($adjacencyList)),$adjacencyList)))" +
			s", x -> array(x['0'],x['$adjacencyList']))" +
			s", x -> x[0] < x[1])"
		val pairDF= kmerDF.select(expr(combinationExpr).as("pairs")).where(size(col("pairs"))=!=0)
		val weightedPairs = pairDF.select(explode(col("pairs")).as("pair")).groupBy("pair").count()
		val weightedEdges = weightedPairs.filter(col("count") >= params.getThreshold)

		val edgeRDD = weightedEdges.rdd.map(row => {
			val pair = row.getSeq[Long](0)
			val weight = row.getLong(1).toInt
			((pair(0),pair(1)), weight)
		})
		edgeRDD
	}

	// GraphFrame
	def runGraphFrame(readRDD: RDD[ReadType],pairRDD: RDD[((ReadIndex,ReadIndex),Int)],params: InputParams,
										spark: SparkSession): RDD[(ReadIndex,Map[ReadIndex,NodeClass])] = {
		import spark.implicits._
		val edges = pairRDD.map(_._1).toDF("src","dst")
		val vertices = readRDD.map(_._1).toDF("id")
		val graphFrame = GraphFrame.apply(vertices,edges)
		if (params.isVerbose) {
			graphFrame.persist(StorageLevel.MEMORY_AND_DISK_SER)
			Logging.logInfo(s"Number of vertices: ${graphFrame.vertices.count}")
			Logging.logInfo(s"Number of edges: ${graphFrame.edges.count}")
		}
		val ccDF = graphFrame.connectedComponents.run()
		if (params.isVerbose) { graphFrame.unpersist(false) }
		val ccRDD = ccDF.rdd.map(row => row.getAs[ReadIndex]("id") ->	row.getAs[ReadIndex]("component"))
		val ccSeqs = readRDD.map(read => (read._1,read._3)).leftOuterJoin(ccRDD).map(read => {
			val (id, prop) = read
			val seqs = prop._1
			val minCCId = prop._2.getOrElse(id)
			(id, (minCCId,seqs))
		})
		val nbrs = collectNeighborsRDD(pairRDD.map(_._1))
		val res = mergeRDD2RDD(ccSeqs,nbrs)
		res
	}
	def runGraphFrameFromFile(readRDD: RDD[ReadType],params: InputParams,spark: SparkSession): RDD[(ReadIndex,Map[ReadIndex,NodeClass])] = {
		import spark.implicits._
		val edges = IOProcess.loadEdgesFile(params,spark).persist(StorageLevel.MEMORY_AND_DISK_SER)
		val vertices = readRDD.map(_._1).toDF("id")
		val graphFrame = GraphFrame.apply(vertices,edges).dropIsolatedVertices()
		if (params.isVerbose) {
			graphFrame.persist(StorageLevel.MEMORY_AND_DISK_SER)
			Logging.logInfo(s"Number of vertices: ${graphFrame.vertices.count}")
			Logging.logInfo(s"Number of edges: ${graphFrame.edges.count}")
		}
		val ccDF = graphFrame.connectedComponents.run()
		if (params.isVerbose) { graphFrame.unpersist(false) }
		val ccRDD = ccDF.rdd.map(row => (
			row.getAs[ReadIndex]("id"),
			row.getAs[ReadIndex]("component")
		))
		val ccSeqs = readRDD.map(read => (read._1,read._3)).leftOuterJoin(ccRDD).map(read => {
			val (id, prop) = read
			val seqs = prop._1
			val minCCId = prop._2.getOrElse(id)
			(id, (minCCId,seqs))
		})
		val pairRDD = edges.rdd.map(row => (
			row.getAs[ReadIndex]("src"),
			row.getAs[ReadIndex]("dst")
		))
		val nbrs = collectNeighborsRDD(pairRDD)
		val res = mergeRDD2RDD(ccSeqs,nbrs)
		edges.unpersist(false)
		res
	}

	private def mergeRDD2RDD(ccRDD: RDD[(ReadIndex,(ReadIndex,Array[DNASeq]))],
													 nbrsRDD: RDD[(ReadIndex,Array[ReadIndex])]): RDD[(ReadIndex,Map[ReadIndex,NodeClass])] = {
		val res = ccRDD.leftOuterJoin(nbrsRDD).map(vertex => {
			val (id, prop) = vertex
			val minCCId = prop._1._1
			val seqs = prop._1._2
			val nbrs = prop._2.getOrElse(Array())
			(minCCId, Map(id -> NodeClass(id,minCCId,seqs,nbrs,false)))
		}).reduceByKey(_++_)
		res
	}
	private def collectNeighborsRDD(pairRDD: RDD[(ReadIndex,ReadIndex)]): RDD[(ReadIndex,Array[ReadIndex])] = {
		val graphx = Graph.fromEdgeTuples(pairRDD,1)
		val nbrs = graphx.collectNeighborIds(EdgeDirection.Either).map(v=>v)
		nbrs
	}
}
