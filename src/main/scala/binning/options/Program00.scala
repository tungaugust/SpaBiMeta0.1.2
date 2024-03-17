package binning.options

import binning.tools._
import binning.tools.Utils._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object Program00 {
	def run(startProgramTime: Long,data: Array[String],params: InputParams,sc: SparkContext,spark: SparkSession): Unit = {
		val parallelism = Utils.getPartitions(spark)
		IOProcess.setOutBlocksNum((parallelism/(TASKS_PER_CORE*2.0)).ceil.toInt)
		Utils.setShufflePartitions(Utils.computeCurrentPartitions(Utils.getFileSizeInBytes(params.getInputFilePath), 0.8, sc, spark), spark)
		Utils.setDefaultMaxPartitonBytes(spark)

		//**********//
		Logging.logInfo("===== Extracting Phase")
		val dataRDD = sc.parallelize(data, parallelism)
		val sequenceRDD: RDD[SequenceType] = dataRDD.map(Sequences.parseRawSequence)

		val _fullReadRDD  = sequenceRDD.map(seq => seq._1 -> Array(seq._2 -> (seq._3,Kmers.seqBaseToByte(seq._4),seq._5)))
		val fullReadRDD: RDD[FullReadType] = _fullReadRDD.reduceByKey(_ ++ _).zipWithIndex
			.map(Sequences.mergeSequences)
			.persist(StorageLevel.MEMORY_AND_DISK_SER)
		val readRDD: RDD[ReadType] = fullReadRDD.map(read => (read._1,read._3,read._4))
			.persist(StorageLevel.MEMORY_AND_DISK_SER)
		if (params.isVerbose) {
			Logging.logInfo(s"Number of reads: ${readRDD.count}")
			val timeLine = Utils.timePrintout(startProgramTime)
			Logging.logInfo(s"Pre-processing reads has been completed: $timeLine")
		}
		val bcMinKmersNum = sc.broadcast(MIN_KMERS_NUM)
		val bcKmerSize = sc.broadcast(params.getKmerSize)
		val _kmerRDD = readRDD.flatMap(read => Kmers.sliceSequences(read, bcKmerSize.value))

		val kmerRDD =	_kmerRDD.reduceByKey(_ ++ _).filter(_._2.size >= bcMinKmersNum.value)
		if (params.isVerbose) {
			kmerRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
			Logging.logInfo(s"Number of distinct k-mers: ${kmerRDD.count}")
			val timeLine = Utils.timePrintout(startProgramTime)
			Logging.logInfo(s"Extracting k-mers has been completed: $timeLine")
		}

		Logging.logInfo("===== Grouping Phase")
		Logging.logInfo("Combinating read pairs ...")
		val edgeRDD = OverlapGraph.generateDFEdges(kmerRDD, params, spark)
		IOProcess.saveRDDEdgesFile(edgeRDD, params, spark)
		if (params.isVerbose) {
			kmerRDD.unpersist(false)
			val timeLine = Utils.timePrintout(startProgramTime)
			Logging.logInfo(s"Combinating read pairs has been completed: $timeLine")
		}
		bcMinKmersNum.destroy()
		bcKmerSize.destroy()

		Logging.logInfo("Building overlap graph ...")
		val componentRDD = OverlapGraph.runGraphFrameFromFile(readRDD,params,spark)
		Logging.logInfo("Grouping reads ...")
		val groupRDD = Groups.grouping(componentRDD, params, sc).persist(StorageLevel.MEMORY_AND_DISK_SER)
		val groupVectorRDD = Groups.computeFrequencyVector(groupRDD, params, sc)
		IOProcess.saveRDDVectorsFile(groupVectorRDD, params, spark)
		if (params.isVerbose) {
			val timeLine = Utils.timePrintout(startProgramTime)
			Logging.logInfo(s"Grouping Phase has been completed: $timeLine")
		}

		Logging.logInfo("===== Clustering Phase")
		Logging.logInfo("Clustering groups and Labeling reads ...")
		val clusterRDD = Clusters.clusteringMlFromFile(params, spark)
		val labelRDD = Clusters.labelingRDD(clusterRDD,groupRDD,readRDD,sc)
		groupRDD.unpersist(false)
		readRDD.unpersist(false)

		//**********//
		Logging.logInfo(s"Results are written in directory: ${params.getOutputDirPath(false)}")
		IOProcess.saveResults(fullReadRDD, labelRDD, params, spark)
		Logging.logInfo(s"===== Runtime: ${Utils.timePrintout(startProgramTime)}")

		val evaluation = Metrics.evaluation(labelRDD,params,spark,sc,false)
		val metrics = s"\n\tPrecision = ${evaluation.precision}\n\tRecall = ${evaluation.recall}\n\tF-measure = ${evaluation.fMeasure}"
		Logging.logInfo(s"===== Performance Metrics: $metrics")
	}
}