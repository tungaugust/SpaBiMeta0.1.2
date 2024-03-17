package binning.tools

import Utils._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.io.Source
import scala.collection.mutable.ArrayBuffer

import java.io._
import java.io.{BufferedWriter, File, FileWriter}

object IOProcess {
	private var OUT_BLOCKS_NUM: Int = 1
	def setOutBlocksNum(blocksNum: Int): Unit = {if (blocksNum != 0) OUT_BLOCKS_NUM = blocksNum}
	private def getOutBlocksNum: Int = OUT_BLOCKS_NUM

	private val KMER_FIELDNAME = "kmer"
	private val ADJ_FIELDNAME = "adjacency"
	private val SRC_FIELDNAME = "src"
	private val DST_FIELDNAME = "dst"
	private val GROUP_ID_FIELDNAME = "groupId"
	private val VECTOR_FIELDNAME = "features"

	private val SAVE_MODE = "overwrite"
	private val ELEMENT_DELIMITER = " "
	private val KV_DELIMITER = "="
	private val EDGES_SCHEMA = new StructType().add(SRC_FIELDNAME,LongType,false).add(DST_FIELDNAME,LongType,false)
	private val VECTORS_SCHEMA = new StructType().add(GROUP_ID_FIELDNAME,StringType,false).add(VECTOR_FIELDNAME,ArrayType(DoubleType),false)


	// Read DNA File
	def readRawFastaFile(path: String): Array[String] = Source.fromFile(path).mkString.split(">").tail.map(_.trim)
	def readRawLargeFastaFile(path: String): Array[String] = {
		val br = new BufferedReader(new FileReader(path))
		var line = ""
		val rawFile = new ArrayBuffer[String]()
		var readNum = 0
		while ({line = br.readLine; line != null}) {
			if (line.length > 0) {
				if (line.startsWith(">")) {
					rawFile += line.drop(1).trim
					readNum +=1
				} else {
					rawFile(readNum-1) += "\n".concat(line.trim)
				}
			}
		}
		br.close
		rawFile.toArray
	}

	// Save KmerHashMap File From RDD
	def saveRDDKmersFile(data: RDD[(KmerHashCode, Array[ReadIndex])],params: InputParams,spark: SparkSession): Unit = {
		import spark.implicits._
		try {
			params.getFormat match {
				case "parquet" => {
					val kmerRDD = spark.createDataFrame(data).toDF(KMER_FIELDNAME, ADJ_FIELDNAME)
					kmerRDD.coalesce(getOutBlocksNum)
						.write.mode(SAVE_MODE).format(params.getFormat)
						.save(params.getKmerDirPath(false))
				}
				case "text" => {
					val kmerToString = (kmer: (KmerHashCode, Array[ReadIndex])) => {
						kmer._1.concat(KV_DELIMITER) + kmer._2.mkString(ELEMENT_DELIMITER)
					}
					data.map(e => kmerToString(e))
						.coalesce(getOutBlocksNum,false)
						.saveAsTextFile(params.getKmerDirPath(false))
				}
			}
		} catch {
			case e: IllegalArgumentException => Utils.quitProgram(false,"# ERROR # Cannot save kmers files in temporary directory.")
		}
	}

	// Save Edges File From RDD
	def saveRDDEdgesFile(data: RDD[((ReadIndex,ReadIndex),Int)],params: InputParams,spark: SparkSession): Unit = {
		import spark.implicits._
		try {
			params.getFormat match {
				case "parquet" => {
					val edgeDF = data.map(_._1).toDF(SRC_FIELDNAME, DST_FIELDNAME)
					edgeDF.coalesce(getOutBlocksNum)
						.write.mode(SAVE_MODE).format(params.getFormat)
						.save(params.getEdgeDirPath(false))
				}
				case "text" => {
					val pairToString = (pair: (ReadIndex, ReadIndex)) => pair._1.toString + ELEMENT_DELIMITER + pair._2.toString
					data.map(e => pairToString(e._1))
						.coalesce(getOutBlocksNum,false)
						.saveAsTextFile(params.getEdgeDirPath(false))
				}
			}
		} catch {
			case e: IllegalArgumentException => Utils.quitProgram(false,"# ERROR # Cannot save edge files in temporary directory.")
		}
	}

	// Save Frequency Vector File From RDD
	def saveRDDVectorsFile(data: RDD[(GroupId, DenseVectorType)],params: InputParams,spark: SparkSession): Unit = {
		import spark.implicits._
		try {
			params.getFormat match {
				case "parquet" => {
					val dataDF = data.map(vector => Row(vector._1, vector._2.toList))
					val vectorDF = spark.createDataFrame(dataDF,VECTORS_SCHEMA)
					vectorDF.coalesce(getOutBlocksNum)
						.write.mode(SAVE_MODE).format(params.getFormat)
						.save(params.getVectorDirPath(false))
				}
				case "text" => {
					val vectorToString = (vector: (GroupId, DenseVectorType)) => {
						vector._1.concat(KV_DELIMITER) + vector._2.mkString(ELEMENT_DELIMITER)
					}
					data.map(e => vectorToString(e))
						.coalesce(getOutBlocksNum,false)
						.saveAsTextFile(params.getVectorDirPath(false))
				}
			}
		} catch {
			case e: IllegalArgumentException => Utils.quitProgram(false,"# ERROR # Cannot save vector files in temporary directory.")
		}
	}


	// Load Kmers File
	def loadKmersFile(params: InputParams, spark: SparkSession): RDD[(KmerHashCode,Array[ReadIndex])] = {
		try {
			val res = params.getFormat match {
				case "parquet" => {
					val kmerDF = spark.read.format(params.getFormat).load(params.getKmerFilesPath)
					val kmerRDD = kmerDF.rdd.map(row => (row.getString(0), row.getSeq[ReadIndex](1).toArray))
					kmerRDD
				}
				case "text" => {
					val kmerDF = spark.read.format("csv").option("delimiter",KV_DELIMITER).option("header","false").load(params.getKmerFilesPath)
					val kmerRDD = kmerDF.rdd.map(row => (row.getString(0), row.getString(1).trim.split(ELEMENT_DELIMITER).map(_.toLong)))
					kmerRDD
				}
				case _ => {
					Utils.quitProgram(false, "# ERROR # Cannot load kmer files in temporary directory.")
					spark.sparkContext.emptyRDD[(KmerHashCode,Array[ReadIndex])]
				}
			}
			return res
		} catch {
			case e: IllegalArgumentException => {
				Utils.quitProgram(false, "# ERROR # Cannot load kmer files in temporary directory.")
				spark.sparkContext.emptyRDD[(KmerHashCode,Array[ReadIndex])]
			}
		}
	}

	// Load Edges File
	def loadEdgesFile(params: InputParams, spark: SparkSession): DataFrame = {
//		import org.apache.spark.util.SizeEstimator
//		val maxPartitonBytes = (Math.pow(2, 20) * MAX_PARTITION_MEGABYTES).toInt
//		val readFileSize = SizeEstimator.estimate(data)
//		val openCostInBytes = spark.conf.getOption("spark.sql.files.openCostInBytes") match {
//			case Some(_str) => _str.toInt
//			case None => 0
//		}
//		val partitionBytesPerCore = ((initialPartitionNum*openCostInBytes*Math.pow(2, 20) + readFileSize)/initialPartitionNum).toInt + 1
//		println(s"${maxPartitonBytes}::${partitionBytesPerCore}")
//		spark.conf.set("spark.sql.files.maxPartitonBytes", Math.min(maxPartitonBytes, partitionBytesPerCore))

		try {
			val res = params.getFormat match {
				case "parquet" => spark.read.format(params.getFormat).load(params.getEdgeFilesPath)
				case "text" => {
					spark.read.format("csv").option("delimiter",ELEMENT_DELIMITER).option("header","false")
					.load(params.getEdgeFilesPath).select(
						col("_c0").cast("long").as("src"),
						col("_c1").cast("long").as("dst")
					)
				}
				case _ => {
					Utils.quitProgram(false, "# ERROR # Cannot load edge files in temporary directory.")
					spark.createDataFrame(spark.sparkContext.emptyRDD[Row],EDGES_SCHEMA)
				}
			}
			return res
		} catch {
			case e: IllegalArgumentException => {
				Utils.quitProgram(false, "# ERROR # Cannot load edge files in temporary directory.")
				spark.createDataFrame(spark.sparkContext.emptyRDD[Row],EDGES_SCHEMA)
			}
		}
	}

	// Load Vectors File
	def loadVectorsFile(params: InputParams, spark: SparkSession): RDD[(GroupId,DenseVectorType)] = {
		import spark.implicits._
		try {
			val res = params.getFormat match {
				case "parquet" => {
					val vectorDF = spark.read.format(params.getFormat).load(params.getVectorFilesPath)
					val vectorRDD = vectorDF.rdd.map(row => (row.getString(0), row.getSeq[Double](1).toArray))
					vectorRDD
				}
				case "text" => {
					val vectorDF = spark.read.format("csv").option("delimiter",KV_DELIMITER).option("header","false").load(params.getVectorFilesPath)
					val vectorRDD = vectorDF.rdd.map(row => (row.getString(0), row.getString(1).trim.split(ELEMENT_DELIMITER).map(_.toDouble)))
					vectorRDD
				}
				case _ => {
					Utils.quitProgram(false, "# ERROR # Cannot load vector files in temporary directory.")
					spark.sparkContext.emptyRDD[(GroupId,DenseVectorType)]
				}
			}
			return res
		} catch {
			case e: IllegalArgumentException => {
				Utils.quitProgram(false, "# ERROR # Cannot load vector files in temporary directory.")
				spark.sparkContext.emptyRDD[(GroupId,DenseVectorType)]
			}
		}
	}

	//
	def saveResultsDF(sequenceDF: DataFrame,labelRDD: RDD[(ReadIndex,String,Int)],params: InputParams,spark: SparkSession): Unit = {
		import spark.implicits._
		val dataDF = sequenceDF.select("id","raw")
			.groupBy("id").agg(collect_list(col("raw")).as("data"))
			.select(col("id"),concat(lit(">"),concat_ws("\n>",col("data"))).as("data"))
		val predictDF = spark.createDataFrame(labelRDD).toDF("id","label","predict").select("id","predict")
		val resultsDF = dataDF.join(predictDF,dataDF("id")===predictDF("id"),"left").select(dataDF("id"),dataDF("data"),predictDF("predict"))
		resultsDF.persist(StorageLevel.MEMORY_AND_DISK_SER)

		val infoPath = params.mkOutPath(INFO_PATH_NAME)
		val clusterPath = params.mkOutPath(CLUSTER_PATH_NAME)
		val binoutPath = params.mkOutPath(BINOUT_PATH_NAME)
//		val dirList = List(infoPath,clusterPath,binoutPath)
//		Utils.deleteDirectories(dirList)
		Logging.logInfo("Writing result files ...")

		//Logging.logInfo("Writing info file ...")
		saveInfoFile(resultsDF,infoPath,params)
		//Logging.logInfo("Writing binout file ...")
		saveBinoutFile(resultsDF,binoutPath)
		//Logging.logInfo("Writing clustering result file ...")
		saveClusterFiles(resultsDF,clusterPath)

		resultsDF.unpersist(false)
		sequenceDF.unpersist(false)
	}

	//
	private def mergeSeqsData(data: Array[String]): String = data.map(seq=>">".concat(seq)).mkString("\n")
	//private def mkOutPath(params: InputParams, msg: String): String = { params.getOutputDirPath(true)+params.getOutFileName+".".concat(msg)}

	def saveResults(fullReadType: RDD[FullReadType],labelRDD: RDD[(ReadIndex,String,Int)],
									params: InputParams, spark: SparkSession): Unit = {
		import spark.implicits._
		val dataRDD = fullReadType.map(read => read._1 -> (read._2,read._5))
		val predictRDD = labelRDD.map(read => read._1 -> read._3)
		val resultsRDD = dataRDD.join(predictRDD).map(read => {
			val (index, prop) = read
			val id = prop._1._1
			val data = mergeSeqsData(prop._1._2)
			val predict = prop._2
			val res = (id, data, predict)
			res
		})
		val resultsDF = spark.createDataFrame(resultsRDD).toDF("id","data","predict")
			.orderBy(length(col("id")).asc,col("id").asc)
			.persist(StorageLevel.MEMORY_AND_DISK_SER)

		val infoPath = params.mkOutPath(INFO_PATH_NAME)
		val clusterPath = params.mkOutPath(CLUSTER_PATH_NAME)
		val binoutPath = params.mkOutPath(BINOUT_PATH_NAME)
//		val dirList = List(infoPath,clusterPath,binoutPath)
//		Utils.deleteDirectories(dirList)
		Logging.logInfo("Writing result files ...")

		//Logging.logInfo("Writing info file ...")
		saveInfoFile(resultsDF,infoPath,params)
		//Logging.logInfo("Writing binout file ...")
		saveBinoutFile(resultsDF,binoutPath)
		//Logging.logInfo("Writing clustering result files ...")
		saveClusterFiles(resultsDF,clusterPath)

		resultsDF.unpersist(false)
		fullReadType.unpersist(false)
	}

	private def saveBinoutFile(df: DataFrame, path: String): Unit = {
		val binoutDF = df.select(col("predict").cast("string"))
		binoutDF.coalesce(1).write.format("text")
			.mode(SAVE_MODE).option("lineSep","\r\n").save(path)
	}
	private def saveClusterFiles(df: DataFrame, path: String): Unit = {
		val resultDF = df.select(col("data"),col("predict").as("cluster"))
		resultDF.coalesce(1).write.partitionBy("cluster").format("text")
			.mode(SAVE_MODE).option("lineSep","\r\n").save(path)
	}
	private def saveInfoFile(df: DataFrame, path: String, params: InputParams): Unit = {
		val infoDF = df.groupBy("predict").count.orderBy(col("predict").asc)
		val infoPrintoutDF = infoDF.select(concat(
			lit("Cluster "), col("predict"), lit(" : "),
			col("count"), lit(" reads")
		).as("info"))
		infoPrintoutDF.coalesce(1).write.format("text")
			.mode(SAVE_MODE).option("lineSep","\r\n").save(path)
		addText(params,path)
	}
	private def addText(params: InputParams, dirPath: String): Unit = {
		val numCluster = params.getSpeciesNum
		val addContent = "Number of cluster: " + numCluster.toString
		val path = Utils.findFilePath(dirPath,"txt")
		val readContent = Source.fromFile(path).getLines.mkString("\n")
		val file = new File(path)
		val bw = new BufferedWriter(new FileWriter(file,false))
		val writeContent = addContent.concat("\n") + readContent
		bw.write(writeContent)
		bw.close
	}

}

