package binning.tools

import Utils._
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.commons.io.FileUtils

import scala.reflect.ClassTag
import java.io.IOException
import java.io.File

object PreProcess {
	// Default Parameters
	private val OUTPUT_DIR: String = ""
	private val CHECKPOINT_DIR: String = ""
	private val RUN_MODE: Int = 0
	private val SP_NUM: Int = 2
	private val KMER_SIZE: Int = 30
	private val SHARED_KMERS_NUM: Int = 5
	private val SEED_SIZE: Int = 20
	private val DISCARD: Boolean = true
	private val VERBOSE: Boolean = false
	private val TEMP_FILE_TYPE: String = "parquet"
	private val MIN_GROUP_NUM: Int = 5
	private val K_VALUE: Int = 4

	private def parseCommandLine(programName: String, programVersion: String,
															 _args: Array[String], _defaultArgs: Array[String]): InputParams = {
		val args = if (_args.length==0) _defaultArgs else _args
		val hasHelp = args.length==0 || ( args.length==1 && (args.contains("-h")||args.contains("--help")) )
		if (hasHelp) Utils.quitProgram(true)
		val hasInput = args.contains("-i") || args.contains("--input")
		if (hasInput.equals(false)) Utils.quitProgram(true,"# WARNING # Arguments list do not contain input file.")
		val _argMap = args.sliding(2,1).filter(elems => elems(0).head.equals('-') && !elems(1).head.equals('-'))

		val argMap = _argMap.map(arg => {
			val key = if (arg(0).equals("-M")) "-md" else arg(0)
			val value = arg(1)
			key.toLowerCase match {
				case "-md"|"--mode" => "mode" -> value
				case "-i"|"--input" => "input" -> value
				case "-o"|"--output" => "output" -> value
				case "-p"|"--checkpoint" => "checkpoint" ->value
				case "-c"|"--numclus"|"--numsp" => "numsp" -> value
				case "-l"|"--ksize"|"--kmersize" => "kmersize" -> value
				case "-m"|"--thres"|"--threshold" => "thres" -> value
				case "-s"|"--ssize"|"--seedsize" => "ssize" -> value
				case "-d"|"--discard" => "discard" -> value
				case "-v"|"--verbose" => "verbose" -> value
				case "-f"|"--format" => "format" -> value
				case _ => key -> value
			}
		}).toMap
		val argKeys = argMap.keys.toArray
		val input = if (Utils.exists(argMap("input")).equals(false)) {
			Utils.quitProgram(false, "# WARNING # The input file not found.")
			""
		} else argMap("input")
		val checkpoint = if (argKeys.contains("checkpoint").equals(true)) {
			val checkpointDir = argMap("checkpoint")
			if (Utils.exists(checkpointDir).equals(false)) {
				Utils.quitProgram(false, "# WARNING # The checkpoint directory not found.")
				""
			} else checkpointDir
		} else CHECKPOINT_DIR
		val output = if (argKeys.contains("output").equals(true)) {
			val outputDir = argMap("output")
			if (Utils.exists(outputDir).equals(false)) {
				quitProgram(false, "# WARNING # The output directory not found.")
				""
			} else outputDir
		} else OUTPUT_DIR
		val mode =  if (argKeys.contains("mode")) argMap("mode").toInt else RUN_MODE
		val numSp = if (argKeys.contains("numsp")) argMap("numsp").toInt else SP_NUM
		val kmerSize = if (argKeys.contains("kmersize")) argMap("kmersize").toInt else KMER_SIZE
		val thres = if (argKeys.contains("thres")) argMap("thres").toInt else SHARED_KMERS_NUM
		val ssize = if (argKeys.contains("ssize")) argMap("ssize").toInt else SEED_SIZE
		val discard = if (argKeys.contains("discard")) { argMap("discard") match {
				case "0" => DISCARD
				case _ => !DISCARD
			}
		} else DISCARD
		val verbose = if (argKeys.contains("verbose")) { argMap("verbose") match {
				case "0" => VERBOSE
				case _ => !VERBOSE
			}
		} else VERBOSE
		val fileType = if (argKeys.contains("format")) { argMap("format") match {
				case "parquet" => TEMP_FILE_TYPE
				case "text" => "text"
				case _ => TEMP_FILE_TYPE
			}
		} else TEMP_FILE_TYPE
		val res = new InputParams(
			programName,programVersion,
			mode,input,output,checkpoint,numSp,kmerSize,thres,ssize,
			discard,verbose,fileType,MIN_GROUP_NUM,K_VALUE
		)
		res
	}

	private def processCheckpointDir(checkpointPath: String): Unit = {
		val existCheckpointDir = Utils.exists(checkpointPath)
		if (existCheckpointDir) {
			try {
				FileUtils.cleanDirectory(new File(checkpointPath))
			}
			catch {
				case e: IOException => println(s"# WARNING # Cleaning is unsuccessful in directory $checkpointPath")
				case e: IllegalArgumentException => println(s"# WARNING # Please check and try again directory $checkpointPath")
			}
		} else {
			val mkcheckpointDir = new File(checkpointPath).mkdirs
			if ((mkcheckpointDir && Utils.exists(checkpointPath)).equals(false))
				Utils.quitProgram(false,"# WARNING # Cannot create checkpoint directory not found.")
		}
	}

	private def processTempDir(tempPath: String): Unit = {
		val existTempDir = Utils.exists(tempPath)
		if (existTempDir) {
			try {
				FileUtils.cleanDirectory(new File(tempPath))
			}
			catch {
				case e: IOException => println(s"# WARNING # Cleaning is unsuccessful in directory $tempPath")
				case e: IllegalArgumentException => println(s"# WARNING # Please check and try again directory $tempPath")
			}
		}
	}

	private def processOutputDir(params: InputParams): Unit = {
		val infoPath = params.mkOutPath(INFO_PATH_NAME)
		val clusterPath = params.mkOutPath(CLUSTER_PATH_NAME)
		val binoutPath = params.mkOutPath(BINOUT_PATH_NAME)
		val dirList = List(infoPath,clusterPath,binoutPath)
		Utils.deleteDirectories(dirList)
	}

	private def registerKryo(conf: SparkConf): SparkConf ={
		conf.registerKryoClasses(Array(
			classOf[SparkConf],classOf[InputParams],classOf[(InputParams,SparkConf)],classOf[Broadcast[InputParams]],
			classOf[SequenceType],classOf[(ReadId,Int,String,String,String)],
			classOf[binning.tools.Kmers.DNASubSeq],classOf[Array[Byte]],
			classOf[DNASeq],classOf[Array[binning.tools.Kmers.DNASubSeq]],
			classOf[(ReadId,Array[(Int,(String,DNASeq,String))])],
			classOf[Array[DNASeq]],classOf[Array[Array[binning.tools.Kmers.DNASubSeq]]],
			classOf[(ReadId,String,Array[DNASeq],Array[String])],
			classOf[((ReadId,Array[(Int,(String,DNASeq,String))]),ReadIndex)],
			classOf[FullReadType],classOf[(ReadIndex,ReadId,String,Array[DNASeq],Array[String])],
			classOf[ReadType],classOf[(ReadIndex,String,Array[DNASeq])],
			classOf[(KmerHashCode,Array[ReadIndex])],classOf[Array[(String,Array[ReadIndex])]],classOf[Array[ReadIndex]],classOf[Array[String]],
			classOf[(ReadIndex,ReadIndex)],classOf[((ReadIndex,ReadIndex),Int)],classOf[Array[(ReadIndex,ReadIndex)]],
			classOf[OverGraph],classOf[org.apache.spark.graphx.Graph[VD,ED]],
			classOf[(ReadIndex,(ReadIndex,Array[DNASeq]))],
			classOf[(org.apache.spark.graphx.VertexId,Array[org.apache.spark.graphx.VertexId])],
			classOf[org.apache.spark.graphx.VertexRDD[Array[org.apache.spark.graphx.VertexId]]],
			classOf[binning.tools.OverlapGraph.NodeClass],
			classOf[(ReadIndex,Map[ReadIndex,binning.tools.OverlapGraph.NodeClass])],
			classOf[Map[ReadIndex,binning.tools.OverlapGraph.NodeClass]],
			classOf[org.apache.spark.sql.DataFrame],classOf[org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]],
			classOf[String],
			classOf[SeedType],classOf[Array[(ReadIndex,Array[DNASeq])]],classOf[NonSeedType],
			classOf[binning.tools.Groups.GroupClass],classOf[Array[binning.tools.Groups.GroupClass]],
			classOf[binning.tools.Kmers.FrequencyVector],
			classOf[DenseVectorType],classOf[Array[Double]],
			classOf[SparseVectorType],classOf[Array[(Int,Double)]],
			classOf[(GroupId,Int)],classOf[(ReadIndex,String)],classOf[(ReadIndex,String,Int)],classOf[(ReadIndex,Int)],
			classOf[(GroupId,DenseVectorType)],classOf[(GroupId,ReadIndex)],
			classOf[(GroupId,org.apache.spark.mllib.linalg.Vector)],
			classOf[(GroupId,org.apache.spark.ml.linalg.Vector)],
			classOf[((String, Int), Int)],
			classOf[(ReadIndex, Array[(Int, String)])],
			classOf[(ReadIndex, (ReadId, Array[String]))],

			classOf[org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage],
			classOf[org.apache.spark.sql.execution.datasources.WriteTaskResult],
			classOf[org.apache.spark.sql.execution.datasources.ExecutedWriteSummary],
			classOf[org.apache.spark.sql.execution.datasources.BasicWriteTaskStats],
			classOf[org.apache.spark.sql.types.StructType],classOf[Array[org.apache.spark.sql.types.StructType]],
			classOf[org.apache.spark.sql.types.StructField],classOf[Array[org.apache.spark.sql.types.StructField]],
			classOf[org.apache.spark.sql.types.Metadata],
			classOf[org.apache.spark.sql.types.ArrayType],
			classOf[org.apache.spark.sql.catalyst.expressions.GenericInternalRow],
			classOf[Array[org.apache.spark.sql.catalyst.InternalRow]],
			classOf[org.apache.spark.unsafe.types.UTF8String],
			classOf[java.math.BigDecimal],
			classOf[scala.collection.mutable.ArraySeq[_]],

			classOf[org.apache.spark.sql.execution.columnar.DefaultCachedBatch],
			classOf[org.apache.spark.sql.execution.joins.LongHashedRelation],
			Class.forName("org.apache.spark.sql.execution.joins.LongToUnsafeRowMap"),
			Class.forName("org.apache.spark.sql.types.LongType$"),
			Class.forName("org.apache.spark.sql.types.StringType$"),
			Class.forName("org.apache.spark.sql.types.DoubleType$"),
			Class.forName("[[B"),
			Class.forName("org.apache.spark.util.HadoopFSUtils$SerializableFileStatus"),
			Class.forName("org.apache.spark.util.HadoopFSUtils$SerializableBlockLocation"),
			Class.forName("scala.reflect.ClassTag$GenericClassTag"),
//			Class.forName("scala.reflect.ClassTag$$anon$1",false,getClass.getClassLoader),
			ClassTag(Class.forName("org.apache.spark.util.HadoopFSUtils$SerializableBlockLocation")).wrap.runtimeClass,
			ClassTag(Class.forName("org.apache.spark.mllib.clustering.VectorWithNorm")).wrap.runtimeClass
		))
		conf
	}

	private def sparkConf: SparkConf = {
		val conf = new SparkConf()
			.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
			.set("spark.kryo.registrationRequired","true")
			.set("spark.speculation","true")
		conf
	}

	def run(logName: String, programName: String, programVersion: String,
					args: Array[String], defaultArgs: Array[String]): (InputParams,SparkConf) = {
		val conf = registerKryo(sparkConf)
		val _appName = conf.getOption("spark.app.name").getOrElse(logName)
		val appName = if (_appName.equals("binning.SpaBiMeta")) logName else _appName
		conf.setAppName(appName)

		conf.set("spark.memory.fraction","0.7") // Default: 0.6 - Working Memory
		conf.set("spark.memory.storageFraction","0.3") // Default: 0.5 - Storage Memory
//		conf.set("spark.sql.shuffle.partitions","8")
//		conf.set("spark.default.parallelism","40")
//		conf.set("spark.sql.inMemoryColumnarStorage.compressed","true")
//		conf.set("spark.sql.inMemoryColumnarStorage.batchSize","10000")
//		conf.set("spark.sql.cbo.enabled","true")
//		conf.set("spark.kryoserializer.buffer.max","512m")
//		conf.set("spark.dynamicAllocation.enabled","false")
//		conf.set("spark.network.timeout","360000")
//		conf.set("spark.executor.heartbeatInterval","10000000")
//		conf.set("spark.rdd.compress","true")
//		conf.set("spark.shuffle.compress","true")
//		conf.set("spark.shuffle.spill.compress","true")
//		conf.set("spark.executor.extraJavaOptions","-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'")
//		conf.set("spark.driver.extraJavaOptions","-Xms2g -XX:+UseCompressedOops -XX:+UseConcMarkSweepGC -XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'")
//		conf.set("spark.driver.maxResultSize","5g")
//		conf.set("spark.storage.level","MEMORY_AND_DISK_SER")

		val params = parseCommandLine(programName,programVersion,args,defaultArgs)
		processCheckpointDir(params.getCheckpointDirPath(false))
		processTempDir(params.getTempDirPath(false))
		processOutputDir(params)
		(params,conf)
	}
}
