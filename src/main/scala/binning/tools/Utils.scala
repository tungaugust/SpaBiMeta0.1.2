package binning.tools

import Kmers._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import java.io.File
import scala.reflect.io.Directory
import scala.util.matching.Regex
import org.apache.spark.util.SizeEstimator

import scala.util.control.Breaks

object Utils {
	private val USAGE_TEXT: String = """
	| [ Spark Version: 3.2.1 | Scala Version: 2.12.10 ]
	| USAGE: arguments of *.jar file:
	|	Syntax:	app_name.jar -i /path_to_input_file/file.fna [options]
	|	Options:
	| 	-h, --help : Show Usage.
	| 	-M, --mode : Run mode of program. Default value: 0
	| 	-i, --input : Path of input file.
	| 	-o, --output : Path of output directory.
	| 	-p, --checkpoint : Path of checkpoint directory.
	| 	-c, --numSp, --numClus : Number of species. Default value: 2
	| 	-l, --ksize, --kmersize : Length of l-mers which are used to merge reads in the first phase of binning process. Default value: 30
	| 	-m, --thres, --threshold : Number of shared l-mers between reads, which are used to determine whether it should merge reads or not. Default value: 5
	| 	-s, --ssize, --seedsize : The maximum number of reads in each seed. Default value: 20
	| 	-d, --discard : Discard reads, default has discard reads, if yes then the cluster is labeled as -1. Default value: 0
	| 	-v, --verbose	: Display detailed information, default does not display verbose. Default value: 0
	| 	-f, --format : File Type is saved temporary directory [parquet|text]. Default value: parquet
	|
	|	Examples:
	|		app.jar -i /path_to_input_file/file.fna -o /path_of_output_dir -p /path_of_checkpoint_dir -c 2 -l 30 -m 5 -s 20 -d 0 -v 0 -f parquet
	|		app.jar -i /path_to_input_file/file.fna
	|
  """.stripMargin
	// Default Parameters
	private val PROGRAM_VERSION: String = "0.0.0"
	private val PROGRAM_NAME: String = "SpaBiMeta"
	val DISCARD_LABEL: Int = -1
	val MIN_KMERS_NUM: Int = 2

	val BINOUT_PATH_NAME: String = "binout"
	val INFO_PATH_NAME: String = "info"
	val CLUSTER_PATH_NAME: String = "cluster"
	val DECIMAL_SIZE = 18

	val TASKS_PER_CORE = 2
	val MAX_PARTITION_MEGABYTES = 100 //MB
	val MAX_PARTITION_MEGABYTES_LIMIT = 200 //MB
	val INCREASE_RATE = 1.1

	// User-defined Types
	type ReadId = String
	type ReadIndex = Long
	type SequenceType = (ReadId,Int,String,String,String)
	type DNASeq = Array[DNASubSeq]
	type FullReadType = (ReadIndex,ReadId,String,Array[DNASeq],Array[String])
	type ReadType = (ReadIndex,String,Array[DNASeq])
	type KmerHashCode = String
//	type KmerHashCode = Long

	type VD = Int
	type ED = Int
	type OverGraph = org.apache.spark.graphx.Graph[VD,ED]

	type GroupId = String
	type SeedType = Array[(ReadIndex,Array[DNASeq])]
	type NonSeedType = Array[ReadIndex]
	type SparseVectorType = Array[(Int,Double)]
	type DenseVectorType = Array[Double]

	class InputParams(private val programName: String = PROGRAM_NAME, private val programVersion: String = PROGRAM_VERSION,
										private val mode: Int, private val input: String, private val output: String, private val checkpoint: String,
										private val numSpecies: Int, private val kmerSize: Int, private val threshold: Int, private val seedSize: Int,
										private val discard: Boolean, private val verbose: Boolean,private val format: String,
										private val minGroupNum: Int, private val k_value: Int
									 ){
		private def getMainDirName: String = this.programName.toLowerCase.concat("-"+this.programVersion+"-output")

		def getProgramName: String = this.programName
		def getFileName: String = this.input.split("/").takeRight(1).mkString
		def getInputFilePath: String = this.input
		def getMode: Int = this.mode
		def getSpeciesNum: Int = this.numSpecies
		def getKmerSize: Int = this.kmerSize
		def getThreshold: Int = this.threshold
		def getSeedSize: Int = this.seedSize
		def isDiscard: Boolean = this.discard
		def isVerbose: Boolean = this.verbose
		def getFormat: String = this.format
		def getMinGroupNum: Int = this.minGroupNum
		def getK: Int = this.k_value

		private def getDirContainInput(endSlash: Boolean = true): String = {
			val slash = if (endSlash) "/" else ""
			this.input.substring(0, this.input.lastIndexOf("/")).concat(slash)
		}
		def getOutputDirPath(endSlash: Boolean = true): String = {
			val slash = if (endSlash) "/" else ""
			if (this.output.equals("")) {
				this.getDirContainInput(true).concat(this.getMainDirName+"/output"+slash)
			} else {
				val outPath = this.output
				val endString = outPath.takeRight(1)
				val hasEndSlash = {	endString == "/" || endString == "\\" }
				val out = hasEndSlash match {
					case true => outPath.concat("output"+slash)
					case false => outPath.concat("/output"+slash)
				}
				out
			}
		}
		def getCheckpointDirPath(endSlash: Boolean = true): String = {
			val slash = if (endSlash) "/" else ""
			if (this.checkpoint.equals("")) {
				this.getDirContainInput(true).concat(this.getMainDirName+"/checkpoint"+slash)
			} else this.checkpoint
		}
		def getTempDirPath(endSlash: Boolean = true): String = {
			if (endSlash) this.getOutputDirPath(false).concat("/temp/")
			else this.getOutputDirPath(false).concat("/temp")
		}
		def getOutFileName: String = {
			this.getFileName.concat(".") + Array(
				this.getKmerSize.toString, this.getThreshold.toString,
				this.getSeedSize.toString, this.getSpeciesNum.toString
			).mkString(".")
		}

		private val PARQUET_EDGE_SUFFIX = ".edges.parquet"
		private val TEXT_EDGE_SUFFIX = ".edges.text"
		def getEdgeDirPath(endSlash: Boolean = false): String = {
			val suffix = this.getFormat match {
				case "parquet" => PARQUET_EDGE_SUFFIX
				case "text" => TEXT_EDGE_SUFFIX
				case _ => PARQUET_EDGE_SUFFIX
			}
			val slash = if (endSlash) "/" else ""
			val res = this.getTempDirPath(true) + this.getOutFileName + suffix + slash
			res
		}
		private val PARQUET_KMER_SUFFIX = ".kmers.parquet"
		private val TEXT_KMER_SUFFIX = ".kmers.text"
		def getKmerDirPath(endSlash: Boolean = false): String = {
			val suffix = this.getFormat match {
				case "parquet" => PARQUET_KMER_SUFFIX
				case "text" => TEXT_KMER_SUFFIX
				case _ => PARQUET_KMER_SUFFIX
			}
			val slash = if (endSlash) "/" else ""
			val res = this.getTempDirPath(true) + this.getOutFileName + suffix + slash
			res
		}
		private val PARQUET_VECTOR_SUFFIX = ".vectors.parquet"
		private val TEXT_VECTOR_SUFFIX = ".vectors.text"
		def getVectorDirPath(endSlash: Boolean = false): String = {
			val suffix = this.getFormat match {
				case "parquet" => PARQUET_VECTOR_SUFFIX
				case "text" => TEXT_VECTOR_SUFFIX
				case _ => PARQUET_VECTOR_SUFFIX
			}
			val slash = if (endSlash) "/" else ""
			val res = this.getTempDirPath(true) + this.getOutFileName + suffix + slash
			res
		}

		private val PARQUET_PATTERN = "part*.parquet"
		private val TEXT_PATTERN = "part*"
		def getEdgeFilesPath: String = {
			val pattern = this.getFormat match {
				case "parquet" => PARQUET_PATTERN
				case "text" => TEXT_PATTERN
				case _ => PARQUET_PATTERN
			}
			val res = this.getEdgeDirPath(true) + pattern
			res
		}
		def getKmerFilesPath: String = {
			val pattern = this.getFormat match {
				case "parquet" => PARQUET_PATTERN
				case "text" => TEXT_PATTERN
				case _ => PARQUET_PATTERN
			}
			val res = this.getKmerDirPath(true) + pattern
			res
		}
		def getVectorFilesPath: String = {
			val pattern = this.getFormat match {
				case "parquet" => PARQUET_PATTERN
				case "text" => TEXT_PATTERN
				case _ => PARQUET_PATTERN
			}
			val res = this.getVectorDirPath(true) + pattern
			res
		}

		def mkOutPath(msg: String): String = { this.getOutputDirPath(true)+this.getOutFileName+".".concat(msg) }

		override def toString: String = { s"\n\tFile: ${this.input}" +
			s"\n\tNumber of species: ${this.numSpecies}" +
			s"\n\tLength of l-mers: ${this.kmerSize}" +
			s"\n\tNumber of shared l-mers: ${this.threshold}"
		}
	}

	//
	def exists(path: String): Boolean = new File(path).exists
	def quitProgram(help: Boolean = false, msg: String = ""): Unit = {
		if (msg.length > 0) println(msg)
		if (help) println(USAGE_TEXT)
		System.exit(0)
	}

	def deleteDirectories(dirs: List[String]): Boolean = dirs.map(dir=>deleteDir(dir)).reduce({(x,y) => x && y})
	private def deleteDir(path: String): Boolean = (new Directory(new File(path))).deleteRecursively()

	def findFilePath(dirPath: String, extension: String = "txt"): String = {
		val filePath = Directory(dirPath).walkFilter(_.extension==extension).map(p=>p.path).toArray.head
		filePath
	}

	def timePrintout(timeStart: Long): String = {
		val millis: Long = System.currentTimeMillis() - timeStart
		val seconds = round(millis/1000.0,2)
		val (hoursTime,minutesTime,secondsTime) = convertTime(seconds)
		val time = "%d:%02d:%02d".format(hoursTime,minutesTime,secondsTime)
		val res = s"$seconds s ~ [$time]"
		res
	}
	def round(value: Double, scale: Int): Double = if (scale >= 0) s"%.${scale}f".format(value).toDouble else Double.MaxValue
	private def convertTime(seconds: Double): (Long,Long,Long) = {
		var secondsTime = seconds.round
		val hoursTime = secondsTime / 3600
		secondsTime %= 3600
		val minutesTime = secondsTime / 60
		secondsTime %= 60
		(hoursTime,minutesTime,secondsTime)
	}

	def matchNumber(str: String): Array[String] = {
		val numPattern = new Regex("[0-9]+")
		val matches = numPattern.findAllIn(str)
		matches.toArray
	}
	def matchPattern(str: String, pattern: String): Array[String] = {
		val _pattern = new Regex(pattern)
		val matches = _pattern.findAllIn(str)
		matches.toArray
	}
	def getCanonicalCombinations(list: Array[ReadIndex]): Array[(ReadIndex,ReadIndex)] = {
		val res = list.combinations(2)
			.map(pair => (pair(0), pair(1)))
			.map(pair => if (pair._1 <= pair._2) pair else pair.swap)
		res.toArray
	}
	//
	def getFileSizeInBytes(path: String): Long = {
		val inputFile = new File(path)
		inputFile.length()
	}
	def getPartitions(spark: SparkSession): Int = {
		val partitions: Int = spark.conf.getOption("spark.default.parallelism") match {
			case Some(_str) => _str.toInt
			case None => spark.sparkContext.defaultParallelism*TASKS_PER_CORE
		}
		partitions
	}
	def setShufflePartitions(partitions: Int, spark: SparkSession): Unit = {
		spark.conf.set("spark.sql.shuffle.partitions", partitions.toLong)
	}
	def setMaxPartitonBytes(bytes: Long, spark: SparkSession): Unit = {
		spark.conf.set("spark.sql.files.maxPartitonBytes", bytes.toLong)
	}
	def getMaxPartitonBytes(spark: SparkSession): Unit = {
		val bytes: Long = spark.conf.getOption("spark.sql.files.maxPartitonBytes") match {
			case Some(_str) => _str.toLong
			case None => 134217728
		}
	}
	def setDefaultMaxPartitonBytes(spark: SparkSession): Unit = {
		setMaxPartitonBytes((Math.pow(2,20)*MAX_PARTITION_MEGABYTES).toLong, spark)
	}
	def computeCurrentPartitions(fileSizeInBytes: Long,rate: Double,sc: SparkContext,spark: SparkSession): Int = {
		val parallelism: Int = getPartitions(spark)
		val maxPartitonBytes: Long = spark.conf.getOption("spark.sql.files.maxPartitonBytes") match {
			case Some(_str) => _str.toLong
			case None => (Math.pow(2, 20) * MAX_PARTITION_MEGABYTES).toLong
		}
		val _partitions: Int = ((fileSizeInBytes*rate/maxPartitonBytes).ceil.toInt/parallelism).floor.toInt * parallelism
		val partitions: Int = if (_partitions < parallelism) parallelism else _partitions
		partitions
	}
}
