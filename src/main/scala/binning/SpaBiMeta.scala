/*------------------------------------------------------/
Faculty of Information technology
Hochiminh City University of Technology and Education
Reference: https://bioinfolab.fit.hcmute.edu.vn/BiMeta
/------------------------------------------------------*/
package binning

import binning.options._
import binning.tools.{Logging, IOProcess, PreProcess}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.GraphXUtils

object SpaBiMeta {
	def main(args: Array[String]): Unit = {
		val LOG_PROGRAM_NAME: String = "Spark BiMeta"
		val PROGRAM_NAME: String = "SpaBiMeta"
		val PROGRAM_VERSION: String = "0.1.3"

		Logger.getLogger("org").setLevel(Level.ERROR)
//		val defaultCMLArgs = "--help"
//		val defaultCMLArgs = "-M 0 -i C:/bio-data/input/testfile.fna -o C:/bio-data/output -p C:/bio-data/checkpoint -c 2 -l 30 -m 5 -s 20 -d 0 -f parquet -v 0"
//		val defaultCMLArgs = "-M 0 -i C:/bio-data/input/L1.fna -o C:/bio-data/output -p C:/bio-data/checkpoint -c 2 -l 30 -m 5 -s 20 -d 0 -f parquet -v 0"
//		val defaultCMLArgs = "-M 0 -i C:/bio-data/input/R1.fna -o C:/bio-data/output -p C:/bio-data/checkpoint -c 2 -l 30 -m 45 -s 20 -d 0 -f parquet -v 0"
		val defaultCMLArgs = "-M 0 -i C:/bio-data/input/S1.fna -o C:/bio-data/output -p C:/bio-data/checkpoint -c 2 -l 30 -m 5 -s 20 -d 0 -f parquet -v 0"
//		val defaultCMLArgs = "-M 0 -i C:/bio-data/input/S4.fna -o C:/bio-data/output -p C:/bio-data/checkpoint -c 2 -l 30 -m 5 -s 20 -d 0 -f parquet -v 0"
//		val defaultCMLArgs = "-M 0 -i C:/bio-data/input/S6.fna -o C:/bio-data/output -p C:/bio-data/checkpoint -c 3 -l 30 -m 5 -s 20 -d 0 -f parquet -v 0"
//		val defaultCMLArgs = "-M 0 -i C:/bio-data/input/S9.fna -o C:/bio-data/output -p C:/bio-data/checkpoint -c 15 -l 30 -m 5 -s 20 -d 0 -f parquet -v 0"
//		val defaultCMLArgs = "-M 0 -i C:/bio-data/input/S10.fna -o C:/bio-data/output -p C:/bio-data/checkpoint -c 30 -l 30 -m 5 -s 20 -d 0 -f parquet -v 0"
		val defaultArgs = defaultCMLArgs.split(" ")
		val (params,conf) = PreProcess.run(LOG_PROGRAM_NAME,PROGRAM_NAME,PROGRAM_VERSION,args,defaultArgs)

		GraphXUtils.registerKryoClasses(conf)
		val MASTER = "local[*]"
		val spark = SparkSession.builder()
			.master(MASTER)
			.config(conf)
			.getOrCreate()
		val sc = spark.sparkContext
		sc.setCheckpointDir(params.getCheckpointDirPath(false))
		val logAppName = sc.appName + " ~ v".concat(PROGRAM_VERSION.replace('.','-'))
		Logging.setAppName(logAppName)

		if (params.isVerbose) {
			Logging.logInfo("===== Config Information:")
			Logging.logInfo(s"App Name: ${sc.appName}")
			Logging.logInfo(s"Master: ${sc.master}")
			Logging.logInfo(s"Parallelism: ${sc.defaultParallelism}")
			Logging.logInfo(s"spark.memory.fraction: ${spark.conf.getOption("spark.memory.fraction").getOrElse(None)}")
			Logging.logInfo(s"spark.memory.storageFraction: ${spark.conf.getOption("spark.memory.storageFraction").getOrElse(None)}")
			Logging.logInfo(s"spark.driver.memory: ${spark.conf.getOption("spark.driver.memory").getOrElse(None)}")
			Logging.logInfo(s"spark.driver.cores: ${spark.conf.getOption("spark.driver.cores").getOrElse(None)}")
			Logging.logInfo(s"spark.executor.cores: ${spark.conf.getOption("spark.executor.cores").getOrElse(None)}")
			Logging.logInfo(s"spark.executor.memory: ${spark.conf.getOption("spark.executor.memory").getOrElse(None)}")
			Logging.logInfo(s"spark.sql.shuffle.partitions: ${spark.conf.getOption("spark.sql.shuffle.partitions").getOrElse(None)}")
			Logging.logInfo(s"spark.sql.files.maxPartitonBytes: ${spark.conf.getOption("spark.sql.files.maxPartitonBytes").getOrElse(None)}")
			Logging.logInfo(s"spark.sql.files.openCostInBytes: ${spark.conf.getOption("spark.sql.files.openCostInBytes").getOrElse(None)}")
			Logging.logInfo(s"spark.default.parallelism: ${spark.conf.getOption("spark.default.parallelism").getOrElse(None)}")
		}
		Logger.getRootLogger.setLevel(Level.ERROR)

		Logging.logInfo("********** BEGIN PROGRAM **********")
		Logging.logInfo("===== Input Parameters:" + params)
		val startProgramTime = System.currentTimeMillis()

		//if (params.isVerbose) Logging.logInfo(s"===== MODE: ${params.getMode}")
		Logging.logInfo("===== Reading file ...")
//		val rawFastaData = IOProcess.readRawFastaFile(params.getInputFilePath)
		val rawFastaData = IOProcess.readRawLargeFastaFile(params.getInputFilePath)
		params.getMode match {
			case 0 => Program00.run(startProgramTime,rawFastaData,params,sc,spark)
			case _ => println("=====>>>>> Unknown <<<<<=====")
		}

		Logging.logInfo("********** END PROGRAM **********\n")
		sc.stop
		spark.stop
		Thread.sleep(1000)
		println("Done.")
		System.exit(0)
	}
}
