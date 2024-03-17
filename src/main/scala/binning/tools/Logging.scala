package binning.tools

import Utils._

import org.apache.log4j.{Level, Logger, LogManager}

object Logging extends Serializable {
	private var appName: String = ""
	def setAppName(name: String = ""): Unit = if (name.length != 0) appName = name
	private def getAppName: String = appName
	private def logger: Logger = {
		lazy val log = LogManager.getLogger(getAppName)
		log.setLevel(Level.ALL)
		log
	}
	def logInfo(msg: String = ""): Unit = if (msg.length>0) logger.info(msg)
	def logDebug(msg: String = ""): Unit = if (msg.length>0) logger.debug(msg)
	def logWarn(msg: String = ""): Unit = if (msg.length>0) logger.warn(msg)
}