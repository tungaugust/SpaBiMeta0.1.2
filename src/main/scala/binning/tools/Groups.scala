package binning.tools

import Utils._
import OverlapGraph._
import Kmers._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

object Groups {
	case class GroupClass(val id: GroupId,val seed: SeedType,val nonSeed: NonSeedType){
		def getSize: Int = this.seed.length + this.nonSeed.length
		override def toString: String = s"[Group]:id<${this.id}>-size[${this.getSize}]-seed[${this.seed.length}]-nonSeed[${this.nonSeed.length}]"
	}
	def grouping(componentRDD: RDD[(ReadIndex,Map[ReadIndex,NodeClass])],params: InputParams,sc: SparkContext): RDD[GroupClass] = {
		val bcParams = sc.broadcast(params)
		val _params = bcParams.value
		val seedMax = _params.getSeedSize
		val minGroup = _params.getMinGroupNum
		val discard = _params.isDiscard
		//val _groupRDD = componentRDD.flatMap(component => _grouping(component,seedMax))
		val _groupRDD = componentRDD.mapPartitions(iterator => {
			val res = iterator.map(component => _grouping(component,seedMax))
			res
		}).flatMap(groups=>groups)
		val groupRDD = if (discard) _groupRDD.filter(group => group.getSize >= minGroup) else _groupRDD
		groupRDD
	}
	private def isAdjacency(node: ReadIndex,adjacencyList: Array[ReadIndex],block: Map[ReadIndex,NodeClass]): Boolean = {
		for (v <- adjacencyList) { if (block(v).neighbors.contains(node)) return true }
		return false
	}
	private def _grouping(component: (ReadIndex,Map[ReadIndex,NodeClass]),seedMax: Int): Array[GroupClass] = {
		val (minCCId, block) = component
		val groups = new ArrayBuffer[GroupClass]()
		var grpIdx = 0
		for (vertex <- block.keys.toArray) {
			val seed = new ArrayBuffer[(ReadIndex,Array[DNASeq])]()
			val nonSeed = new ArrayBuffer[ReadIndex]()
			if (block(vertex).isVisited.equals(false)) {
				seed += Tuple2(vertex, block(vertex).seqs)
				block(vertex).setVisited(true)
				for (nbr <- block(vertex).neighbors) {
					if (block(nbr).isVisited.equals(false)) {
						nonSeed += nbr
						block(nbr).setVisited(true)
					}
				}
				var sIdx = 0
				var nsIdx = 0
				val whileLoop = new Breaks()
				whileLoop.breakable {
					while (seed.length < seedMax){
						if (nsIdx < nonSeed.length) {
							// Checking in Non-Seed Set
							for (nsNbr <- block(nonSeed(nsIdx)).neighbors) {
								if (block(nsNbr).isVisited.equals(false)) {
									val seedList = seed.toMap.keys.toArray
									if (isAdjacency(nsNbr,seedList,block)) { nonSeed += nsNbr }
									else { seed += Tuple2(nsNbr,block(nsNbr).seqs) }
									block(nsNbr).setVisited(true)
								}
							}
							nsIdx += 1
						} else if (sIdx < seed.length) {
							// Checking in Seed Set
							for (sNbr <- block(seed(sIdx)._1).neighbors) {
								if (block(sNbr).isVisited.equals(false)) {
									nonSeed += sNbr
									block(sNbr).setVisited(true)
								}
							}
							sIdx += 1
						} else {
							whileLoop.break()
						}
					}
				} // end while loop
				val groupId = Seq(minCCId,grpIdx).mkString(".")
				groups += GroupClass(groupId,seed.toArray,nonSeed.toArray)
				grpIdx += 1
			}
		} // end for loop
		return groups.toArray
	}
	//
	def computeFrequencyVector(groupRDD: RDD[GroupClass],params: InputParams,sc: SparkContext): RDD[(GroupId,DenseVectorType)] = {
		val l = params.getK
		val bcKmerVector = sc.broadcast(Kmers.generateKmerVector(l))
		val vectorRDD = groupRDD.map(group => _computeFrequencyVector(group,bcKmerVector.value,l))
		vectorRDD
	}
	private def _computeFrequencyVector(group: GroupClass,kmerVector: Array[String],l: Int): (GroupId,DenseVectorType) = {
		var contigSize = 0
		val vector = new FrequencyVector(kmerVector,0.0)
		for (read <- group.seed) {
			for (seq <- read._2) {
				val seqs = seq.map(subSeq => subSeq.toBase)
				val kmers = seqs.flatMap(generateKmers(_,l))
				val revKmers = kmers.map(reverseComplement(_))
				vector.setValues(keyList = kmers++revKmers, value = 1.0, increment = true)
				contigSize += kmers.length
			}
		}

		val freqVector = vector.getDenseVector.map(e => Utils.round(e/contigSize,DECIMAL_SIZE))
		(group.id,freqVector)
	}
}
