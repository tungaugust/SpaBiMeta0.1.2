package binning.tools

import Utils._
import Kmers._

object Sequences {
	def parseRawSequence(rawSequence: String): SequenceType = {
		val sequence = extractRawSequence(rawSequence)
		val description = sequence.head
		val seq = sequence.tail.map(_.trim).mkString
		val fastaRegex = "[\\d\\w]+"
		val matches = Utils.matchPattern(description,fastaRegex)
		val id = matches(0)
		val flag = if (matches(1).equals("1")) 0 else 1
		val label = matches(4)
		val res: SequenceType = (id,flag,label,seq,rawSequence)
		res
	}
	private def extractRawSequence(rawSequence: String): Array[String] = rawSequence.split('\n').map(_.trim.filter(_ >= ' '))
	def mergeSequences(sequences: ((ReadId,Array[(Int,(String,DNASeq,String))]),ReadIndex)): (ReadIndex,ReadId,String,Array[DNASeq],Array[String]) = {
		val (_prop, index) = sequences
		val (id, prop) = _prop
		val label = prop.map(_._2._1).head
		val seqs = prop.map(seq => seq._1 -> seq._2._2).sortBy(_._1).map(_._2)
		val raws = prop.map(seq => seq._1 -> seq._2._3).sortBy(_._1).map(_._2)
		val res = (index,id,label,seqs,raws)
		res
	}
}
