package binning.tools

import Utils._

object Kmers {
	val normalizeBase = (base: Char) => base match {
		case 'A'|'a' => 'A'
		case 'C'|'c' => 'C'
		case 'G'|'g' => 'G'
		case 'T'|'t' => 'T'
		case _ => 'N'
	}
	private val baseToInt =  Map[Char,Int]('A' -> 0, 'C' -> 1, 'G' -> 2, 'T' -> 3)
	private val intToBase =  Map[Int,Char](0 -> 'A', 1 -> 'C', 2 -> 'G', 3 -> 'T')
	private val reverseBase = Map[Char,Char]('A' -> 'T', 'T' -> 'A', 'C' -> 'G', 'G' -> 'C', 'N' -> 'N')

	class DNASubSeq(val subSeq: Array[Byte], val size: Int){
		private def byteToUInt(byte: Byte): Int = if (byte > 0) byte.toInt else (byte+256).toInt
		def toBase: String = {
			val seqInts = this.subSeq.map(this.byteToUInt(_))
				.flatMap(int => Array((int>>>6)%4,(int>>>4)%4,(int>>>2)%4,int%4))
			val res = seqInts.take(this.size).map(intToBase(_)).mkString
			res
		}
		override def toString: String = this.toBase
	}
	class FrequencyVector(private val vector: Array[String] = Array("Frequency Vector"),private val defaultValue: Double = 0.0) {
		private val indexVectorMap: Map[String,Int] = this.vector.zipWithIndex.toMap
		private val frequencyVector: Array[Double] =  this.vector.map(_ => defaultValue)
		private val nonFindIndex: Int = -1
		private def _getIndex(key: String): Int = if (this.indexVectorMap.contains(key)) this.indexVectorMap(key) else this.nonFindIndex
		private def _getValue(index: Int): Double = this.frequencyVector(index)
		private def _incrementValue(index: Int, value: Double): Unit = this.frequencyVector(index) += value
		private def _changeValue(index: Int, value: Double): Unit = this.frequencyVector(index) = value
		def getInitialVector: Array[String] = this.vector
		def getDefaultValue: Double = this.defaultValue
		def capacity: Int = this.vector.length
		def getValue(key: String): Double = this._getValue(this._getIndex(key))
		def setValue(key: String, value: Double = 1D, increment: Boolean = true): Boolean = {
			val index: Int = this._getIndex(key)
			if (index == this.nonFindIndex) return false
			if (increment) this._incrementValue(index,value) else this._changeValue(index,value)
			return true
		}
		def setValues(keyList: Array[String], value: Double = 1D, increment: Boolean = true): Boolean = {
			val map = keyList.map(kmer => (kmer,1)).groupBy(_._1).mapValues(elems => elems.map(_._2).reduce(_+_))
			if (!increment) keyList.distinct.foreach{ elems => this.setValue(elems,value,false)}
			map.foreach{ case (k,v) => this.setValue(k,v,true) }
			return true
		}
		def getSparseVector: SparseVectorType = this.frequencyVector.zipWithIndex.map(_.swap).filter(elems=>elems._2!=0.0)
		def getDenseVector: DenseVectorType = this.frequencyVector
		def sparseToDenseVector(sparse: SparseVectorType): DenseVectorType = {
			val vMap = sparse.toMap
			val size = vMap.keys.max + 1
			val dense = Array.fill(size)(0.0)
			for (i <- 0 until size) dense(i) = if (vMap.contains(i)) vMap(i) else dense(i)
			dense
		}
		def denseToSparseVector(dense: DenseVectorType): SparseVectorType = dense.zipWithIndex.map(_.swap).filter(elems=>elems._2!=0.0)
		override def toString: String = s"[${this.getDenseVector.mkString(", ")}]"
	}

	def reverseComplement(kmer: String): String = kmer.reverse.map(reverseBase(_))
	def seqBaseToByte(seqStr: String): DNASeq = {
		val seqBytes = seqStr.map(normalizeBase).split("N")
			.filter(subSeq => subSeq.length > 0)
			.map(_seqBaseToByte(_))
		seqBytes
	}
	private def _seqBaseToByte(seqStr: String): DNASubSeq = {
		val seqSize = seqStr.length
		val seqBytes = seqStr.map(baseToInt(_)).grouped(4).map(bits => {
			val size = bits.length
			val byte = size match {
				case 4 => (bits(0)<<6) + (bits(1)<<4) + (bits(2)<<2) + bits(3)
				case 3 => (bits(0)<<6) + (bits(1)<<4) + (bits(2)<<2)
				case 2 => (bits(0)<<6) + (bits(1)<<4)
				case 1 => (bits(0)<<6)
			}
			byte.toByte
		}).toArray
		val res = new DNASubSeq(seqBytes,seqSize)
		res
	}
	private def hashFunctionKmer(kmer: String): Long = {
		val k = 4
		val _res = for(i <- 0 until kmer.length-1) yield {
			val nu = baseToInt(kmer(i))
			val nu_unit = scala.math.pow(k, i)
			nu * nu_unit
		}
		val res = _res.reduce(_+_).toLong
		res
	}
	def sliceSequences(read: ReadType,kmerSize: Int): Array[(KmerHashCode,Array[ReadIndex])] = {
		val seqs = read._3.flatMap(seq => seq)
		val res = seqs.flatMap(subSeq => generateKmers(subSeq.toBase,kmerSize)).distinct
//			.map(kmer => hashFunctionKmer(kmer) -> Array(read._1)).distinct
			.map(kmer => kmer -> Array(read._1))
		res
	}

	def generateKmers(str: String, kmerSize: Int): Array[String] = {
		val fragments = str.length-kmerSize+1
		if (fragments.equals(0)) return Array()
		val res = (0 until fragments).map(pos => str.substring(pos,pos+kmerSize))
		return res.toArray
	}

	def generateKmerVector(l: Int): Array[String] = {
		val bases = "ACGT"
		val vector = _generateKmerVector(bases,l).toBuffer
		var i: Int = 0
		while (i < vector.length) {
			val revKmer: String = reverseComplement(vector(i))
			if (vector.takeRight(vector.length-i-1).contains(revKmer)) {
				vector -= revKmer
				i -= 1
			}
			i += 1
		}
		vector.toArray
	}
	private def _generateKmerVector(bases: String, l: Int): Array[String] = {
		if (l == 1) return bases.toArray.map(_.toString)
		for (nu_i <- _generateKmerVector(bases,l-1); nu_k <- bases) yield nu_i + nu_k
	}

}
