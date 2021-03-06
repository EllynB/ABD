import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def decoupage(a:Array[String]) : Array[(String,Int)]  = {
	val it = a.toIterator
	var r = Array[(String,Int)]()
	var c = it.next()
	var ancien = c

	while (it.hasNext) {
		c = it.next()
		if (c!="" && ancien!="") {
			var str = ancien + " " + c
			r = r :+ (str,1)
		}
		ancien = c 
	}
	return r
}

  def main(args: Array[String]) {

        val conf = new SparkConf().setAppName("Spark Integrale")
        val spark = new SparkContext(conf)
 	
	val textFile = spark.textFile("miser.txt", 2)

	//val t1 = System.currentTimeMillis()
	val mots = textFile.flatMap(line => decoupage(line.split(" "))).reduceByKey(_+_)

	//println("\n\n Temps d'execution en ms = " + (System.currentTimeMillis()-t1) + "\n\n")
	
	mots.saveAsTextFile("result.txt")
	spark.stop()
  }
}
