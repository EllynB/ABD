import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def decoupage(a:Array[String]) : String = {
	val it = a.toIterator
	var r = Array[(String,String)]()
	var c = it.next()
	var ancien = c

	while (it.hasNext) {
		c = it.next()
		if (c!="" && ancien!="")  r = r :+ (ancien,c)
		ancien = c 
	}
	return r.mkString
}

  def main(args: Array[String]) {

        val conf = new SparkConf().setAppName("Spark Integrale")
        val spark = new SparkContext(conf)
 	
	val textFile = spark.textFile("miser.txt")

	val mots = textFile.map(line => decoupage(line.split(" ")))
	
	mots.saveAsTextFile("result.txt")
  }
}
