import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils

object KMeansModelTest {
  def main(args: Array[String]): Unit = {
    //disable logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val modelInput = "C:\\dataForScalaProjects\\kMeans";

    val conf = new SparkConf()
    conf.setAppName("spark-sreaming")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(1))

    //in order to avoid IO.Exception
    System.setProperty("hadoop.home.dir", "C:\\winutil\\")

    // Configure your Twitter credentials
    val apiKey = "Kj3RSuJf9A497P4frKnLGr0DX"
    val apiSecret = "OFZLCakyRlU2GcJSrO5uES3dzgtmVCmZjJU4bRxsAzCPCu5gGz"
    val accessToken = "924169777068822531-eVnwe3T5LU6MLhYqT80ff0TTde7PbXn"
    val accessTokenSecret = "QkGYy1oBk4FhyLyfNNqQQivNbAQotoUkM4BtHsVSMaeUR"

    System.setProperty("twitter4j.oauth.consumerKey", apiKey)
    System.setProperty("twitter4j.oauth.consumerSecret", apiSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // Create Twitter Stream
    val stream = TwitterUtils.createStream(ssc, None)
    val tweets = stream.map(t => t.getText)

    println("Initializing the KMeans model...")
    val model = KMeansModel.load(sc, modelInput)
    val langNumber = 2
    tweets.foreachRDD(rdd =>{
      val count = rdd.count()
      println(count)
      if (count > 0) {
        val outputRDD = rdd.repartition(1)
        val filtered = outputRDD.filter(t => model.predict(featurize(t)) == langNumber)
        filtered.foreach(println)
        println("---------------------------------------------------------------------------------------------")
      }
    })


    ssc.start()
    ssc.awaitTermination()
  }

  def featurize(s: String) = {
    val numFeatures = 1000
    val tf = new HashingTF(numFeatures)
    tf.transform(s.sliding(2).toSeq)
  }
}
