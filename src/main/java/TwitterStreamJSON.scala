import com.google.gson.Gson
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils


object TwitterStreamJSON {

  def main(args: Array[String]): Unit = {

    val outputDirectory = "C:\\dataForScalaProjects"

    val conf = new SparkConf()
    conf.setAppName("spark-sreaming")
    conf.setMaster("local[*]")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(30))

    // Configure your Twitter credentials
    val apiKey = "Kj3RSuJf9A497P4frKnLGr0DX"
    val apiSecret = "OFZLCakyRlU2GcJSrO5uES3dzgtmVCmZjJU4bRxsAzCPCu5gGz"
    val accessToken = "924169777068822531-eVnwe3T5LU6MLhYqT80ff0TTde7PbXn"
    val accessTokenSecret = "QkGYy1oBk4FhyLyfNNqQQivNbAQotoUkM4BtHsVSMaeUR"

    System.setProperty("twitter4j.oauth.consumerKey", apiKey)
    System.setProperty("twitter4j.oauth.consumerSecret", apiSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    //in order to avoid IO.Exception
    System.setProperty("hadoop.home.dir", "C:\\winutil\\")

    // Create Twitter Stream in JSON
    val tweets = TwitterUtils
      .createStream(ssc, None)
      .map(new Gson().toJson(_))

    val numTweetsCollect = 10000L
    var numTweetsCollected = 0L

    tweets.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0) {
        val outputRDD = rdd.repartition(1)
        outputRDD.saveAsTextFile(outputDirectory+"/tweets_"+ time.milliseconds.toString)
        numTweetsCollected += count
        if (numTweetsCollected > numTweetsCollect) {
          System.exit(0)
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}