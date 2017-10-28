import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkTwitterStreaming {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("spark-sreaming")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(1))

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
    val splitedTweets = tweets.map(s => s.split(" "))
    val filteredHashTags = splitedTweets.map(s => s.filter(s => s.startsWith("#")))

    filteredHashTags.map(a => a.mkString(" ")).print()

    ssc.start()
    ssc.awaitTermination()
  }
}