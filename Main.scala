package text2parents

/**
  * Created by Neta on 09/04/2016.
*/

  import org.apache.spark.rdd.RDD
  import org.joda.time.format.DateTimeFormat
  import org.joda.time.{DateTime, DateTimeZone}
  import org.slf4j.{Logger, LoggerFactory}

  object Main {

    val logger: Logger = LoggerFactory.getLogger(this.getClass)

    val runTime: String = DateTime.now(DateTimeZone.UTC).toString(DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm"))


    def main(args: Array[String]): Unit = {

      val optionsParser: UserDataOptions = new UserDataOptions()
      val options = optionsParser.parseArgs(this.getClass.getName, args)

      logger.info("Running with RawData from location - " + options.getRawDataInputLocation)
      logger.info("Running with UserData from location - " + options.getUserDataInputLocation)
      logger.info("Output of job result - " + options.getJobResultOutputLocation)

      val rawDataRDD = RDDUtils.getRawDataRDD(options)
      val parentsbyTweet: RDD[UserDataObject] = ParentsByTweet.run(rawDataRDD, options.isCluster)

      parentsbyTweet.map(JsonParser.toJson).saveAsTextFile(options.getJobResultOutputLocation + "parentsByTweet/" + runTime)

      val userDataRDD = RDDUtils.getUserDataRDD(options)
      val parentsbyBio: RDD[UserDataObject] = ParentsByBio.run(userDataRDD, options.isCluster)

      parentsbyBio.map(JsonParser.toJson).saveAsTextFile(options.getJobResultOutputLocation + "parentsByBio/" + runTime)
    }
}
