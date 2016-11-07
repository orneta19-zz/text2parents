package text2parents
import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.io.Source
import scala.util.matching.Regex
/**
  * Created by Neta on 09/04/2016.
  */
object parentsByTweet {


    val logger: Logger = LoggerFactory.getLogger(this.getClass)

    def extractTweet(rawdata: JsonNode, tweetParentsRegexList: List[Regex], tweetFemaleRegexList: List[Regex], tweetMaleRegexList: List[Regex]): ((Boolean, String), (String, String)) = {
      if (rawdata == null) return null
      if (!rawdata.has("agent") || !rawdata.has("data") || !rawdata.path("data").has("text") ||
        !rawdata.path("agent").asText().equals("twitter"))
        return  null

      val tweet = rawdata.path("data").path("text").asText()

      if ((tweet.contains("porn") || tweet.contains("\"")) ||
        (tweet.contains("RT ") || tweet.contains("RT:") || tweet.startsWith("RT")) ||
        (tweet.contains(0x201C.toChar) || tweet.contains(0x201D.toChar)) ||
        (tweet.contains(0x201E.toChar) || tweet.contains(0x201F.toChar)) ||
        (tweet.contains(0x301D.toChar) || tweet.contains(0x301E.toChar)) ||
        (tweet.contains(0x301F.toChar) || tweet.contains(0xFF02.toChar)) ||
        tweet.filter(_=='-').length>3
      )
        return null

      val tweetLowerCase = tweet.toLowerCase

      val isParentRegex = tweetParentsRegexList.map(elem => elem.findFirstIn(tweetLowerCase))
        .map(_.getOrElse("")).filter(_.nonEmpty).headOption.getOrElse("")

      val isParent_res = if (isParentRegex.nonEmpty) (true, isParentRegex) else (false, null)

      val femaleRegex =  if (isParent_res._1 == true) tweetFemaleRegexList.map(elem => elem.findFirstIn(tweetLowerCase))
        .map(_.getOrElse("")).filter(_.nonEmpty).headOption.getOrElse("") else ""

      val maleRegex = if (isParent_res._1 == true) tweetMaleRegexList.map(elem => elem.findFirstIn(tweetLowerCase))
        .map(_.getOrElse("")).filter(_.nonEmpty).headOption.getOrElse("") else ""

      val gender_res = if (femaleRegex.nonEmpty) ("female", femaleRegex) else
      if (maleRegex.nonEmpty) ("male", maleRegex) else ("unknown", null)


      (isParent_res, gender_res)
    }


    def createParentJobResult(isParent: Boolean, explained: String): UserDataJobResult = {
      val feature = UserDataConstants.FeatureParentName
      val name = "isParentByTweet"
      val version = "1.0"
      val confidence = 0.93
      val explain: mutable.Map[String, Any] = mutable.Map("RegexMatched" -> explained)
      val result = isParent

      val jobResult = new UserDataJobResult(feature = feature, name = name, version = version, confidence = confidence,
        explain = explain, result = result, extraProperties = null)
      jobResult.addExtraProperties("usingRawData", "true")

      return jobResult
    }

    def createGenderJobResult(gender: String, explained: String): UserDataJobResult = {
      val feature = UserDataConstants.FeatureGenderName
      val name = "genderByTweet"
      val version = "1.0"
      val confidence = 0.95
      val explain: mutable.Map[String, Any] = mutable.Map("RegexMatched" -> explained)
      val result = gender

      val jobResult = new UserDataJobResult(feature = feature, name = name, version = version, confidence = confidence,
        explain = explain, result = result, extraProperties = null)
      jobResult.addExtraProperties("usingRawData", "true")

      return jobResult
    }

    def run(allRawData: RDD[String], isCluster: Boolean = true): RDD[UserDataObject] = {
      val tweetParentsRegexList = Source.fromURL(getClass.getResource("/tweet/tweetparentsList")).getLines().filter(_.nonEmpty).map(_.r).toList
      val tweetFemaleRegexList = Source.fromURL(getClass.getResource("/tweet/tweetfemaleList")).getLines().filter(_.nonEmpty).map(_.r).toList
      val tweetMaleRegexList = Source.fromURL(getClass.getResource("/tweet/tweetmaleList")).getLines().filter(_.nonEmpty).map(_.r).toList

      allRawData.map(rawdata => (rawdata, extractTweet(JsonParser.fromJson(rawdata), tweetParentsRegexList, tweetFemaleRegexList, tweetMaleRegexList)))
        .filter(_._2 != null)
        .filter(_._2._1._1 == true)
        .filter(tup => RawDataUtils.RawDataToUserObject(tup._1) != null)
        .map(elem => (new UserDataObject(jobs = null, tkid = null,
          aliases = RawDataUtils.RawDataToUserObject(elem._1).getAliases,
          extraProperties = null, data = null), createParentJobResult(elem._2._1._1, elem._2._1._2), createGenderJobResult(elem._2._2._1, elem._2._2._2)))
        .map(data_tup => {
          val newUser = new UserDataObject()
          newUser.setAliases(data_tup._1.getAliases)
          newUser.addJob(data_tup._2)
          newUser.addJob(data_tup._3)
          newUser
        })
    }

  }
