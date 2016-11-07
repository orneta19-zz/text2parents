package text2parents

import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.util.matching.Regex
import scala.io.Source

/**
  * Created by Neta on 09/04/2016.
  */
object parentsByBio {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def extractBio(userdata: UserDataObject, bioParentsRegexList: List[Regex], bioFemaleRegexList: List[Regex], bioMaleRegexList: List[Regex]): ((Boolean, String), (String, String)) = {

    val data = userdata.getData
    if (data==null)
      return null
    val twitter: UserDataSource = data.get("twitter").getOrElse(null)
    if (twitter == null)
      return null
    val rawData = twitter.getRawData()
    if (rawData == null)
      return null
    val bio_list = getDescriptionFromRawData(rawData)
    if (bio_list.isEmpty || bio_list == null)
      return null
    val bio = bio_list.reduce(_++_)
    if (bio.contains("porn") || bio.contains("\""))
      return null
    if (bio.filter(_=='-').length>=3)
      return null
    val descriptionLowerCase = bio.toLowerCase()

    val isParentRegex = bioParentsRegexList.map(elem => elem.findFirstIn(descriptionLowerCase))
      .map(_.getOrElse("")).filter(_.nonEmpty).headOption.getOrElse("")

    val isParent_res = if (isParentRegex.nonEmpty) (true, isParentRegex) else (false, null)

    val femaleRegex = if (isParent_res._1 == true) bioFemaleRegexList.map(elem => elem.findFirstIn(descriptionLowerCase))
      .map(_.getOrElse("")).filter(_.nonEmpty).headOption.getOrElse("") else ""

    val maleRegex = if (isParent_res._1 == true) bioMaleRegexList.map(elem => elem.findFirstIn(descriptionLowerCase))
      .map(_.getOrElse("")).filter(_.nonEmpty).headOption.getOrElse("") else ""

    val gender_res = if (femaleRegex.nonEmpty) ("female", femaleRegex) else
    if (maleRegex.nonEmpty) ("male", maleRegex) else ("unknown", null)

    (isParent_res, gender_res)
  }
  def createIsParentJobResult(isParent: Boolean, explained: String): UserDataJobResult = {
    val feature = UserDataConstants.FeatureParentName
    val name = "parentsByBio"
    val version = "1.0"
    val confidence = 0.95
    val explain: mutable.Map[String, Any] = mutable.Map("RegexMatched" -> explained)

    val jobResult = new UserDataJobResult(feature = feature, name = name, version = version, confidence = confidence,
      explain = explain, result = isParent, extraProperties = null)

    return jobResult
  }

  def createGenderJobResult(gender: String, explained: String): UserDataJobResult = {
    val feature = UserDataConstants.FeatureGenderName
    val name = "genderByParentBio"
    val version = "1.0"
    val confidence = 0.95
    val explain: mutable.Map[String, Any] = mutable.Map("RegexMatched" -> explained)
    val result = gender

    val jobResult = new UserDataJobResult(feature = feature, name = name, version = version, confidence = confidence,
      explain = explain, result = result, extraProperties = null)

    return jobResult
  }


  def run(allUserData: RDD[UserDataObject], isCluster: Boolean = true):RDD[UserDataObject] = {

    val bioParentsRegexList = Source.fromURL(getClass.getResource("/bio/bioparentsList")).getLines().filter(_.nonEmpty).map(_.r).toList

    val bioFemaleRegexList = Source.fromURL(getClass.getResource("/bio/biofemaleList")).getLines().filter(_.nonEmpty).map(_.r).toList

    val bioMaleRegexList = Source.fromURL(getClass.getResource("/bio/biomaleList")).getLines().filter(_.nonEmpty).map(_.r).toList

    allUserData
      .map(userdata => (userdata, extractBio(userdata, bioParentsRegexList, bioFemaleRegexList, bioMaleRegexList)))
      .filter(_._2 != null)
      .filter(_._2._1._1 == true)
      .map(elem =>
        (elem._1, createIsParentJobResult(elem._2._1._1, elem._2._1._2), createGenderJobResult(elem._2._2._1, elem._2._2._2)))
      .map(data_tup => {
        val newUser = new UserDataObject()
        newUser.setAliases(data_tup._1.getAliases)
        newUser.addJob(data_tup._2)
        newUser.addJob(data_tup._3)
        newUser
      })

  }




}
