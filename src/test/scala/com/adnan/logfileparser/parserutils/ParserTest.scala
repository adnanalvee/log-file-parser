package com.adnan.logfileparser.parserutils

import com.adnan.logfileparser.parserutils.Parser._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

import org.scalatest._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import com.github.mrpowers.spark.fast.tests.DataFrameComparer

class ParserTest
    extends FlatSpec
    with PrivateMethodTester {

  it should "correctly extract values from 'REQUEST' field" in {
    println(retrieveValuesFromRequest("method=xcv&page=mcv"))
    assert(retrieveValuesFromRequest("method=xcv&page=mcv") === ("xcv", "mcv"))
    assert(retrieveValuesFromRequest("method=xcv&page=") === ("xcv", ""))
    assert(retrieveValuesFromRequest("method=xcv") === ("xcv", ""))
    assert(retrieveValuesFromRequest("page=mcv") === ("", "mcv"))
    assert(retrieveValuesFromRequest("sage=xyz") === ("", ""))
    assert(retrieveValuesFromRequest("") === ("", ""))
    assert(retrieveValuesFromRequest(null) === ("", ""))
    assert(retrieveValuesFromRequest("method=xcv") != ("", ""))
    assert(retrieveValuesFromRequest("page=mcv") != ("mcv", ""))
    assert(retrieveValuesFromRequest("sage=mcv") != ("mcv", "mcv"))
   
  }

  it should "correctly extract values from 'HEADERS' field" in {
    val args_1 = "host=opusmobiled12.Code.net&connection=keep-alive&user-agent=ipad"
    assert(retrieveValuesFromHeaders(args_1) === ("opusmobiled12.Code.net", "ipad"))
    assert(retrieveValuesFromHeaders("host=") === ("", ""))
    assert(retrieveValuesFromHeaders("") === ("", ""))
    assert(retrieveValuesFromHeaders(null) === ("", ""))
    assert(retrieveValuesFromHeaders("") != null)
  }

  it should "correctly extract values from 'ARGS' field" in {
    val omReqId1 = "_om_req_id=0489%3A22%3Acw371s%3A4026104%3Aajax"
    val args_1 = "accountNumber=123&" + omReqId1 + "&emailId=xyz%2540yahoo.com&notes=fraud"
    val args_2 = "accountNumber=123&emailId=xyz%2540yahoo.com"
    val args_3 = omReqId1 + "&accountNumber=123&"
    val args_4 = "accotNumber=123&emlId=xyz%2540yahoo.com"
    val args_5 = "accountNumber=&_om_req_id=&emailId&"
    val args_6 = "emailId.emailId=xyz%2540yahoo.com"
    val args_7 = "emailAddress=xyz%2540yahoo.com"
    // A valid return stored inside a variable
    var validExtractedTuple = ("123", "cw371s", "xyz@yahoo.com", "fraud")
    assert(retrieveValuesFromArgs(args_1) === validExtractedTuple)
    assert(retrieveValuesFromArgs(args_2) === ("123", "", "xyz@yahoo.com", ""))
    assert(retrieveValuesFromArgs(args_3) === ("123", "cw371s", "", ""))
    assert(retrieveValuesFromArgs("") === ("", "", "", ""))
    assert(retrieveValuesFromArgs(null) === ("", "", "", ""))
    assert(retrieveValuesFromArgs(args_5) === ("", "", "", ""))
    assert(retrieveValuesFromArgs(args_6) === ("", "", "xyz@yahoo.com", ""))
    assert(retrieveValuesFromArgs(args_7) === ("", "", "xyz@yahoo.com", ""))
    assert(retrieveValuesFromArgs(args_4) != ("123", "", "xyz@yahoo.com", ""))
    assert(retrieveValuesFromArgs("") != (null, null, null, null))

  }

  it should "correctly extract values from 'COOKIE' field" in {
    val args_1 =
      """JSESSIONID=RSLTZ3LDxhvyQb153fhzyddpfh5vJzvz15sYy9wwns
      qhJP0FzmzK!366767447& REGISTER_HOSTNAME=ede5848634861c9bdea4ea011be23
      1f5150d0052& OPUSM_NETC_AUTH_KEY=qsk54uDKFJ90&eshr=DONOVAN%257cDALY
      %257cdd641q%2540us%252eCode%252ecom%257c5072884021%257c%257ckd484v%257c
      %257cdd641q%252cRBBHRHH%252cZKFF7G6%252c7119481%257cNNNNYNNNNNNNNNNNN
      YNNNNNN%257cDONOVAN%257cXCW921002%257c1723asdfjlksdaf20980D"""
        .replaceAll("\n", "")
        .replaceAll("\\s", "")
    val valid_eshr =
      """DONOVAN|DALY|dd641q@us.Code.com|5072884021||kd484v|
    |dd641q,RBBHRHH,ZKFF7G6,7119481|NNNNYNNNNNNNNNNNNYNNNNNN|DONOVAN
    |XCW921002|1723asdfjlksdaf20980D"""
        .replaceAll("\n", "")
        .replaceAll("\\s", "")
    val valid_JESSIONID =
      "RSLTZ3LDxhvyQb153fhzyddpfh5vJzvz15sYy9wwnsqhJP0FzmzK!366767447"
    val args_2 = ""
    val args_3 = ""
    assert(retrieveFromCookie(args_1) === (valid_JESSIONID, valid_eshr))
    assert(retrieveFromCookie("JSESSIONID=X") === ("X", ""))
    assert(retrieveFromCookie("eshr=Y") === ("", "Y"))
    assert(retrieveFromCookie("") === ("", ""))
    assert(retrieveFromCookie(null) === ("", ""))
    assert(retrieveFromCookie("JSESSIONID=X&CodeESHr=Y") != ("", ""))
    assert(retrieveFromCookie("") != (null, null))
  }

  it should "correctly extract values from 'TCPXN' field" in {
    val args_1 = """clientIp=135.28.31.92&serverIp=135.31.236.119&
      clientPort=44592&serverPort=8443&pcapTick100us=15093951592865&
      clientTcpTick=4269517543&serverTcpTick=1043310476"""
      .replaceAll("\n", "")
      .replaceAll("\\s", "")
    val validExtractedTuple =
      ("135.28.31.92", "135.31.236.119", "44592", "8443")
    val args_2 = "clientIp=&serverIp=&clientPort=&serverPort="
    val args_3 = "clientIp=135.28.31.92"
    val args_4 = "serverIp=135.31.236.119"
    val args_5 = "clientPort=44592"
    val args_6 = "serverPort=8443"
    val args_7 = "clientIp=1&serverIp=2&clientPort=3&serverPort=4"
    assert(retrieveFromTcpcxn(args_1) === validExtractedTuple)
    assert(retrieveFromTcpcxn(args_2) === ("", "", "", ""))
    assert(retrieveFromTcpcxn(args_3) === ("135.28.31.92", "", "", ""))
    assert(retrieveFromTcpcxn(args_4) === ("", "135.31.236.119", "", ""))
    assert(retrieveFromTcpcxn(args_5) === ("", "", "44592", ""))
    assert(retrieveFromTcpcxn(args_6) === ("", "", "", "8443"))
    assert(retrieveFromTcpcxn("") === ("", "", "", ""))
    assert(retrieveFromTcpcxn(null) === ("", "", "", ""))
    assert(retrieveFromTcpcxn(args_7) != ("", "", "", ""))
    assert(retrieveFromTcpcxn("") != (null, null, null, null))
  }

  it should "extract value from the 2nd index" in {
    assert(extractValueBySingleSplit("key=value") === "value")
    assert(extractValueBySingleSplit("key=") === "")
    assert(extractValueBySingleSplit("") === "")
    assert(extractValueBySingleSplit(null) === "")
    assert(extractValueBySingleSplit("key=value") != "vaslue")
    assert(validateEmail(null) != null)
  }

  it should "extract email address by replacing %2540 for '@' sign" in {
    assert(
      validateEmail("email=customer1%2540gmail.com") === "customer1@gmail.com")
    assert(validateEmail("email=") === "")
    assert(validateEmail("") === "")
    assert(validateEmail(null) === "")
    assert(validateEmail("email=customer1@gmail.com") === "customer1@gmail.com")
    assert(validateEmail("email=thisIsNotanEmail") != "")
    assert(
      validateEmail("emailcustomer1%2540gmail.com") != "customer1@gmail.com")
    assert(validateEmail(null) != null)
  }

  it should "decode a string twice to extract the value" in {
    val decodeEshr =
      """LISA%257cSTONE%257cls458g%2540us%252eCode%252ecom
       %257c7275124674%257c%257ccm946g%257c%257cls458g%252cRGWKLKF%252cH3
       FWXB6%252c6857755%257cNNNNNYNNNNNNNNNNNYNNNNNN%257cLISA%257cXCW925
       036%257c0"""
        .replaceAll("\n", "")
        .replaceAll("\\s", "")
    val encodedEshr =
      """LISA|STONE|ls458g@us.Code.com|7275124674||cm946g||
      ls458g,RGWKLKF,H3FWXB6,6857755|NNNNNYNNNNNNNNNNNYNNNNNN|
      LISA|XCW925036|0"""
        .replaceAll("\n", "")
        .replaceAll("\\s", "")

    assert(decodeTwice(decodeEshr) === encodedEshr)
    assert(decodeTwice("") === "")
    assert(decodeTwice(null) === "")
    assert(decodeTwice(null) != null)
  }

  it should "correctly validate CodeUID from string" in {
    val omReqId1 = "_om_req_id=0489%3A22%3Acw371s%3A4026104%3Aajax"
    val omReqId2 = "_om_req_id=0701:34:mh481u:1908968448"
    val correctCodeUID1 = "cw371s"
    val correctCodeUID2 = "mh481u"
    assert(validateCode(omReqId1) === correctCodeUID1)
    assert(validateCode(omReqId2) === correctCodeUID2)
    assert(validateCode("_om_req_id=") === "")
    assert(validateCode("") === "")
    assert(validateCode("_om_req_id=BLANK:231070220") === "")
    assert(validateCode("_om_req_id=BLANK%231070220") === "")
    assert(validateCode("_om_req_id=4769:39:sl614f:") === "sl614f")
    assert(validateCode(null) === "")
    assert(validateCode(null) != null)

  }

  
}
