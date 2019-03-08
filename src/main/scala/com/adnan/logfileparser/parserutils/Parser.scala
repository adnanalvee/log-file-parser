package com.adnan.logfileparser.parserutils
/* *
 * Making things immutable in version 2.0
 */

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window

import org.apache.log4j.LogManager
import org.apache.log4j.Level

import java.net.{URLDecoder, URLEncoder}

object Parser {

  /**
    * Parses string and splits by '=' to map in key, value pairs.
    * Example: The string "ip=101&port=4" would look like Map((ip, 101), (port, 4))
    *
    * @param values Multiple key, value string corresponding to the field "TCPCXN" in the data
    * @return Four tuples containing ip and port addresses of client and server.
    *         Each of the four tuples can return empty string if not found.
    */
  def valueMapper(values: String): Map[String, String] = {
    if (values.contains("=")) {
      values
        .split("&", -1)
        .flatMap { x =>
          x.trim.split("=")(0) match {
            case key: String => Some(key -> extractValueBySingleSplit(x))
            case _           => None
          }
        }
        .toMap
    } else Map[String, String]()
  }

  /**
    * Parse required values from REQUEST field.
    *
    * @param values Multiple key, value string corresponding to the field "HEADERS" in the data
    * @return Method and page values.
    *         Returns an empty string if corresponding value/s are not found.
    */
  def retrieveValuesFromRequest(values: String): (String, String) = {
    if (values != null) {
      val valueMap = valueMapper(values)
      (
        valueMap.getOrElse("method", ""),
        valueMap.getOrElse("page", "")
      )
    } else ("", "")
  }

  /**
    * Parse required values from HEADERS field.
    *
    * @param values Multiple key, value string corresponding to the field "HEADERS" in the data
    * @return host (weblink) in string
    *         Returns an empty string if corresponding value/s are not found.
    */
  def retrieveValuesFromHeaders(values: String): (String, String) = {
    if (values != null) {
      val valueMap = valueMapper(values)
      (
        valueMap.getOrElse("host", ""),
        valueMap.getOrElse("user-agent", "")
      )
    } else ("", "")  
  }

  /**
    * Parse required values from ARGS field.
    *
    * @param values Multiple key, value string corresponding to the field "ARGS" in the data
    * @return Values extracted from keys "wc/accountNumber", "_om_req_id" and "emailId".
    *         Returns an empty string if corresponding value/s are not found.
    */
  def retrieveValuesFromArgs(
      values: String): (String, String, String, String) = {
    if (values != null) {
    val valueMap = if (values.contains("=")) {
      values
        .split("&", -1)
        .flatMap { x =>
          val key = x.trim.split("=", -1)(0) 
          key match {
            case "accountNumber" | "wcAccountNumber" =>
              Some("account_number" -> extractValueBySingleSplit(x))
            case "_om_req_id" => Some("om_req_id" -> validateCode(x))
            case "emailId" | "emailId.emailId" | "emailAddress" =>
              Some("customer_email" -> validateEmail(x))
            case "notes" =>
              Some("notes" -> decodeTwice(extractValueBySingleSplit(x)))
            case key: String => 
            if (key.toLowerCase.contains("email")) {
              val str = x.trim.split("=", -1)(1)
              if (!str.isEmpty && str.contains("%2540"))
               Some("customer_email" -> validateEmail(x))
               else None
            } else None
            case _ => None
          }
        }
        .toMap
    } else Map[String, String]()
    

    (
      valueMap.getOrElse("account_number", ""),
      valueMap.getOrElse("om_req_id", ""),
      valueMap.getOrElse("customer_email", ""),
      valueMap.getOrElse("notes", "")
    )
    } else ("", "", "", "")
  }

  /**
    * Retrieve values from COOKIE field as needed.
    *
    * @param values Multiple key, value string corresponding to the field "COOKIE" in the data
    * @return Values extracted from keys "JSESSIONID", "eshr".
    *         Returns an empty string if corresponding value/s are not found or is null.
    */
  def retrieveFromCookie(values: String): (String, String) = {
    if (values != null) {
      val valueMap = if (values.contains("=")) {
        values
          .split("&", -1)
          .flatMap { x =>
            val key = x.trim.split("=", -1)(0)
            key match {
              case "JSESSIONID" =>
                Some("JSESSIONID" -> extractValueBySingleSplit(x))
              case "eshr" =>
                Some("eshr" -> decodeTwice(extractValueBySingleSplit(x)))
              case _ => None
            }
          }
          .toMap
      } else Map[String, String]()

      (
        valueMap.getOrElse("JSESSIONID", ""),
        valueMap.getOrElse("eshr", "")
      )
    } else ("", "")
  }

  /**
    * Parse IP and port addresses of server and client
    *
    * @param values Multiple key, value string corresponding to the field "TCPCXN" in the data
    * @return Four tuples containing ip and port addresses of client and server.
    *         Each of the four tuples can return empty string if not found.
    */
  def retrieveFromTcpcxn(values: String): (String, String, String, String) = {
    if (values != null) {
    val valueMap = valueMapper(values)
    (
      valueMap.getOrElse("clientIp", ""),
      valueMap.getOrElse("serverIp", ""),
      valueMap.getOrElse("clientPort", ""),
      valueMap.getOrElse("serverPort", "")
    )
    }  else ("", "", "", "")
  }

  /**
    * Function to extract values from String by one split
    * Example: Passing in a string"key=value" will return string "value"
    *
    * @param value String to be parsed
    * @param index To retrieve value at index 1
    * @return Extracted value, empty string if not found.
    */
  def extractValueBySingleSplit(value: String): String = {
    scala.util.Try(value.trim.split("=", -1)(1)).getOrElse("")
  }

  /**
    * Decodes a value twice to extract data.
    * Used to decode value from "esh" key from the field COOKIE
    *
    * @param value String to be parsed
    * @return Pipe delimited values containing employee information
    */
  def decodeTwice(value: String): String = {
    scala.util.Try(URLDecoder.decode(URLDecoder.decode(value))).getOrElse("")
  }

  /**
    * Extract email address to clean up the email address value.
    *
    * @param values Multiple key, value string corresponding to the field "ARGS" in the data
    * @return An email address. Empty string if not found
    *
    */
  def validateEmail(emailAddress: String): String = {
    val emailAddr =
      scala.util.Try(emailAddress.trim.split("=", -1)(1)).getOrElse("")
    if (emailAddr.contains("%2540")) emailAddr.replaceAll("%2540", "@")
    else emailAddr
  }

  /**
    * Extracts code from a string by decoding.
    *
    * @param values A key, value string corresponding to the field "ARGS" in the data
    * @return code
    */
  def validateCode(values: String): String = {
    val value = scala.util.Try(values.trim.split("=", -1)(1)).getOrElse("")
    if (value.contains("%")) {
      val decodedVal: String = URLDecoder.decode(value, "UTF-8")
      if (decodedVal.contains(":")) decodedVal.split(":", -1)(2)
      else if (decodedVal.contains("%")) decodedVal.split("%3A", -1)(2)
      else ""
    } else if (value.contains(":") && !value.toLowerCase.contains("blank"))
      value.split(":", -1)(2)
    else ""
  }

  /**
    * Parse each (field, value) Seqeunce and extract new columns.
    *
    * @param data Contains columns "field" and "values" in a sequence
    * @return A sequence of columns that contains the required values after parsing.
    *         If no values were found after parsing, the column values would be an
    *         empty string.
    */
  def parseFieldValues(data: Seq[Row]): Seq[Any] = {
    val dataMapper = data.flatMap { elem =>
      val field = elem(0).toString.trim
      val values = elem(1).toString.trim
      field match {
        case "REQUEST" =>
          val (method, page) = retrieveValuesFromRequest(values)
          Iterator("method" -> method, "page" -> page)
        case "HEADERS" =>
          val (host, user_agent) = retrieveValuesFromHeaders(values)
          Iterator("host" -> host, "user_agent" -> user_agent)
        case "BARS" =>
          val (jsession_id, eshr) = retrieveFromCookie(values)
          Iterator("jsession_id" -> jsession_id, "eshr" -> eshr)
        case "TCP" =>
          val (client_ip, client_port, server_ip, server_port) =
            retrieveFromTcpcxn(values)
          Iterator("client_ip" -> client_ip,
                   "client_port" -> client_port,
                   "server_ip" -> server_ip,
                   "server_port" -> server_port)
        case "PARAMS" =>
          val (account_number, om_req_id, customer_email_address, notes) =
            retrieveValuesFromArgs(values)
          Iterator("customer_email_address" -> customer_email_address,
                   "om_req_id" -> om_req_id,
                   "account_number" -> account_number,
                   "notes" -> notes)
        case "AGENT" =>
          if (values.length > 1) Some("user_agent_changed" -> values)
          else Some("user_agent_changed" -> "")
        case "XDATA" =>
          if (values.length > 1) Some("sdata" -> values) else Some("sdata" -> "")
        case _ => None
      }
    }.toMap


    Seq(
      dataMapper.getOrElse("method", ""),
      dataMapper.getOrElse("page", ""),
      dataMapper.getOrElse("host", ""),
      dataMapper.getOrElse("jsession_id", ""),
      dataMapper.getOrElse("eshr", ""),
      dataMapper.getOrElse("client_ip", ""),
      dataMapper.getOrElse("client_port", ""),
      dataMapper.getOrElse("server_ip", ""),
      dataMapper.getOrElse("server_port", ""),
      dataMapper.getOrElse("customer_email_address", ""),
      dataMapper.getOrElse("om_req_id", ""),
      dataMapper.getOrElse("account_number", ""),
      dataMapper.getOrElse("notes", ""),
      dataMapper.getOrElse("user_agent", ""),
      dataMapper.getOrElse("user_agent_changed", ""),
      dataMapper.getOrElse("sdata", "")
    )
  }
}
