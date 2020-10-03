/*
 *  Copyright (C) 2019-2020 bkosaraju
 *  All Rights Reserved.
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package io.github.bkosaraju.inferencer

import java.util.Properties
import org.apache.hadoop.fs.Path
import collection.JavaConverters._
import io.github.bkosaraju.utils.spark.DataEncryptor._

class LoadCuratedData() extends AppFunctions {
  var edwsCols : String= _
  def loadCuratedData(props: Properties): Unit = {

    val instanceId = getRunId
    logger.info(s"Using instanceId : ${instanceId}")
    val landingURI = new Path(props.getProperty("dataSourceURI") + "/" + props.getProperty("dataSet") + LANDING_DIR)
    val processingURI = new Path(props.getProperty("dataSourceURI") + "/" + props.getProperty("dataSet") + PROCESSING_DIR + "/" + instanceId)
    val archiveURI = new Path(props.getProperty("dataSourceURI") + "/" + props.getProperty("dataSet") + s"${ARCHIVE_DIR}/${instanceId.substring(0, 4)}/${instanceId.substring(4, 6)}/${instanceId.substring(6, 8)}")

      if(props.containsKey("extraDWSColumns")) {
        edwsCols = props.getProperty("extraDWSColumns","")
        props.keySet().asScala.toList.asInstanceOf[List[String]].foreach(r => edwsCols=edwsCols.replaceAll(s"#${r}#",props.getProperty(r)))
        props.setProperty("dwsVars",
          Array(props.getProperty("dwsVars", ""),edwsCols).filter(_.nonEmpty).mkString(",")
        )
      }

    val srcPath =
      if (props.getProperty("runId").equalsIgnoreCase(MANAGED_PROVIDER)) {
        moveData(landingURI, processingURI,
          props.getProperty("sourceFilePattern", ".*")
        )
        processingURI.toString
      } else {
        props.getProperty("dataSourceURI") + "/" +
          props.getProperty("dataSet") + "/" +
          props.getProperty("runId")
      }

    logger.info(s"""started processing files in ${srcPath}""")

    try {
      val writeMode = props.getProperty("writeMode", "append")
      val readerFormat = props.getProperty("readerFormat", "")
      val filePattern = props.getProperty("fileBasedTagPattern", "")
      val taggedColumns = props.getProperty("fileBasedTagColumns", "").split(",").map(_.toLowerCase)
      val sourceDataSets = getLatestDataSets(srcPath, readerFormat, writeMode)

      logger.info(s"""listed datasets for processing :${sourceDataSets.mkString("\n")}""")

      for (srcFile <- sourceDataSets) {

        logger.info(s"pickup the file ${srcFile} for processing..")

        val readerOptions = stringToMap(props.getProperty("readerOptions", ""))
        val headerMap = if (props.getProperty("headerFormat", "").nonEmpty) {
          headerToColumns(srcFile, props)
        } else Map[String, String]()

        val headerdwsVars = (for (k <- headerMap.keys) yield k + "=" + headerMap(k)
          .replaceAll(",", "\u0001")
          .replaceAll("=", "\u0002"))
          .mkString(",")
        val passedDwsVars =
          if (headerdwsVars.trim.nonEmpty && props.getProperty("dwsVars", "").equals("")) {
            headerdwsVars
          } else if (headerdwsVars.trim.isEmpty) {
            props.getProperty("dwsVars", "")
          } else {
            props.getProperty("dwsVars", "") + "," + headerdwsVars
          }

        if (filePattern.nonEmpty && taggedColumns.nonEmpty) {
          props.remove("dwsVars")
          val taggedProps = extractTaggedColumns(srcFile, filePattern, taggedColumns)
          val taggedVars = (for (k <- taggedProps.keys) yield k + "=" + taggedProps(k)).mkString(",")
          val updDwsVars =
            if (passedDwsVars
              .trim
              .replaceAll("^\"(.*)\"$", "$1")
              .trim
              .isEmpty) {
              taggedVars
            } else {
              passedDwsVars + "," + taggedVars
            }
          props.setProperty("dwsVars", updDwsVars)
        } else {
          props.setProperty("dwsVars", passedDwsVars)
        }
        val curatedData = getCuratedDF(srcFile, props)
        val srcCuratedDF = curatedData(0)
        val confDF =
          srcToTgtColRename(
            srcCuratedDF,
            stringToMap(props.getProperty("srcToTgtColMap", ""))
          )

        val targetDF =
          if (props.containsKey("encryptionKey") && props.containsKey("encryptionColumns")) {
            logger.info("proceeding to perform column level encryption ..")
            confDF.encrypt(props.getProperty("encryptionKey"), props.getProperty("encryptionColumns").split(","): _*)
          } else {
            logger.info("encryptionKey/encryptionColumns[both must] not provided hence skipping data column encryption..")
            confDF
          }

        val curatedDF = {
            if (props.getProperty("generateRowHash","").equalsIgnoreCase("true")) {
              targetDF.genHash(
                props.getProperty("srcSchemaColumns", "").split(","),
                props.getProperty("generateRowHashColumn","row_hash")
              )
            } else {
                targetDF
              }
        }



        if ( props.getProperty("targetDatabase","").nonEmpty && props.getProperty("targetTable","").nonEmpty) {
          var dbtable = props.getProperty("targetDatabase") + "." + props.getProperty("targetTable")
          val writerOptions = stringToMap(
            props.getProperty("writerOptions", "")
          )
          val targetOptions =
          if (props.getProperty("writerFormat","").isEmpty) {
            writerOptions
          } else if (props.getProperty("writerFormat").toLowerCase.contains("snowflake")) {
            dbtable = props.getProperty("targetTable")
            var tOps: collection.mutable.Map[String,String]= collection.mutable.Map[String,String]() ++ writerOptions
            tOps.put("sfDatabase" , props.getProperty("targetDatabase"))
            tOps.put("writerFormat", props.getProperty("writerFormat"))
            props.stringPropertyNames().asScala
              .filter( key => key.matches("^sf.*"))
              .map(k => tOps.put(k,props.getProperty(k)))
            tOps.toMap
          }
          else {
            writerOptions ++ Map("writerFormat" -> props.getProperty("writerFormat"))
          }
          writeDataFrame(curatedDF, dbtable, writeMode, targetOptions)
        } else if (props.getProperty("targetURI","").nonEmpty) {
          writeDataFrame(curatedDF,
            props.getProperty("targetURI")
              .replaceAll("[sS]3://","s3a://")   //HDFS compatible URI
              .replace("/$", "") +
              "/" +
              props.getProperty("targetKey", props.getProperty("dataSet", "")),
            props.getProperty("writerFormat",WRITER_FORMAT),
            writeMode,
            stringToMap(
              props.getProperty("writerOptions", "")
            ),
            props.getProperty("targetPartition","")
          )
        } else {
          throw new IllegalArgumentException("targetDatabase,targeTable or targeURI,[targetKey] parameter must be specified to write data")
        }
        if (props.getProperty("errorThresholdPercent", "").nonEmpty) {
          try {
            val invalidDataDF = curatedData(1)
            val errorDataFormat = {
              if (Seq("jdbc", "rdbms", "snowflake", "net.snowflake.spark.snowflake")
                .contains(props.getProperty("writerFormat", WRITER_FORMAT).toLowerCase)
              ) {
                props.getProperty("errorDataFormat", WRITER_FORMAT)
              } else {
                props.getProperty("errorDataFormat", props.getProperty("writerFormat", WRITER_FORMAT))
              }
            }
            writeDataFrame(invalidDataDF,
              props.getProperty("targetURI", props.getProperty("dataSourceURI"))
                .replaceAll("[sS]3://","s3a://")
                .replace("/$", "") + "/" +
                props.getProperty("targetKey", props.getProperty("dataSet", "")) + ".rejected",
              errorDataFormat, "append", Map()
            )
          } catch {
            case e: Throwable =>
              logger.error("Unable to Write the Error Records into Error Bucket ", e)
              //throw e  //Enable to fail job if error are not loading into error bucket
          }
        }
        if (props.getProperty("runId").equalsIgnoreCase(MANAGED_PROVIDER)) {
          moveData(new Path(srcFile.replaceAll("s3a://","s3://")),
            new Path(
            srcFile
              .replaceAll("s3a://","s3://")
              .replaceAll(
              s"${PROCESSING_DIR}/${instanceId}",
              s"${ARCHIVE_DIR}/${instanceId.substring(0, 4)}/${instanceId.substring(4, 6)}/${instanceId.substring(6, 8)}"
            ))
          )
        } else {
          archiveFiles(srcFile, srcPath, writeMode)
        }
      }

      logger.info(
        s"""Final cleanup : archiving datasets
           |into: ${ARCHIVE_DIR} from: ${PROCESSING_DIR}
           |""".stripMargin)
      if (props.getProperty("runId").equalsIgnoreCase(MANAGED_PROVIDER)) {
        moveData(processingURI, archiveURI)
      }
    } catch {
      case e: Exception => {
        logger.info("rolling back data due to exception in processing..")
        if (props.getProperty("runId").equalsIgnoreCase(MANAGED_PROVIDER)) {
          moveData(processingURI, landingURI)
        }
        throw e
      }
    }
  }
}
