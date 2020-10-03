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

package io.github.bkosaraju.inferencer.functions

import java.util.Properties

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import collection.JavaConverters._

trait GetCuratedDF
  extends StringToMap
    with Session
    with LoadSchema
    with GetStgSchema
    with LoadStdDF
    with LoadDataFile
    with ConvNonStdDateTimes
    with AmendDwsCols
    with GetTargetSelList
    with DataTypeValidation
    with LoadFixedWidthFile
    with GetDttmCols
    with RemoveHeaderAndFooter
    with GetExternalSchema {
  /**
    * Top level method to generate the Curated Dataframe out of given paramaters wich are taken from properties files and arguments.
    *
    * @param sourceFile location of the data to be loaded.
    * @param props      input Propertis
    * @return Curated data frame.
    */
  def getCuratedDF(sourceFile: String, props: Properties ): Array[DataFrame] = {

    val targetDatabase = props.getProperty("targetDatabase", "")
    val targetTable = props.getProperty("targetTable", "")
    val recDelimiter = props.getProperty("recordDelimiter", "\n")
    val fieldDelimiter = props.getProperty("fieldDelimiter", ",")
    val cleanseChar = props.getProperty("cleanseChar", "")
    val readerFormat = props.getProperty("readerFormat", "")
    val readerLayout = props.getProperty("recordLayout", "").replaceAll("\"", "")
    val errorThresholdPercent = props.getProperty("errorThresholdPercent","100").replaceAll("%", "")
    val srcFormats = stringToMap(props.getProperty("srcFormats", "").replaceAll("\"", ""))
      .map( x =>
        x._1.toLowerCase -> x._2
          .replaceAll("\u000E",",")   //escape wildcard char for ,
          .replaceAll("\u000F","=")   //escape wildcard char for =
      )

    val srcdwsVals = stringToMap(props.getProperty("dwsVars", ""))
    logger.info("loading runtime column values : "+srcdwsVals)
    val readerOptions = stringToMap(props.getProperty("readerOptions", "").replaceAll("\"", ""))
    val srttoTgtMap = stringToMap(props.getProperty("srcToTgtColMap", ""))
    try {
      val extractdTgtSchema =
        if (Seq("rdbms","jdbc").contains(props.getProperty("writerFormat", "").toLowerCase)) {
          try {
         loadSchema(targetDatabase, targetTable
          , props.getProperty("writerFormat")
          , stringToMap(
            props.getProperty("writerOptions", "")
          )
        )} catch {
            case e: Exception => {
              logger.warn("Unable to read target hence using schemaFile property to infer schema")
              if (props.getProperty("schemaFile", "").nonEmpty) {
                getExternalSchema(props.getProperty("schemaFile"))
              }
              else throw e
            }
          }
      }  else if (props.getProperty("writerFormat","").toLowerCase.contains("snowflake")) {
          try {
            var dbtable = props.getProperty("targetTable")
            val wOpts = stringToMap(props.getProperty("writerOptions", ""))
            var tOps: collection.mutable.Map[String, String] = collection.mutable.Map[String, String]() ++ wOpts
            tOps.put("sfDatabase", props.getProperty("targetDatabase"))
            tOps.put("writerFormat", props.getProperty("writerFormat"))
            tOps.put("dbtable", dbtable)
            props.stringPropertyNames().asScala
              .filter(key => key.matches("^sf.*"))
              .map(k => tOps.put(k, props.getProperty(k)))
            sparkSession.read.options(tOps.toMap).format(props.getProperty("writerFormat")).load().schema
          } catch {
              case e: Exception => {
                logger.warn("Unable to read target hence using schemaFile property to infer schema")
                if (props.getProperty("schemaFile", "").nonEmpty) {
                  getExternalSchema(props.getProperty("schemaFile"))
                }
                else throw e
              }
            }
        } else if (props.getProperty("schemaFile","").nonEmpty) {
          getExternalSchema(props.getProperty("schemaFile"))
        } else {
        loadSchema(targetDatabase,targetTable)
      }

      val tgtSchema = StructType(extractdTgtSchema.map(x => x.copy(x.name.toLowerCase)))
      val tgttoPropsdwsValMap = srcdwsVals.keys.map(x =>
        x -> tgtSchema.fieldNames(tgtSchema.fieldNames.map(_.toLowerCase).indexOf(x.toLowerCase))).toMap
      val dwsVals = srcdwsVals.map({ case (k :String ,v:String) => tgttoPropsdwsValMap(k) ->
        ( if (v.trim.equalsIgnoreCase("null")) null else v)
        })

      val dwsCols =
        if (props.getProperty("generateRowHash","").equalsIgnoreCase("true")) {
        dwsVals.keys.toSeq :+ props.getProperty("generateRowHashColumn", "row_hash")
      } else {
        dwsVals.keys.toSeq
      }

      val srcSchema = getStgSchema(tgtSchema, dwsCols)(0)
      val srcConvFullSchema = getStgSchema(tgtSchema, dwsCols, srttoTgtMap)(1)
      val srcConvSchema =
        if (props.getProperty("generateRowHash","").equalsIgnoreCase("true")) {
          dropdwsCols(srcConvFullSchema,Seq(props.getProperty("generateRowHashColumn", "row_hash")))
        } else {
          srcConvFullSchema
        }
      props.setProperty("srcSchemaColumns",srcSchema.names.mkString(","))
      val srcDF =
        if (Seq("binary", "text").contains(readerFormat.toLowerCase)) {
          val sRDD =
            loadDataFile(sourceFile, recDelimiter, fieldDelimiter, cleanseChar, readerFormat, readerOptions)
          sparkSession.createDataFrame(sRDD, srcSchema)
        } else if ("fixedwidth".equals(readerFormat.toLowerCase())) {
          val sRDD =
          loadFixedWidthFile(sourceFile, readerLayout, cleanseChar,recDelimiter,readerOptions)
          sparkSession.createDataFrame(sRDD, srcSchema)
        }
      else {
          val srcRawDF = loadStdDF(sourceFile, readerFormat, readerOptions, srcSchema,srcConvSchema)
          if (Seq("json","xml","com.databricks.spark.xml","cobol","ebcdic").contains(readerFormat.toLowerCase)) {
            SchemaFlattener(srcRawDF,props.getProperty("nestedColSep",""))
          } else srcRawDF
        }

      val convSrcDF = convNonStdDateTimes(
                          amendDwsCols(srcDF,dwsVals,tgtSchema)
                          ,getDttmCols(srcConvSchema,srcFormats),srcConvSchema,errorThresholdPercent)

       dataTypeValidation(convSrcDF, srcDF, srcConvSchema, props)
    } catch {
      case e: Throwable =>
        logger.error("Unable to Prepare Target Dataframe ..", e)
        throw e
    }
  }

}
