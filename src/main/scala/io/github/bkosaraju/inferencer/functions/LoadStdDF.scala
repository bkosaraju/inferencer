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

//import org.apache.calcite.avatica.ColumnMetaData
import org.apache.spark.sql.{DataFrame, types}
import org.apache.spark.sql.types.{StructField, StructType}

trait LoadStdDF
  extends IsSchemaSame
    with Session
    with Exceptions {
  /**
    * loads the schema driven files into a dataframe.
    * @param path - path of the files
    * @param readerFormat reader format such as csv, orc, com.databricks.avro, parquet
    * @param readerOptions any options that should be passed to low level spark api to read the data such as skipthe header or any other formats.
    * @param recordSchema Input record schema incase of schema infer data files.
    * @return Data from with source data.
    */
  def loadStdDF(path: String, readerFormat: String, readerOptions: Map[String, String], recordSchema: StructType, conRecordSchema : StructType = null): DataFrame = {
    try {
      if (Seq("csv", "com.databricks.spark.csv").contains(readerFormat.toLowerCase)) {
        if ( readerOptions.contains("header")  && readerOptions("header").equals("true") &&  readerOptions.contains("useHeader") && readerOptions("useHeader").equals("true")) {
          sparkSession.read.format(readerFormat).options(readerOptions).load(path)
        } else {
          sparkSession.read.format(readerFormat).schema(recordSchema).options(readerOptions).load(path)
        }
      } else if (Seq("cobol","ebcdic").contains((readerFormat.toLowerCase()))) {
        if  ( Seq("copybook_contents","copybook").intersect(readerOptions.keySet.toSeq).isEmpty) {
          throw new InvalidCopyBookOption("it is mandatory to pass copybook/copybook_content option as part of cobol parser")
        } else {
          sparkSession
            .read
            .format("cobol")
            .option("schema_retention_policy",readerOptions.getOrElse("schema_retention_policy","collapse_root"))
            .options(readerOptions)
            .load(path)
        }
      } else {
        val srcDF = sparkSession.read.format(readerFormat).options(readerOptions).load(path)
        if (Seq("json","xml","com.databricks.spark.xml").contains(readerFormat.toLowerCase) && readerOptions.contains("inferTargetSchema") && readerOptions("inferTargetSchema").equals("true")) {
          val additionalSchema = StructType(
              conRecordSchema.map(x => StructField(x.name.toLowerCase,x.dataType,x.nullable)) diff
                srcDF.schema.map(x => StructField(x.name.toLowerCase,x.dataType,x.nullable))
              )
          val tRecSchema = StructType(additionalSchema union StructType(
            srcDF.schema.map(x => StructField(x.name.toLowerCase,x.dataType,x.nullable)).filter(x => ! additionalSchema.map(_.name).contains(x.name))
          )
          )
          sparkSession.read.format(readerFormat).schema(tRecSchema).options(readerOptions).load(path)
        } else if (Seq("json","xml","com.databricks.spark.xml","cobol","ebcdic").contains(readerFormat.toLowerCase)){
          srcDF
        } else {
          val srcSchema = srcDF.schema
          if (isSchemaSame(srcSchema, conRecordSchema)) {
            srcDF
          } else {
            logger.error("Source and Target Schemas are differing \nSource Schema :\n" + srcSchema.mkString("\n")
              + "\nTarget Schema:\n" + recordSchema.mkString("\n"))
            throw new SchemaMismatchException("Source and Target Schemas are differing \nSource Schema :\n" + srcSchema.mkString("\n")
              + "\nTarget Schema:\n" + recordSchema.mkString("\n"))
          }
        }
      }
    } catch {
      case e: Throwable =>
        logger.error("Unable to Load Dataframe From Location:" + path, e)
        throw e
    }
  }
}
