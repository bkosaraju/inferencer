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

import io.github.bkosaraju.utils.aws.AwsUtils
import com.amazonaws.services.s3.AmazonS3URI
import org.apache.hadoop.fs.{FileSystem, Path}

trait GetLatestDataSets extends Session {
  /**
    * Method to get the latest dataset for given directory in case of full-refresh load senario.
    * this method will give back the entire content in case of orc,parquet,avro or append write mode.
    * @param location readerFormat writeMode
    * @return absolute Path
    */
  def getLatestDataSets(location : String, readerFormat : String, writeMode : String ) : Array[String] = {
    /*
  Note : Please note that for the full refresh datasets to pick only te latest files from given input filse for CSV , Text Json
         this specific block which ensure that for the source fils which are not part of
         orc/parquet,avro will pick only the latest partition from the given input files, these files are excuded due to the nature that these will have many part files
         in case of bringing the structured data into platform raw area (orc/parquet/avro), ensure that you overwrite the files in RAW area
   */
    try {
      if (! List("orc", "parquet", "avro", "com.databricks.spark.avro").contains(readerFormat)) {
        if (location.toLowerCase.matches("s3[a]?://.*")) {
          val awsSourceURI = new AmazonS3URI(location.toString)
          val s3Utils = new AwsUtils
          val fileList = s3Utils
            .listS3Objects(awsSourceURI.getBucket, awsSourceURI.getKey)
            .filter(file => ! file.getKey.matches(".*/$"))
            .map(file => "s3a://" + file.getBucketName + "/" + file.getKey).toList.sorted
          if (writeMode.toLowerCase.equals("overwrite")) {
            Array(fileList.max)
          } else {
            fileList.toArray
          }
        } else {
          val hadoopfs: FileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
          val pathStatus = hadoopfs.listStatus(new Path(location))
          if (writeMode.toLowerCase.equals("overwrite")) {
            Array((for (itm <- pathStatus) yield itm.getPath.toString).sorted.max)
          } else {
            val validDataSets = pathStatus.filter(itm => itm.getLen.toInt != 0)
            for (itm <- validDataSets) yield {
              itm.getPath.toString
            }
          }
        }
      } else Array(location)
    } catch {
      case e: Exception =>
        logger.error("Unable to Determine Latest Files for the Given Raw Directory", e)
        throw e
    }
  }
}
