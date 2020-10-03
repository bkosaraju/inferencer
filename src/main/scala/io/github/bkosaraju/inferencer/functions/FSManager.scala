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

import java.util.Date

import org.apache.hadoop.fs.{FileSystem, Path}
import io.github.bkosaraju.utils.aws.AwsUtils
import com.amazonaws.services.s3.AmazonS3URI
import java.text.SimpleDateFormat


trait FSManager extends Session {

  val s3Utils = new AwsUtils

  def getRunId: String = {
    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    sdf.format(new Date())
  }

  def moveData(sourceURI : Path, targetURI : Path, filePattern: String = ".*" ) : Unit = {
    try {
      if (sourceURI.toString.toLowerCase.matches("s3[a]?://.*")) {
        val awsSourceURI = new AmazonS3URI(sourceURI.toString.replaceAll("s3[a]?://","s3://"))
        val awsTargetURI = new AmazonS3URI(targetURI.toString.replaceAll("s3[a]?://","s3://"))
        val fileList = s3Utils.listS3Objects(awsSourceURI.getBucket,awsSourceURI.getKey)
        fileList
          .filter(file =>( file.getKey.matches(filePattern) || file.getKey.replaceAll("^.*/","").matches(filePattern)))
          .foreach(file => {
          val config = collection.mutable.Map[String, String]()
          config.put("sourceBucketName", file.getBucketName)
          config.put("sourceObjectKey", file.getKey)
          config.put("targetBucketName", file.getBucketName)
          config.put("targetObjectKey",
            file.getKey.replaceAll(awsSourceURI.getKey,awsTargetURI.getKey)
          )
          s3Utils.copyS3Object(config.toMap)
          s3Utils.deleteS3Object(file.getBucketName, file.getKey)
        } ) }
      else {
        val hadoopfs: FileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
        hadoopfs.listStatus(sourceURI)
          .filter(file => file.getPath.getName.matches(filePattern)).foreach(
          file => hadoopfs.rename(file.getPath,
            new Path(
              s"""${targetURI}/${file.getPath.getName.replaceAll(file.getPath.getParent.getName,"")}"""
            )
        )
        )
      }
    } catch {
      case e : Exception => {
        logger.warn(s"Unable to move data between instances source: ${sourceURI}, target: ${targetURI}")
        throw e
      }
    }
  }
}
