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

import org.apache.spark.input.PortableDataStream
import java.util.zip.ZipInputStream

import org.apache.commons.io.IOUtils
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

trait UnzipFile extends Session {

  def unzipFile (path: String ) : RDD[String] = {
    logger.info("Found the un-spported archive (.zip): "+path+" hence using binary Zip extractor")
    sparkSession.sparkContext.binaryFiles(path, 1)
      .flatMap((file: (String, PortableDataStream)) => {
        val zpStr = new ZipInputStream(file._2.open)
        val entry =  zpStr.getNextEntry //moving index pass to the content.
        IOUtils.readLines(zpStr, "UTF-8").asScala
      })
  }
}
