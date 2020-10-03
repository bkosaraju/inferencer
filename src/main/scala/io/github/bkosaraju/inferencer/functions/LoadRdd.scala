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

import org.apache.hadoop.fs.{ContentSummary, FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

trait LoadRdd extends Session with Exceptions with RemoveHeaderAndFooter with UnzipFile {
  /**
    * Loads the flat flies into RDD of Map of Filenames and File contents.
    * this method is used to get the binary data as RDD so that it will eventually consumed by LoadData API
    *
    * @param path   input file path
    * @param readerFormat input reader format - binary to read the non standard files
    * @return RDD[(String, String)]
    * @example
    *         loadRdd(path,readerformat)
    *         {{{
    *   if ("binary".equals(readerFormat.toLowerCase)) {
    *           sparkSession.sparkContext.binaryFiles(path).mapValues(data => new String(data.toArray(), StandardCharsets.ISO_8859_1))
    *         } else {
    *           sparkSession.sparkContext.wholeTextFiles(path)
    *         }
    *       }
    *         }}}
    */
  def loadRdd(path: String, readerFormat: String="",recDelimiter: String = "\n",readerOptions :Map[String,String] =Map()): RDD[(String, String)] = {
    try {
      val content: ContentSummary =
        if (! path.contains("s3a://")) {
          FileSystem.get(sparkSession.sparkContext.hadoopConfiguration).getContentSummary(new Path(path))
        } else {
          new ContentSummary.Builder().length(1).build()
          }

      val dataRDD = {
        if (content.getLength == 0) {
          logger.info("Input Path :" + path + " consists of empty file(s), hence terminating the App..")
          throw new EmptyInputFileException("Input Path :\" + path + \" consists of empty file(s), hence terminating the App..")
        } else if (path.toLowerCase.contains(".zip")) {
          removeHeaderAndFooter(unzipFile(path), readerOptions)
        } else {
          val conf = sparkSession.sparkContext.hadoopConfiguration
          val defaultSep = conf.get("textinputformat.record.delimiter", "\n")
          conf.set("textinputformat.record.delimiter", recDelimiter)
          val rawRDD = sparkSession
            .sparkContext
            .newAPIHadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], conf)
            .map(_._2.toString)
            .filter(_.length > 0)
          conf
            .set("textinputformat.record.delimiter", defaultSep)
          removeHeaderAndFooter(rawRDD, readerOptions)
        }
      }
      if ( readerOptions.getOrElse("recordFormat","").nonEmpty) {
        val rgx = readerOptions("recordFormat").r
        dataRDD
          .flatMap(x => rgx.findFirstIn(x)).map(x => (path,x))
      } else { dataRDD.map(x => (path,x)) }
    } catch {
      case e: Throwable =>
        logger.error("Unable to Load File as RDD from Given Location :" + path, e)
        throw e
    }
  }
}



