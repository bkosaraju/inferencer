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

import org.apache.hadoop.fs.{FileSystem, Path,FileUtil}

trait ArchiveFiles extends Session with Exceptions {
  /**
    * Method to write the processed files into archive area in case of successful senario
    * @param location  source file location
    */
  def archiveFiles(location : String, srcPath : String, writemode: String = "")  {
    /*
  Note : Please note that for the full refresh datasets to pick only te latest files from given input filse for CSV , Text Json
         this specific block which ensure that for the source fils which are not part of
         orc/parquet,avro will pick only the latest partition from the given input files, these files are excuded due to the nature that these will have many part files
         in case of bringing the structured data into platform raw area (orc/parquet/avro), ensure that you overwrite the files in RAW area
   */
    val archPath = srcPath.substring(0,srcPath.length-4).replaceAll("/processing", "")
    try {
      val hadoopfs: FileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
      val pathStatus = hadoopfs.listStatus(new Path(location))
      val archFile = new Path(archPath+"/"+location.replaceAll(".*/",""))
      val sourcePath =  new Path(location)
      //for (fileName <- pathStatus) {
        if (srcPath.contains("/processing/")) {
          hadoopfs.mkdirs(new Path(archPath))
          if ("overwrite".equalsIgnoreCase(writemode)) {
            val filesToMove = hadoopfs.listFiles(new Path(srcPath),false)
            val targetDirectory = new Path(archPath)
            while (filesToMove.hasNext) {
              val nm = filesToMove.next().getPath
              logger.info("Archiving file : "+nm.toString+" to : "+targetDirectory.toString)
              hadoopfs.rename(nm,targetDirectory)
            }
          } else {
            logger.info("Archiving file : "+location +" to : "+archFile)
            if (! hadoopfs.rename(sourcePath,archFile) ){
              throw new FileRenameExcetpion("Unable to move the file")
            }
          }
        }
      //}
    } catch {
      case e :Exception => {
        logger.error("Unable to archive the file into Partition directory source :"+location+" target :"+archPath ,e)
        throw e
      }
    }
  }
}