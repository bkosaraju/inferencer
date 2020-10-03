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

import org.apache.hadoop.fs.Path

trait RemoveHdfsData extends Session {
  /**
    * Method to remove a files from HDFS Directory
    * this is used to delete the file recursively from a directory for the external table in case of table is being loaded in overwrite mode.
    * this method throws an exception while it fail to remove the path except the cases if the path is not existed.
    * @param location
    * @return Unit
    */
  def removeHdfsData(location : String) : Unit = {
    try {
      val tableURI = new Path(location)
      val hadoopfs = tableURI.getFileSystem(sparkSession.sessionState.newHadoopConf())
      if (hadoopfs.exists(tableURI) && ! tableURI.isRoot) {
        logger.info("deleting the table path : "+location)
        hadoopfs.delete(tableURI, true)
      } else {
        logger.info("Table path : "+location+" not existed or root path, hence proceeding to load the data ..")
      }
    } catch {
      case e: Exception =>
        logger.error("Unable to Delete the Existing Data from Location: " + location, e)
        throw e
    }
  }
}
