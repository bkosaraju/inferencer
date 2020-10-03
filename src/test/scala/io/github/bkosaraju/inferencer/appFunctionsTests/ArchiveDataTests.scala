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

package io.github.bkosaraju.inferencer.appFunctionsTests

import io.github.bkosaraju.inferencer.appFunctionTests
import io.github.bkosaraju.inferencer.{AppInterface, appFunctionTests}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, FileUtil, Path}

trait ArchiveDataTests extends AppInterface {

  private val hadoopfs: FileSystem = FileSystem.get(context.sparkContext.hadoopConfiguration)

  private val filetobemoved = "build/tmp/processing/201812141518/abc.txt"
  test ("archiveData : Move the processed data into archive area",appFunctionTests) {
    assertResult(true) {
      if (! hadoopfs.exists(new Path(filetobemoved))) {
      val fs = hadoopfs.create(new Path(filetobemoved))
       fs.close()
      }
      af.archiveFiles(filetobemoved,"build/tmp/processing/201812141518")
      hadoopfs.exists(new Path("build/tmp/20181214/abc.txt"))
    }
  }

  test ("archiveData : Exception in case if it could be able to move the processed data into archive area due to source issue",appFunctionTests) {
    intercept[Exception] {
      af.archiveFiles(null,"build/tmp/processing/20181214151853")
    }
  }


  test ("archiveData : Exception in case if not be able to move the processed data into archive area",appFunctionTests) {
    intercept[Exception] {
      af.archiveFiles(null,null)
    }
  }

  private val file1 = "build/tmp/processing/201901311518/file1.txt"
  private val file2 = "build/tmp/processing/201901311518/file2.txt"
  private val file3 = "build/tmp/processing/201901311518/file3.txt"
  test ("archiveData : Move the processed data into archive area - in overwrite mode move all files in processing directory",appFunctionTests) {
    assertResult(true) {
      for (k <- 1 to 3) {
        if (!hadoopfs.exists(new Path("build/tmp/processing/201901311518/file" + k + ".txt"))) {
          val fs = hadoopfs.create(new Path("build/tmp/processing/201901311518/file" + k + ".txt"))
          fs.close()
        }
      }
      af.archiveFiles("build/tmp/processing/201901311518/file3.txt","build/tmp/processing/201901311518","overwrite")
      hadoopfs.exists(new Path("build/tmp/20190131/file2.txt")) && hadoopfs.exists(new Path("build/tmp/20190131/file1.txt"))
    }
  }

}