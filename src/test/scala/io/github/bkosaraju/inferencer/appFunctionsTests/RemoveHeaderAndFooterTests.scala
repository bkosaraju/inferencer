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

trait RemoveHeaderAndFooterTests extends AppInterface{

  private  val testRDD = context.read.textFile("src/test/resources/csvdatasets/rand_data.csv").rdd

  test ("removeHeaderAndFooter : remove the header from text/binary/fixedwidth files ",appFunctionTests) {
    assertResult(500) {
      val resRDD = af.removeHeaderAndFooter(testRDD,Map("header" ->"true"))
      resRDD.count()
    }
  }

  test ("removeHeaderAndFooter : remove the footer from text/binary/fixedwidth files ",appFunctionTests) {
    assertResult(500) {
      val resRDD = af.removeHeaderAndFooter(testRDD,Map("footer" ->"true"))
      resRDD.count()
    }
  }

  test ("removeHeaderAndFooter : remove the header and footer from text/binary/fixedwidth files ",appFunctionTests) {
    assertResult(499) {
      val resRDD = af.removeHeaderAndFooter(testRDD,Map("footer" ->"true","header"->"true"))
      resRDD.count()
    }
  }

  test ("removeHeaderAndFooter : remove the header (of size 10 rows ) from text/binary/fixedwidth files ",appFunctionTests) {
    assertResult(491) {
      val resRDD = af.removeHeaderAndFooter(testRDD,Map("header" ->"true","headerLength" -> "10"))
      resRDD.count()
    }
  }

  test ("removeHeaderAndFooter : remove the footer (of size 10 rows ) from text/binary/fixedwidth files ",appFunctionTests) {
    assertResult(491) {
      val resRDD = af.removeHeaderAndFooter(testRDD,Map("footer" ->"true","footerLength" -> "10"))
      resRDD.count()
    }
  }


  test ("removeHeaderAndFooter : remove the header(1 row) and footer (of size 10 rows) from text/binary/fixedwidth files ",appFunctionTests) {
    assertResult(490) {
      val resRDD = af.removeHeaderAndFooter(testRDD,Map("header" -> "true","footer" ->"true","footerLength" -> "10"))
      resRDD.count()
    }
  }


  test ("removeHeaderAndFooter : remove the header from text/binary/fixedwidth files (with other header option (fales)) ",appFunctionTests) {
    assertResult(501) {
      val resRDD = af.removeHeaderAndFooter(testRDD,Map("header" ->"false"))
      resRDD.count()
    }
  }

  test ("removeHeaderAndFooter : remove the header from text/binary/fixedwidth files (with other header option (fales) and headerLength - 5) ",appFunctionTests) {
    assertResult(496) {
      val resRDD = af.removeHeaderAndFooter(testRDD,Map("header" ->"false","headerLength" -> "5"))
      resRDD.count()
    }
  }


  test ("removeHeaderAndFooter : remove the header from text/binary/fixedwidth files (with no header option) ",appFunctionTests) {
    assertResult(501) {
      val resRDD = af.removeHeaderAndFooter(testRDD,Map())
      resRDD.count()
    }
  }


  test("removeHeaderAndFooter : raise an exeception while remove the header ",appFunctionTests) {
    intercept[Exception] {
      af.removeHeaderAndFooter(null,null)
    }
  }


}
