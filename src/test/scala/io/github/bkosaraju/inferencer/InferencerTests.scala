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

package io.github.bkosaraju.inferencer

import io.github.bkosaraju.inferencer.SchemaFlattnerTests._
import io.github.bkosaraju.inferencer.SchemaFlattnerTests.{ExplodeDFTests, FlattenColListTests, GetArrayColListTests}
import io.github.bkosaraju.inferencer.appFunctionsTests.{AmendDwsColsTests, ArchiveDataTests, CobolDataTests, ContextcreationTests, ConvNonStdDateTimesTests, DataTypeValidationTests, DriverTests, DropdwsColsTests, ExtractTaggedColumnsTests, GetCuratedDFTests, GetExternalSchemaTests, GetLatestDataSetsTest, GetStgSchemaTests, GetTargetSelListTests, GetTargetValidationFilter, HeaderToColumnsTests, IsSchemaSameTests, LoadCuratedDataTests, LoadDataFileTests, LoadEnvVarsTests, LoadPropertiesTests, LoadRDDTests, LoadSchemaTests, LoadStdDFTests, LoadTableVarsTests, LoadTableandRegisterTests, RemoveHdfsDataTests, RemoveHeaderAndFooterTests, SrcToTgtColRenameTests, SrcToTgtColSchemaChangeTests, StringToMapTests, UnZipTests, WriteHiveTableTests}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class InferencerTests() extends
  LoadPropertiesTests
  with ContextcreationTests
  with LoadEnvVarsTests
  with ConvNonStdDateTimesTests
  with DropdwsColsTests
  with StringToMapTests
  with GetStgSchemaTests
  with GetTargetSelListTests
  with LoadStdDFTests
  with AmendDwsColsTests
  with SrcToTgtColRenameTests
  with SrcToTgtColSchemaChangeTests
  with LoadTableVarsTests
  with RemoveHdfsDataTests
  with LoadDataFileTests
  with WriteHiveTableTests
  with LoadTableandRegisterTests
  with LoadSchemaTests
  with GetCuratedDFTests
  with LoadCuratedDataTests
  with DriverTests
  with DataTypeValidationTests
  with GetLatestDataSetsTest
  with ExtractTaggedColumnsTests
  with LoadRDDTests
  with ArchiveDataTests
  with GetArrayColListTests
  with FlattenColListTests
  with ExplodeDFTests
  with GetTargetValidationFilter
  with IsSchemaSameTests
  with RemoveHeaderAndFooterTests
  with GetExternalSchemaTests
  with UnZipTests
  with HeaderToColumnsTests
  with CobolDataTests
