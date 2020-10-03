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

import io.github.bkosaraju.inferencer.functions._
import io.github.bkosaraju.utils.spark.{DataHashGenerator, WriteData}
import io.github.bkosaraju.inferencer.functions.{AmendDwsCols, AppConfig, ArchiveFiles, ConvNonStdDateTimes, DataTypeValidation, Exceptions, ExtractTaggedColumns, FSManager, GetCuratedDF, GetExternalSchema, GetLatestDataSets, GetStgSchema, GetTargetSelList, GetTargetValidationFilter, HeaderToColumns, IsSchemaSame, LoadDataFile, LoadFixedWidthFile, LoadRdd, LoadSchema, LoadTableVars, LoadTableandRegister, RemoveHdfsData, RemoveHeaderAndFooter, Session, SrcToTgtColRename, SrcToTgtColSchemaChange, StringToMap, WriteHiveTable}


class AppFunctions
  extends Session
  with StringToMap
  with GetCuratedDF
  with ConvNonStdDateTimes
  with LoadSchema
  with GetStgSchema
  with GetTargetSelList
  with LoadDataFile
  with LoadRdd
  with IsSchemaSame
  with AmendDwsCols
  with SrcToTgtColRename
  with SrcToTgtColSchemaChange
  with LoadTableVars
  with RemoveHdfsData
  with WriteHiveTable
  with LoadTableandRegister
  with GetTargetValidationFilter
  with DataTypeValidation
  with GetLatestDataSets
  with ExtractTaggedColumns
  with Exceptions
  with LoadFixedWidthFile
  with ArchiveFiles
  with RemoveHeaderAndFooter
  with HeaderToColumns
  with GetExternalSchema
  with FSManager
  with WriteData
  with AppConfig
  with DataHashGenerator
