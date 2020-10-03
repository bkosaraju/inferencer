# Inferencer a.k.a DataCurator

inference noun:

A guess that you make or an opinion that you form based on the information that you have:


The following application will load the data into target using the given input parameters.

docs for the process as follows:

## Application Parameters: 
**mode**    =   yarn/local

There are two deploy modes that can be used to launch Spark applications on YARN. In cluster mode, the Spark driver runs inside an application master process which is managed by YARN on the cluster, and the client can go away after initiating the application. In client mode, the driver runs in the client process, and the application master is only used for requesting resources from YARN.

Unlike other cluster managers supported by Spark in which the master’s address is specified in the --master parameter, in YARN mode the ResourceManager’s address is picked up from the Hadoop configuration. Thus, the --master parameter is yarn.

documenation [Link](https://spark.apache.org/docs/latest/running-on-yarn.html#launching-spark-on-yarn)

supported Types : 

* yarn / yarn-client
* yarn-cluster
* local (Not recommended )

At run time this will be overwritten with spark-submit arguments which will be passed 

**appName** = Name_Of_Application

Application Name which will be set by spark while running the job from Yarn/Other mode

Please note that this parameter will be overwritten by the application name specified at spark-submit configuration level such as `--name` which eventually derived from Oozie parameters (in case of oozie as the scheduler )

## Source Data Parameters :

**dataSourceURI** = s3a://landing/

base path of the Application where the source data files resides

**dataSource** = some_source

 3(mostly) character of the system where the data

**dataSet** = dataset_folder_name

source dataset name, Name of the dataset in lowercase separated by underscores (_). Where system of record is not the source (eg. CSA vs Siebel), prefix with 3 character abbreviation of the system of record.

All the above parameters will be used to construct the source file/directory path so that application can pick the file from location and process it and loads to the curated area.

## Target Specific Parameters :

**targetDatabase** = ""

Target Database Name where the curated data is placed.

**targetTable** = s_evt_act

Target Table Name where the curated data is placed.

**targetPartition** = src_dttm

provide the value if the target table is partitioned, this value will be used while loading the table into the target area in saveAsTable/Insert methods as the partition column.

**schemaFile** = s3://src_bucket/config/sch.schema

schema location to infer for input source.

currently it support two types of schemas 

1. DDL schama - keep the same format as DDL excluding  create table and closing index section
    example 
        col1 integer,
        col2 timestap,
        col3 decimal(10,2),
        col4 string
        etc ..
2. Avro Schema - you can pass standarad avro schema URI so that it will infer that schema.

as per order of precidency it looks for target schema by extracting target are and if not found then it apply schemaFile.
         
**writeMode** = append

Target mode to write the data into curated table 

possible values are 

* append : to the existing data
* overwrite : truncates and replaces the target
* delta : Append only changes to target data  
 
## Data Processing Parameters :

**readerFormat** = binary 

Specifies the source data format.

the following formats are developed and tested to be used as source data formats 

|Format | parameter to be passed | Description |
| ------- | ------------------ | ----------- |
|non UTF file| binary| If the file is of UTF or Extended UTF-8 characters in such cases files will be read with binary format and parsed using the extended character set (StandardCharsets.ISO_8859_1)|
|text|text| The standard text files where the data can be separated by plain text characters ex : ```|,,~,#,etc ..```|
|csv|csv `or` com.databrcks.spark.csv (in case if databricks parser included as dependent JAR )| in case of CSV files |
|json|json|In case of Json file format is specified, the result Json elements will be flatten (in case if data has any kind of nested json elements) and loaded in to target, the array elements get exploded please note that The target elements will be flattened with conjunction string `.`. Please Note that Spark 2.2 must be used while you are using this method|
|xml|xml `or ` com.databricks.spark.xml|In case of xml file format is specified, the result xml elements will be flatten (in case if data has any kind of nested xml elements) and loaded in to target, the array elements get exploded please note that The target elements will be flattened with conjunction string `.`. Please Note that Spark 2.2 must be used while you are using this method|
|parquet | parquet | for parquet files|
|avro | com.databricks.spark.avro | for avro files -- must provide the external dependent JAR with `--jars <>` option
|orc| orc | for ORC files|
|fixed width| fixedwidth | for fixed width mainframe files
|cobal files | cobal or za.co.absa.cobrix | cobal parser reads metadata from cobal copybook and parse the files (thanks and credits to https://github.com/AbsaOSS/cobrix)
|SAS datasets | com.github.saurfang.sas.spark | cobal parser reads metadata from cobal copybook and parse the files (thanks and credits to https://github.com/saurfang/spark-sas7bdat)  


**recordDelimiter** = \u00c3\u000a

Line separator for binary or text files 
advised to pass this value with unicode character```\uxxxx``` or HEX character ```\\xNN``` so that it would be easy for support team to read the value   

**fieldDelimiter**=\u001c\u001e

field separator for binary or text files 
advised to pass this value with unicode character```\uxxxx``` or HEX character ```\\xNN``` so that it would be easy for support team to read the value

please note that in text/binary file format if you have delimiter `|` please ensure that you must escape the charector as `|` already used as regular expression or case   

**readerOptions** = "header=false,nullValue=\n"

comma separated list of key values with `=` as mapper.

while using the external options you may refer the external config [options](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameReader@csv(paths:String*):org.apache.spark.sql.DataFrame)

for text/binay/fixedwidth files you may use the following options.

```
header (deafult , false) : true so that parser removes the first record of the file
footer (deafult , false) : true so that parser removes the last record of the file
headerLength : specified number n removes the first n records from file
footerLength : specified number n removes the last n records from file

by setting the headerLength/footerLength framework automatically consider header/footer options are defaulted to ture so no need to specify. 
```
**recordFormat** : specify the regular expression for record format so that the underline RDD will filter out based on the expression(can be used for variable length records and multi format records).
*Please dont confuse with recordFormat in readeroptions with recordLayout for fixed width files. ensure you will not set header and footer as the recordFormat prunes the records.



You can set the following CSV-specific options to deal with CSV files:


```You can set the following CSV-specific options to deal with CSV files:
   
   sep (default ,): sets a single character as a separator for each field and value.
   encoding (default UTF-8): decodes the CSV files by the given encoding type.
   quote (default "): sets a single character used for escaping quoted values where the separator can be part of the value. If you would like to turn off quotations, you need to set not null but an empty string. This behavior is different from com.databricks.spark.csv.
   escape (default \): sets a single character used for escaping quotes inside an already quoted value.
   charToEscapeQuoteEscaping (default escape or \0): sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are different, \0 otherwise.
   comment (default empty string): sets a single character used for skipping lines beginning with this character. By default, it is disabled.
   header (default false): uses the first line as names of columns.
   inferSchema (default false): infers the input schema automatically from data. It requires one extra pass over the data.
   ignoreLeadingWhiteSpace (default false): a flag indicating whether or not leading white spaces from values being read should be skipped.
   ignoreTrailingWhiteSpace (default false): a flag indicating whether or not trailing white spaces from values being read should be skipped.
   nullValue (default empty string): sets the string representation of a null value. Since 2.0.1, this applies to all supported types including the string type.
   nanValue (default NaN): sets the string representation of a non-number" value.
   positiveInf (default Inf): sets the string representation of a positive infinity value.
   negativeInf (default -Inf): sets the string representation of a negative infinity value.
   dateFormat (default yyyy-MM-dd): sets the string that indicates a date format. Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to date type.
   timestampFormat (default yyyy-MM-dd'T'HH:mm:ss.SSSXXX): sets the string that indicates a timestamp format. Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to timestamp type.
   maxColumns (default 20480): defines a hard limit of how many columns a record can have.
   maxCharsPerColumn (default -1): defines the maximum number of characters allowed for any given value being read. By default, it is -1 meaning unlimited length
   mode (default PERMISSIVE): allows a mode for dealing with corrupt records during parsing. It supports the following case-insensitive modes.
   PERMISSIVE : sets other fields to null when it meets a corrupted record, and puts the malformed string into a field configured by columnNameOfCorruptRecord. To keep corrupt records, an user can set a string type field named columnNameOfCorruptRecord in an user-defined schema. If a schema does not have the field, it drops corrupt records during parsing. When a length of parsed CSV tokens is shorter than an expected length of a schema, it sets null for extra fields.
   DROPMALFORMED : ignores the whole corrupted records.
   FAILFAST : throws an exception when it meets corrupted records.
   columnNameOfCorruptRecord (default is the value specified in spark.sql.columnNameOfCorruptRecord): allows renaming the new field having malformed string created by PERMISSIVE mode. This overrides spark.sql.columnNameOfCorruptRecord.
   multiLine (default false): parse one record, which may span multiple lines.
   ```
`useHeader=true` - in case if column names should be derived from CSV header.

You can set the following JSON-specific options to deal with non-standard JSON files:

```
   
   primitivesAsString (default false): infers all primitive values as a string type
   prefersDecimal (default false): infers all floating-point values as a decimal type. If the values do not fit in decimal, then it infers them as doubles.
   allowComments (default false): ignores Java/C++ style comment in JSON records
   allowUnquotedFieldNames (default false): allows unquoted JSON field names
   allowSingleQuotes (default true): allows single quotes in addition to double quotes
   allowNumericLeadingZeros (default false): allows leading zeros in numbers (e.g. 00012)
   allowBackslashEscapingAnyCharacter (default false): allows accepting quoting of all character using backslash quoting mechanism
   allowUnquotedControlChars (default false): allows JSON Strings to contain unquoted control characters (ASCII characters with value less than 32, including tab and line feed characters) or not.
   mode (default PERMISSIVE): allows a mode for dealing with corrupt records during parsing.
   PERMISSIVE : sets other fields to null when it meets a corrupted record, and puts the malformed string into a field configured by columnNameOfCorruptRecord. To keep corrupt records, an user can set a string type field named columnNameOfCorruptRecord in an user-defined schema. If a schema does not have the field, it drops corrupt records during parsing. When inferring a schema, it implicitly adds a columnNameOfCorruptRecord field in an output schema.
   DROPMALFORMED : ignores the whole corrupted records.
   FAILFAST : throws an exception when it meets corrupted records.
   columnNameOfCorruptRecord (default is the value specified in spark.sql.columnNameOfCorruptRecord): allows renaming the new field having malformed string created by PERMISSIVE mode. This overrides spark.sql.columnNameOfCorruptRecord.
   dateFormat (default yyyy-MM-dd): sets the string that indicates a date format. Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to date type.
   timestampFormat (default yyyy-MM-dd'T'HH:mm:ss.SSSXXX): sets the string that indicates a timestamp format. Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to timestamp type.
   multiLine (default false): parse one record, which may span multiple lines, per file
   ```
   `inferTargetSchema=true` -- more on this can be found at Json reader section.

For xml you can use following properties

```
 rowTag: The row tag of your xml files to treat as a row. For example, in this xml <books> <book><book> ...</books>, the appropriate value would be book. Default is ROW. At the moment, rows containing self closing xml tags are not supported.
 samplingRatio: Sampling ratio for inferring schema (0.0 ~ 1). Default is 1. Possible types are StructType, ArrayType, StringType, LongType, DoubleType, BooleanType, TimestampType and NullType, unless user provides a schema for this.
 excludeAttribute : Whether you want to exclude attributes in elements or not. Default is false.
 treatEmptyValuesAsNulls : (DEPRECATED: use nullValue set to "") Whether you want to treat whitespaces as a null value. Default is false
 mode: The mode for dealing with corrupt records during parsing. Default is PERMISSIVE.
   PERMISSIVE :
     When it encounters a corrupted record, it sets all fields to null and puts the malformed string into a new field configured by columnNameOfCorruptRecord.
     When it encounters a field of the wrong datatype, it sets the offending field to null.
   DROPMALFORMED : ignores the whole corrupted records.
   FAILFAST : throws an exception when it meets corrupted records.
 columnNameOfCorruptRecord: The name of new field where malformed strings are stored. Default is _corrupt_record.
 attributePrefix: The prefix for attributes so that we can differentiate attributes and elements. This will be the prefix for field names. Default is _.
 valueTag: The tag used for the value when there are attributes in the element having no child. Default is _VALUE.
 charset: Defaults to 'UTF-8' but can be set to other valid charset names
 ignoreSurroundingSpaces: Defines whether or not surrounding whitespaces from values being read should be skipped. Default is false.
```
   
these values get converted to Map and passed as reader options in the code

**recordLayout** = 1|5,6|12,19|4

Record layout of fixed width files, comma separated list of pipe separated entries 

ex:startPos1|length,startPos2|length  

**cleanseChar** = \\r{0,1}\\n

any characters that needs to be trimmed off from the source file (will be helpful to cleanse some data from input source file)
this will work only in-case of binary format is chosen.

**srcFormats**="src_start_date=yyyy/MM/dd HH:mm:ss"

in case of input data (date/time/datetime/timestamp) is in non slandered format (YYYY-mm-DD HH:MM:SS.sss) the values will be assist to converter.

in case if any column missed here and not in standard format will result in NULL values in target area.

``note`` : since Application uses Java's simple date module which is inherited from hive's unix_timestamp UDF which support upto millisecond precision, 
to ensure to get the nanosecond support method extract anything after second and pads it back 

effectively this module will and should support nano second precision provided the format specified with `S` ex : yyyyMMdd HHmmss`S`   

**srcToTgtColMap** = "src_timestamp=src_timestamp_view,col2=src_col2"

Incase of renaming the columns for Avro/Json/

**errorThresholdPercent**=5

In the event of data quality checks what is the acceptable percent to load the data into table.

lets take when the user defined the value is 5, if the data qualified to be valid upto 95% percent the error records will be loaded into error location(same as data set with .err extension to the directory ).

for the records above the threshold percent the application will fail stating the same.

**fileBasedTagPattern**=same_file_name_(\d{10})_(\d{8}).csv.dat
**fileBasedTagColumns**=src_date,src_time

in case if any of the tags in file must be passed to the file can be loaded by sitting these two variables.

it extracts the tags based on the grouped patterns on the order it mapped and load the same to the tagged columns mentioned.

in case of more columns present than tags there will be an exception and job will be aborted.

**customTransformationOptions** = src_sys='IMF'
Adds the custom logic into filelds should be populated into tables.
example : src_sys='IMF'

this particular property can be used to set the default variables to table in case if source is not sending them.

**generateRowHash** = true

set true in case if needs to amend "row_hash"(or generateRowHashColumn property) column to target.
example : generateRowHash=true

**generateRowHashColumn** 
sets column name for generated for hash (non default row_hash)
example : generateRowHashColumn = src_sys_hash

**encryptionColumns**
Coma separation list of column names where it should be encrypted.
please note that target column should be defined as string as after encryption it gets stored as strings.

**encryptionKey**
key to encrypt and decrypt columns.

## Build Instructions :

**Test execution** :
  code coverage with test case execution
    gradle testScoverageReport
    or 
    gradle jacocoTestReport
**documentation** :

    gradle clean scaladocs

**build jar**:

gradle clean build scaladocs

**More Info on build tasks**
```
> Task :curation-app:tasks

------------------------------------------------------------
All tasks runnable from project :curation-app
------------------------------------------------------------

Build tasks
-----------
assemble - Assembles the outputs of this project.
build - Assembles and tests this project.
buildDependents - Assembles and tests this project and all projects that depend on it.
buildNeeded - Assembles and tests this project and all projects it depends on.
classes - Assembles main classes.
clean - Deletes the build directory.
jar - Assembles a jar archive containing the main classes.
scoverageClasses - Assembles scoverage classes.
testClasses - Assembles test classes.
testScoverageClasses - Assembles test scoverage classes.

Documentation tasks
-------------------
javadoc - Generates Javadoc API documentation for the main source code.
scaladoc - Generates Scaladoc for the main source code.

Help tasks
----------
buildEnvironment - Displays all buildscript dependencies declared in project ':curation-app'.
components - Displays the components produced by project ':curation-app'. [incubating]
dependencies - Displays all dependencies declared in project ':curation-app'.
dependencyInsight - Displays the insight into a specific dependency in project ':curation-app'.
dependentComponents - Displays the dependent components of components in project ':curation-app'. [incubating]
help - Displays a help message.
model - Displays the configuration model of project ':curation-app'. [incubating]
projects - Displays the sub-projects of project ':curation-app'.
properties - Displays the properties of project ':curation-app'.
tasks - Displays the tasks runnable from project ':curation-app'.

Shadow tasks
------------
knows - Do you know who knows?
shadowJar - Create a combined JAR of project and runtime dependencies

Verification tasks
------------------
check - Runs all checks.
jacocoTestCoverageVerification - Verifies code coverage metrics based on specified rules for the test task.
jacocoTestReport - Generates code coverage report for the test task.
test - Runs the unit tests.
```

## Usage Instructions : 

spark-submit <Spark Options> <Application JAR> <ARG1> <ARG2> <ARG3>

**Application JAR** : built Jar which compiled out of this source code.
**ARG1** : Properties file consists of the above mentioned properties. This file must be placed in HDFS as it can  be accessed from anywhere where the driver git initiated 
**ARG2** : Amended Audit column list as key value pairs mapped using  `=` ex : ```src_date=2017-12-31, load_timestamp=2017-12-31 10:15:05,ingestion_id=1524369```
**ARG3** : Extract Run directory to be amended after the source path is reconstructed.
 
## Aditional Info :

### for Json 

In along with loading the data the script will flatten the Json structure.

the following actions will be performed while flattening the json 

```mermaid
graph LR
Raw Data(Strat) --> Schema Infer DataFrame  --> Null Replaced Array Items --> Flatten Array elements (exploded) --> flatten Schema items --> Schema Infer DataFrame  
```

In case if given Json has partial columns and need to be loaded additional column all you need is to set in readerOptions - `inferTargetSchema=true` will merge the target Schema and source schema and infer to the input dataset.

please keep in mind that by setting `inferTargetSchema=true` may lead to full row nulls in case if the file is not belongs to the target, as it just keep nulls when there is no reference.

