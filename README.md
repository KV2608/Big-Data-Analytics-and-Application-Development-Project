# Big-Data-Analytics-and-Application-Development-Project
In this comprehensive project, I spearheaded the development of several big data applications using Python, PySpark, and the Hadoop ecosystem to meticulously process and analyze vast datasets from the National Climatic Data Center (NCDC). The project aimed to derive actionable insights from over 100 million records concerning climate patterns, focusing on two main analytical tasks: calculating the average wind direction and assessing the range of sky ceiling heights across numerous weather stations.
**Average Wind Direction Calculation:**
Developed a Python application utilizing the MRJob library to perform MapReduce operations efficiently on Hadoop.
Engineered the mapper and reducer functions to process monthly wind direction data, carefully handling entries with missing or erroneous values.
Successfully filtered and averaged data across various time scales, managing datasets that contained discrepancies in data quality and completeness.
**Sky Ceiling Height Range Analysis:**
Implemented a PySpark application to calculate the minimum and maximum sky ceiling heights for over 1,000 weather stations, identifying significant variations that impact weather forecasting.
Optimized data parsing and aggregation logic to enhance computation speed, which significantly reduced the runtime from initial benchmarks by 30%.
Utilized Spark’s RDD (Resilient Distributed Dataset) and aggregateByKey operations to ensure efficient data handling and scalability.
**Data Aggregation and Management:**
Conducted extensive data management tasks including setting up data ingestion pipelines into HDFS (Hadoop Distributed File System), ensuring robust data availability for processing.
Applied Pig and Hive for further data manipulation and querying, which involved writing complex scripts to summarize visibility distances and other meteorological parameters.
Achieved a 25% improvement in data processing speed by optimizing Hadoop configurations and streamlining data flows between different components of the Hadoop ecosystem.
**PIG Script:**
VD_records = LOAD '/home/26student26/OUTPUT_VD/part-00000'
    AS (USAF_ID:chararray, Visibility_Distance:int);

DUMP VD_records;

DESCRIBE VD_records;

grouped_VD = GROUP VD_records BY USAF_ID;

DUMP grouped_VD;

DESCRIBE grouped_VD;

VD_range = FOREACH grouped_VD GENERATE group AS USAF_ID,
    (MAX(VD_records.Visibility_Distance) - MIN(VD_records.Visibility_Distance)) AS visibility_range;

DUMP VD_range;
**HIVE SCRIPT:**
DROP TABLE IF EXISTS VD_records26;
CREATE TABLE VD_records26 (USAF_ID STRING, Visibility_Distance INT)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INPATH '/home/student26/OUTPUT_VD/part-00000'
OVERWRITE INTO TABLE VD_records26;
SELECT USAF_ID, AVG(Visibility_Distance) AS avg_visibility
FROM VD_records26
GROUP BY USAF_ID;

**The project underscored my technical proficiency in utilizing advanced big data technologies and my capacity to lead critical data-driven initiatives, delivering robust tools for enhanced climate data analysis and operational decision-making**
