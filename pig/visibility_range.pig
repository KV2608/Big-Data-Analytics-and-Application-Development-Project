VD_records = LOAD '/home/student26/OUTPUT_VD/part-00000'
USING PigStorage('\t')
AS (USAF_ID:chararray, Visibility_Distance:int);

grouped_VD = GROUP VD_records BY USAF_ID;

VD_range = FOREACH grouped_VD
GENERATE
    group AS USAF_ID,
    (MAX(VD_records.Visibility_Distance) -
     MIN(VD_records.Visibility_Distance)) AS visibility_range;

DUMP VD_range;
