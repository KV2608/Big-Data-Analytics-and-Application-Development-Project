from pyspark import SparkContext

def parse_line(line):
    
    usaf_id = line[4:10]
    sky_ceiling_height = line[70:75]
    quality_code = line[75]
    if sky_ceiling_height != '99999' and quality_code in '01459':
        return (usaf_id, int(sky_ceiling_height))
    else:
        return (usaf_id, None)

def range_SCH(rdd):
    
    
    return rdd.filter(lambda x: x[1] is not None) \
              .aggregateByKey((float('inf'), float('-inf')),
                              lambda a, b: (min(a[0], b), max(a[1], b)),
                              lambda a, b: (min(a[0], b[0]), max(a[1], b[1]))) \
              .mapValues(lambda x: x[1] - x[0])

def main():
    
    sc = SparkContext(appName="SkyCeilingHeightRange")
    
    input_path = '/home/26student26/INPUT_PROJECT/ProjectData'
    output_path = '/home/26student26/OUTPUT_Spark_SCH'

    rdd = sc.textFile(input_path)

    parsed_rdd = rdd.map(parse_line)
    range_rdd = range_SCH(parsed_rdd)

    new_rdd = range_rdd.map(lambda x: '{0},{1}'.format(x[0], x[1]))
    new_rdd.saveAsTextFile(output_path)

    sc.stop()

if __name__ == '__main__':
    main()
