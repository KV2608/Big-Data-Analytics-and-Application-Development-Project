from pyspark import SparkContext

sc = SparkContext(appName="SkyCeilingRange")

data = sc.textFile("hdfs:///ncdc/sky_ceiling_data.csv")

def parse_line(line):
    fields = line.split(',')
    try:
        station_id = fields[0]
        ceiling = int(fields[5])
        return (station_id, ceiling)
    except:
        return None

parsed = data.map(parse_line).filter(lambda x: x is not None)

range_by_station = parsed.aggregateByKey(
    (float('inf'), float('-inf')),
    lambda acc, val: (min(acc[0], val), max(acc[1], val)),
    lambda acc1, acc2: (min(acc1[0], acc2[0]), max(acc1[1], acc2[1]))
)

final_output = range_by_station.mapValues(lambda x: x[1] - x[0])

final_output.saveAsTextFile("hdfs:///output/sky_ceiling_range")
