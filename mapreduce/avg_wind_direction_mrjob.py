from mrjob.job import MRJob

class AvgWindDirection(MRJob):

    def mapper(self, _, line):
        fields = line.split(',')
        try:
            wind_dir = int(fields[3])
            if wind_dir != 999:
                yield "wind_direction", wind_dir
        except:
            pass

    def reducer(self, key, values):
        total, count = 0, 0
        for v in values:
            total += v
            count += 1
        yield key, total / count if count else 0

if __name__ == '__main__':
    AvgWindDirection.run()
