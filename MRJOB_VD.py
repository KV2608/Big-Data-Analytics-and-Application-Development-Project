from mrjob.job import MRJob
from mrjob.step import MRStep

class MRVisibilityDistance(MRJob):

    def mapper(self, _, line):
        usaf_id = line[4:10]
        visibility_distance = line[78:84].strip()
        quality_code = line[84]

        if visibility_distance != '999999' and quality_code in '01459':
            yield usaf_id, int(visibility_distance)

    def reducer(self, key, values):
        for value in values:
            yield key, value

    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]

if __name__ == '__main__':
    MRVisibilityDistance.run()


