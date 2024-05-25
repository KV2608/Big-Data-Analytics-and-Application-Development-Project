from mrjob.job import MRJob
from mrjob.step import MRStep

class MRAverageWindDirection(MRJob):

    def mapper(self, _, line):
        
        year_month = line[15:21]  
        wind_direction = line[60:63]  
        quality_code = line[63]  

        
        if wind_direction != '999' and quality_code in '01459':
            yield (year_month, (int(wind_direction), 1))

    def reducer(self, year_month, values):
        total_wind_direction, count = 0, 0
       
        for wind_direction, occurrences in values:
            total_wind_direction += wind_direction * occurrences
            count += occurrences
        
       
        if count:
            average_wind_direction = total_wind_direction / count
            yield (year_month, average_wind_direction)

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

if __name__ == '__main__':
    MRAverageWindDirection.run()

