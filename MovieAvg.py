from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_average_ratings)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, float(rating)

    def reducer_count_ratings(self, key, values):
        ratings = list(values)
        yield key, (sum(ratings), len(ratings))

    def reducer_average_ratings(self, key, values):
        total_ratings = 0
        num_ratings = 0

        for total, count in values:
            total_ratings += total
            num_ratings += count

        average_rating = total_ratings / num_ratings
        yield key, average_rating

if _name_ == '_main_':
    RatingsBreakdown.run()
