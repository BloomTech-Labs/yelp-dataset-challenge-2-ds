from config import DATABASE_URI # get the database URI from file in gitignore
from models import * # download models.py from db_api folder
from db import get_session # download db.py from db_api folder
import pandas as pd
from collections import Counter
from sqlalchemy import or_
from scipy.stats import zscore, norm

# Adapted from code here: https://github.com/DistrictDataLabs/blog-files/blob/master/computing-bayesian-average-of-star-ratings/code/stars.py
# based on article here: https://medium.com/district-data-labs/computing-a-bayesian-estimate-of-star-rating-means-651496a890ab

"""
####################################################################
###########     Ranking Algorithm Description     ##################
####################################################################

The main purpose of this class is to calculate a composite ranking 
for yelp businesses. It uses a bayesian ranking system that matches 
how we intuitively interpret Yelp ratings; we don’t think a business 
with one 5-star review is better than a business with 100 reviews 
and a 4.8-star average. By using a bayesian algorithm, we can start 
with the assumption that a business is ranked the same as similar 
businesses and them move away from that assumption as we get more 
evidence in the form of star ratings, reviews, and tips.  The ranking 
has the following 3 components:

## Bayesian star rating estimate: ##

	We start with the distribution of star ratings in the 
    comparison businesses and scale that so it is equivalent to 
    25 ratings with that distribution. This forms our prior about 
    the distribution, and all ratings for the business in question 
    are added on top of the “existing” 25 ratings. The star rating 
    estimate is calculated from the average ranking of all of the 
    reviews for the business and the prior (that is equivalent to 
    25 reviews).

## Bayesian review sentiment estimate: ##

	Because review sentiment are distributed continuously, 
    forming our prior is simpler. We simply take the mean 
    review sentiment among all of the reviews for 
    comparison businesses and use that as our prior. The 
    review sentiment prior is scaled so that it is 
    equivalent to 25 reviews with that mean sentiment 
    rating. Among the actual reviews for the businesses, 
    reviews that are voted as “useful” are weighted as if 
    each vote of “useful” was an additional review with the 
    same sentiment. The final estimate is calculated as the 
    mean sentiment among the prior (equivalent to 25 reviews) 
    and the sentiment of the weighted reviews for the business.

## Bayesian tip sentiment estimate: ##

	The tip sentiment prior is calculated by taking the mean 
    tip sentiment among all of the tips for comparison businesses. 
    The prior is scaled to be equivalent to 10 tips. Among the 
    actual tips for the businesses, tips that are given compliments
    are weighted as if each compliment was an additional tip with 
    the same sentiment. The final estimate is calculated as the 
    mean sentiment among the prior (equivalent to 10 tips) and the 
    sentiment of weighted tips for the business.

Each of these components is calculated for the business being rated 
and all of their comparison businesses. The comparison businesses 
are evaluated in reference to the group of comparison businesses 
for the business being rated. The comparison businesses are not 
evaluated in reference to their own comparison businesses. The 
z-score is then calculated for each component score of the business 
being rated in reference to the component score of all of the 
comparison groups. We now have a z-score for each component. 

The component z-scores are combined to form a composite z-score 
with a 50% weighting on the star rating component, a 35% weighting 
on the review sentiment component, and a 15% weighting on the tip 
sentiment component. The class then calculates the percentile that 
is equivalent to the composite z-score to form the final percentile 
ranking.
"""

class Ratings(object):
    """
    An analytical wrapper that manages access to the data and wraps various
    statistical functions for easy and quick evaluation.
    """

    def __init__(self, business_id):
        self.business_id  = business_id

        # Initializing more data from database
        self.load()

        # Set dirichlet prior for star rating distribution 
        # based on distribution of comparison ratings.
        # Weights comparison to equal 25 reviews
        self.dirichlet_prior = self.get_dirichlet_prior()

        # Setting prior and confidence for review and tip sentiment
        self.review_prior, \
        self.review_confidence = self.get_review_prior()

        self.tip_prior, \
        self.tip_confidence = self.get_tip_prior()

        # Getting bayesian estimates for components
        self.dirichlet_estimate = self.get_dirichlet_estimate()
        self.review_sentiment_estimate = self.get_review_sentiment_estimate()
        self.tip_sentiment_estimate = self.get_tip_sentiment_estimate()

        self.percentile = self.get_percentile()

    def load(self):
        """
        Load data from db into DataFrames.
        """
        # Getting list of comparison business_ids
        self.comparison_ids = self.get_comparison_ids()

        # Initializing data about individual business
        self.review_df, \
        self.tip_df = self.get_business_data()

        # Initializing data about comparison group
        self.comparison_review_df, \
        self.comparison_tip_df = self.get_comparison_dfs()

    def review_sentiment_bayesian_mean(self, arr):
        """
        Computes the Bayesian mean from the prior and confidence.
        """
        if not self.review_prior or not self.tip_confidence:
            raise TypeError("Bayesian mean must be computed with m and C")

        return (self.review_confidence * self.review_prior + arr.sum()) \
             / (self.review_confidence + arr.count())

    def tip_sentiment_bayesian_mean(self, arr):
        """
        Computes the Bayesian mean from the prior and confidence.
        """
        if not self.tip_prior or not self.tip_confidence:
            raise TypeError("Bayesian mean must be computed with m and C")

        return (self.tip_confidence * self.tip_prior + arr.sum()) \
             / (self.tip_confidence + arr.count())
    
    def dirichlet_mean(self, arr):
        """
        Computes the Dirichlet mean with a prior.
        """
        counter   = Counter(arr)
        votes     = [counter.get(n, 0) for n in range(1, 6)]
        posterior = list(map(sum, zip(votes, self.dirichlet_prior)))
        N         = sum(posterior)
        weights   = map(lambda i: (i[0]+1)*i[1], enumerate(posterior))

        return float(sum(weights)) / N

    def get_review_sentiment_estimate(self):
        return self.review_df['weighted_polarity'].agg(self.review_sentiment_bayesian_mean)

    def get_tip_sentiment_estimate(self):
        return self.tip_df['weighted_polarity'].agg(self.tip_sentiment_bayesian_mean)

    def get_dirichlet_estimate(self):
        return self.review_df['stars'].agg(self.dirichlet_mean)
    
    def get_comparison_ids(self):
        ###############################
        ## TODO change from test list##
        ###############################
        # this needs to be a query that returns what is in the db
        comparison_ids = [
        'ujmEBvifdJM6h6RLv4wQIg',
            'NZnhc2sEQy3RmzKTZnqtwQ',
            'WTqjgwHlXbSFevF32_DJVw',
            'ikCg8xy5JIg_NGPx-MSIDA',
            'b1b1eb3uo-w561D0ZfCEiQ',
            'eU_713ec6fTGNO4BegRaww',
            '3fw2X5bZYeW9xCz_zGhOHg',
            'zvO-PJCpNk4fgAVUnExYAA',
            'b2jN2mm9Wf3RcrZCgfo1cg',
            'oxwGyA17NL6c5t1Etg5WgQ'
        ]
        return comparison_ids

    def get_comparison_dfs(self):

        comparison_ids = self.comparison_ids

        with get_session() as session:

            review_query = session.query(Review.business_id, Review.review_id, Review.stars, \
                Review.useful, ReviewSentiment.review_id, Review.date, \
                ReviewSentiment.polarity, ReviewSentiment.subjectivity \
                ).join(ReviewSentiment).filter(or_(*[Review.business_id == id \
                for id in comparison_ids])).all()
            comparison_review_df = pd.DataFrame(review_query)
            comparison_review_df = self.weight_review_polarity(comparison_review_df)

            tip_query = session.query(Tip.date, \
                Tip.compliment_count, Tip.business_id, \
                TipSentiment.tip_id, TipSentiment.polarity, \
                TipSentiment.subjectivity \
                ).join(TipSentiment).filter(or_(*[Tip.business_id == id \
                for id in comparison_ids])).all()
            comparison_tip_df = pd.DataFrame(tip_query)
            comparison_tip_df = self.weight_tip_polarity(comparison_tip_df)

        return comparison_review_df, comparison_tip_df

    def get_business_data(self):

        # TODO handle exceptions connecting to db
        with get_session() as session:

            # Getting review info
            review_query = session.query(Review.review_id, Review.stars, \
                Review.useful, Review.date, \
                ReviewSentiment.polarity, ReviewSentiment.subjectivity \
                ).join(ReviewSentiment).filter(Review.business_id==self.business_id).all()
            review_df = pd.DataFrame(review_query)
            review_df = self.weight_review_polarity(review_df)

            # Getting tip info
            tip_query = session.query(Tip.date, \
                Tip.compliment_count, \
                TipSentiment.tip_id, TipSentiment.polarity, \
                TipSentiment.subjectivity \
                ).join(TipSentiment).filter(Tip.business_id==self.business_id).all()
            tip_df = pd.DataFrame(tip_query)
            tip_df = self.weight_tip_polarity(tip_df)

        return review_df, tip_df
    
    def get_dirichlet_prior(self):
        value_counts = self.comparison_review_df.stars.value_counts()
        value_counts = value_counts.sort_index()
        distribution = value_counts/value_counts.sum()
        dirichlet_prior = distribution*25
        return dirichlet_prior

    def get_review_prior(self):
        mean = self.comparison_review_df.weighted_polarity.mean()
        confidence = 25
        return mean, confidence

    def get_tip_prior(self):
        mean = self.comparison_tip_df.weighted_polarity.mean()
        confidence = 10
        return mean, confidence

    def weight_review_polarity(self, df):
        df['weighted_polarity'] = (df.useful+1)*df.polarity
        return df

    def weight_tip_polarity(self, df):
        df['weighted_polarity'] = (df.compliment_count+1)*df.polarity
        return df

    def get_percentile(self):
        # getting review polarity scores for comparison group
        comp_r = self.comparison_review_df
        grp_r = comp_r.groupby('business_id')
        comp_r_polarity = grp_r['weighted_polarity'].agg(self.review_sentiment_bayesian_mean)
        
        # getting tip polarity scores for comparison group
        comp_t = self.comparison_tip_df
        grp_t = comp_t.groupby('business_id')
        comp_t_polarity = grp_t['weighted_polarity'].agg(self.review_sentiment_bayesian_mean)
        
        # getting dirichlet star estimate for comparison group
        comp_stars_estimates = grp_r['stars'].agg(self.dirichlet_mean)
        
        # adding business data at the end of each series
        comp_r_polarity = comp_r_polarity.append(pd.Series(self.review_sentiment_estimate))
        comp_t_polarity = comp_t_polarity.append(pd.Series(self.tip_sentiment_estimate))
        comp_stars_estimates = comp_stars_estimates.append(pd.Series(self.dirichlet_estimate))

        ## getting z score ##
        # returned array-like so taking the last element 
        # becuase that is the one that corresponds to the business
        z_r_polarity = zscore(comp_r_polarity)[-1]
        z_t_polarity = zscore(comp_t_polarity)[-1]
        z_stars = zscore(comp_stars_estimates)[-1]

        # getting composite ranking
        weighted_r_polarity = z_r_polarity*0.35
        weighted_t_polarity = z_t_polarity*0.15
        weighted_z_stars = z_stars*.5

        z_composite = weighted_r_polarity + weighted_t_polarity + weighted_z_stars

        percentile = norm.cdf(z_composite)

        return percentile
        



### Scratchpaper
#TODO delete this
business_id = '1SWheh84yJXfytovILXOAQ'
