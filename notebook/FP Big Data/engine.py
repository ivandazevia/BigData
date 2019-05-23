import os
import logging
import pandas as pd

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql import types
from pyspark.sql.functions import explode
import pyspark.sql.functions as func

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RecommendationEngine:
    """A book recommendation engine"""
    def __train_model(self):
        """Train the ALS model with the current dataset"""

        logger.info("--Sedang dilakukan training pada ALS Mode--")
        self.als = ALS(maxIter=5, regParam=0.01, userCol="uid", itemCol="iid_int", ratingCol="rating_int",
                  coldStartStrategy="drop")
        self.model0 = self.als.fit(self.ratingsdf0)
        self.model1 = self.als.fit(self.ratingsdf1)
        self.model2 = self.als.fit(self.ratingsdf2)
        logger.info("ALS model berhasil dibuat")

    """Recommends up to books_count top unrated books to user_id"""
    def get_top_ratings0(self, user_id, books_count):        
        users = self.ratingsdf0.select(self.als.getUserCol())
        users = users.filter(users.uid == user_id)
        userSubsetRecs0 = self.model0.recommendForUserSubset(users, books_count)
        userSubsetRecs0 = userSubsetRecs0.withColumn("recommendations", explode("recommendations"))
        userSubsetRecs0 = userSubsetRecs0.select(func.col('uid'),
                                               func.col('recommendations')['iid_int'].alias('iid_int'),
                                               func.col('recommendations')['Rating'].alias('Rating')).\
                                                                                    drop('recommendations')
        userSubsetRecs0 = userSubsetRecs0.drop('Rating')
        userSubsetRecs0 = userSubsetRecs0.join(self.booksdf, ("iid_int"), 'inner')
        userSubsetRecs0 = userSubsetRecs0.toPandas()
        userSubsetRecs0 = userSubsetRecs0.to_json()
        return userSubsetRecs0

    def get_top_ratings1(self, user_id, books_count):        
        users = self.ratingsdf1.select(self.als.getUserCol())
        users = users.filter(users.uid == user_id)
        userSubsetRecs1 = self.model1.recommendForUserSubset(users, books_count)
        userSubsetRecs1 = userSubsetRecs1.withColumn("recommendations", explode("recommendations"))
        userSubsetRecs1 = userSubsetRecs1.select(func.col('uid'),
                                               func.col('recommendations')['iid_int'].alias('iid_int'),
                                               func.col('recommendations')['Rating'].alias('Rating')).\
                                                                                    drop('recommendations')
        userSubsetRecs1 = userSubsetRecs1.drop('Rating')
        userSubsetRecs1 = userSubsetRecs1.join(self.booksdf, ("iid_int"), 'inner')
        userSubsetRecs1 = userSubsetRecs1.toPandas()
        userSubsetRecs1 = userSubsetRecs1.to_json()
        return userSubsetRecs1

    def get_top_ratings2(self, user_id, books_count):        
        users = self.ratingsdf2.select(self.als.getUserCol())
        users = users.filter(users.uid == user_id)
        userSubsetRecs2 = self.model2.recommendForUserSubset(users, books_count)
        userSubsetRecs2 = userSubsetRecs2.withColumn("recommendations", explode("recommendations"))
        userSubsetRecs2 = userSubsetRecs2.select(func.col('uid'),
                                               func.col('recommendations')['iid_int'].alias('iid_int'),
                                               func.col('recommendations')['Rating'].alias('Rating')).\
                                                                                    drop('recommendations')
        userSubsetRecs2 = userSubsetRecs2.drop('Rating')
        userSubsetRecs2 = userSubsetRecs2.join(self.booksdf, ("iid_int"), 'inner')
        userSubsetRecs2 = userSubsetRecs2.toPandas()
        userSubsetRecs2 = userSubsetRecs2.to_json()
        return userSubsetRecs2


    """Recommends up to books_count top unrated books to user_id"""
    def get_top_book_recommend0(self, book_id, user_count):
        books0 = self.ratingsdf0.select(self.als.getItemCol())
        books0 = books0.filter(books0.iid_int == book_id)
        bookSubsetRecs0 = self.model0.recommendForItemSubset(books0, user_count)
        bookSubsetRecs0 = bookSubsetRecs0.withColumn("recommendations", explode("recommendations"))
        bookSubsetRecs0 = bookSubsetRecs0.select(func.col('iid_int'),
                                                 func.col('recommendations')['uid'].alias('uid'),
                                                 func.col('recommendations')['Rating'].alias('Rating')).\
                                                                                        drop('recommendations')
        bookSubsetRecs0 = bookSubsetRecs0.drop('Rating')
        bookSubsetRecs0 = bookSubsetRecs0.join(self.booksdf, ("iid_int"), 'inner')
        bookSubsetRecs0 = bookSubsetRecs0.toPandas()
        bookSubsetRecs0 = bookSubsetRecs0.to_json()
        return bookSubsetRecs0

    def get_top_book_recommend1(self, book_id, user_count):
        books1 = self.ratingsdf1.select(self.als.getItemCol())
        books1 = books1.filter(books1.iid_int == book_id)
        bookSubsetRecs1 = self.model1.recommendForItemSubset(books1, user_count)
        bookSubsetRecs1 = bookSubsetRecs1.withColumn("recommendations", explode("recommendations"))
        bookSubsetRecs1 = bookSubsetRecs1.select(func.col('iid_int'),
                                                 func.col('recommendations')['uid'].alias('uid'),
                                                 func.col('recommendations')['Rating'].alias('Rating')).\
                                                                                        drop('recommendations')
        bookSubsetRecs1 = bookSubsetRecs1.drop('Rating')
        bookSubsetRecs1 = bookSubsetRecs1.join(self.booksdf, ("iid_int"), 'inner')
        bookSubsetRecs1 = bookSubsetRecs1.toPandas()
        bookSubsetRecs1 = bookSubsetRecs1.to_json()
        return bookSubsetRecs1

    def get_top_book_recommend2(self, book_id, user_count):
        books2 = self.ratingsdf2.select(self.als.getItemCol())
        books2 = books2.filter(books2.iid_int == book_id)
        bookSubsetRecs2 = self.model2.recommendForItemSubset(books2, user_count)
        bookSubsetRecs2 = bookSubsetRecs2.withColumn("recommendations", explode("recommendations"))
        bookSubsetRecs2 = bookSubsetRecs2.select(func.col('iid_int'),
                                                 func.col('recommendations')['uid'].alias('uid'),
                                                 func.col('recommendations')['Rating'].alias('Rating')).\
                                                                                        drop('recommendations')
        bookSubsetRecs2 = bookSubsetRecs2.drop('Rating')
        bookSubsetRecs2 = bookSubsetRecs2.join(self.booksdf, ("iid_int"), 'inner')
        bookSubsetRecs2 = bookSubsetRecs2.toPandas()
        bookSubsetRecs2 = bookSubsetRecs2.to_json()
        return bookSubsetRecs2


    """Get rating history for a user"""
    def get_history0(self, user_id):
        self.ratingsdf0.createOrReplaceTempView("ratingsdata0")
        user_history0 = self.spark_session.sql('SELECT uid, iid_int, rating_int from ratingsdata0 where uid = "%s"' %user_id)
        user_history0 = user_history0.join(self.booksdf, ("iid_int"), 'inner')
        user_history0 = user_history0.toPandas()
        user_history0 = user_history0.to_json()
        return user_history0

    def get_history1(self, user_id):
        self.ratingsdf1.createOrReplaceTempView("ratingsdata1")
        user_history1 = self.spark_session.sql('SELECT uid, iid_int, rating_int from ratingsdata1 where uid = "%s"' %user_id)
        user_history1 = user_history1.join(self.booksdf, ("iid_int"), 'inner')
        user_history1 = user_history1.toPandas()
        user_history1 = user_history1.to_json()
        return user_history1

    def get_history2(self, user_id):
        self.ratingsdf2.createOrReplaceTempView("ratingsdata2")
        user_history2 = self.spark_session.sql('SELECT uid, iid_int, rating_int from ratingsdata2 where uid = "%s"' %user_id)
        user_history2 = user_history2.join(self.booksdf, ("iid_int"), 'inner')
        user_history2 = user_history2.toPandas()
        user_history2 = user_history2.to_json()
        return user_history2


    """Given a user_id and a list of book_ids, predict ratings for them"""
    def get_ratings_for_book_ids0(self, user_id, book_id):        
        request0 = self.spark_session.createDataFrame([(user_id, book_id)], ["uid", "iid_int"])
        ratings0 = self.model0.transform(request0).collect()
        return ratings0

    def get_ratings_for_book_ids1(self, user_id, book_id):        
        request1 = self.spark_session.createDataFrame([(user_id, book_id)], ["uid", "iid_int"])
        ratings1 = self.model1.transform(request1).collect()
        return ratings1

    def get_ratings_for_book_ids2(self, user_id, book_id):        
        request2 = self.spark_session.createDataFrame([(user_id, book_id)], ["uid", "iid_int"])
        ratings2 = self.model2.transform(request2).collect()
        return ratings2


    def __init__(self, spark_session, dataset_path):
        """Init the recommendation engine given a Spark context and a dataset path"""
        logger.info("Sistem sedang dijalankan")
        self.spark_session = spark_session

        # Load ratings data for later use
        logger.info("Data Rating sedang di proses~~~")

        ratings_file_path0 = os.path.join(dataset_path, 'model0.csv')        
        self.ratingsdf0 = spark_session.read.csv(ratings_file_path0, header=False, inferSchema=True).na.drop()
        new_df0 = spark_session.read.csv(ratings_file_path0, header=False, inferSchema=True).na.drop()
        self.ratingsdf0 = self.ratingsdf0.union(new_df0)
        self.ratingsdf0 = self.ratingsdf0.drop("rating","iid")

        ratings_file_path1 = os.path.join(dataset_path, 'model1.csv')
        self.ratingsdf1 = spark_session.read.csv(ratings_file_path1, header=False, inferSchema=True).na.drop()
        new_df1 = spark_session.read.csv(ratings_file_path1, header=False, inferSchema=True).na.drop()
        self.ratingsdf1 = self.ratingsdf1.union(new_df1)
        self.ratingsdf1 = self.ratingsdf1.drop("rating","iid")

        ratings_file_path2 = os.path.join(dataset_path, 'model2.csv')
        self.ratingsdf2 = spark_session.read.csv(ratings_file_path2, header=False, inferSchema=True).na.drop()
        new_df2 = spark_session.read.csv(ratings_file_path2, header=False, inferSchema=True).na.drop()
        self.ratingsdf2 = self.ratingsdf2.union(new_df2)
        self.ratingsdf2 = self.ratingsdf2.drop("rating","iid")

        # Load books data for later use
        logger.info("Data Buku sedang di proses~~~")
        books_file_path = os.path.join(dataset_path, 'BookDetail.csv')
        self.booksdf = spark_session.read.csv(books_file_path, header=True, inferSchema=True).na.drop()
        self.booksdf = self.booksdf.drop("iid")

        # Train the model
        self.ratingsdf0 = self.ratingsdf0.selectExpr("_c0 as uid", "_c1 as iid", "_c2 as rating", "_c3 as iid_int", "_c4 as rating_int")    
        self.ratingsdf1 = self.ratingsdf1.selectExpr("_c0 as uid", "_c1 as iid", "_c2 as rating", "_c3 as iid_int", "_c4 as rating_int")        
        self.ratingsdf2 = self.ratingsdf2.selectExpr("_c0 as uid", "_c1 as iid", "_c2 as rating", "_c3 as iid_int", "_c4 as rating_int")    

        self.__train_model()