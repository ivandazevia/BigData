from flask import Blueprint

main = Blueprint('main', __name__)

import json
from engine import RecommendationEngine

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask, request

""""Untuk menampilkan buku <book_id> terbaik direkomendasikan ke sejumlah <count> user"""
@main.route("/books/<int:book_id>/recommend/batch0/<int:count>", methods=["GET"])
def book_recommending0(book_id, count):
    logger.debug("BookId %s TOP user recommending on batch 0", book_id)
    top_rated0 = recommendation_engine.get_top_book_recommend0(book_id, count)
    return json.dumps(top_rated0)

@main.route("/books/<int:book_id>/recommend/batch1/<int:count>", methods=["GET"])
def book_recommending1(book_id, count):
    logger.debug("BookId %s TOP user recommending on batch 1", book_id)
    top_rated1 = recommendation_engine.get_top_book_recommend1(book_id, count)
    return json.dumps(top_rated1)

@main.route("/books/<int:book_id>/recommend/batch2/<int:count>", methods=["GET"])
def book_recommending2(book_id, count):
    logger.debug("BookId %s TOP user recommending on batch 2", book_id)
    top_rated2 = recommendation_engine.get_top_book_recommend2(book_id, count)
    return json.dumps(top_rated2)


""""Untuk menampilkan sejumlah <count> rekomendasi buku ke user <user_id>"""
@main.route("/<int:user_id>/ratings/top/batch0/<int:count>", methods=["GET"])
def top_ratings_batch0(user_id, count):
    logger.debug("User %s TOP ratings for batch 0 requested", user_id)
    top_rated0 = recommendation_engine.get_top_ratings0(user_id, count)
    return json.dumps(top_rated0)

@main.route("/<int:user_id>/ratings/top/batch1/<int:count>", methods=["GET"])
def top_ratings_batch1(user_id, count):
    logger.debug("User %s TOP ratings for batch 1 requested", user_id)
    top_rated1 = recommendation_engine.get_top_ratings1(user_id, count)
    return json.dumps(top_rated1)

@main.route("/<int:user_id>/ratings/top/batch2/<int:count>", methods=["GET"])
def top_ratings_batch2(user_id, count):
    logger.debug("User %s TOP ratings for batch 2 requested", user_id)
    top_rated2 = recommendation_engine.get_top_ratings2(user_id, count)
    return json.dumps(top_rated2)


""""Untuk melakukan prediksi user <user_id> memberi rating X terhadap buku <book_id>"""
@main.route("/<int:user_id>/ratings/batch0/<int:book_id>", methods=["GET"])
def book_ratings_batch0(user_id, book_id):
    logger.debug("User %s rating requested for book on batch 0 %s", user_id, book_id)
    ratings0 = recommendation_engine.get_ratings_for_book_ids0(user_id, book_id)
    return json.dumps(ratings0)

@main.route("/<int:user_id>/ratings/batch1/<int:book_id>", methods=["GET"])
def book_ratings_batch1(user_id, book_id):
    logger.debug("User %s rating requested for book on batch 1 %s", user_id, book_id)
    ratings1 = recommendation_engine.get_ratings_for_book_ids1(user_id, book_id)
    return json.dumps(ratings1)

@main.route("/<int:user_id>/ratings/batch2/<int:book_id>", methods=["GET"])
def book_ratings_batch2(user_id, book_id):
    logger.debug("User %s rating requested for book on batch 2 %s", user_id, book_id)
    ratings2 = recommendation_engine.get_ratings_for_book_ids2(user_id, book_id)
    return json.dumps(ratings2)


""""Untuk melihat riwayat pemberian rating oleh user <user_id>"""
@main.route("/<int:user_id>/history/batch0", methods=["GET"])
def ratings_history_batch0(user_id):
    logger.debug("History for user %s on batch 0 is requested", user_id)
    user_history0 = recommendation_engine.get_history0(user_id)
    return json.dumps(user_history0)

@main.route("/<int:user_id>/history/batch1", methods=["GET"])
def ratings_history_batch1(user_id):
    logger.debug("History for user %s on batch 1 is requested", user_id)
    user_history1 = recommendation_engine.get_history1(user_id)
    return json.dumps(user_history1)

@main.route("/<int:user_id>/history/batch2", methods=["GET"])
def ratings_history_batch2(user_id):
    logger.debug("History for user %s on batch 2 is requested", user_id)
    user_history2 = recommendation_engine.get_history2(user_id)
    return json.dumps(user_history2)    


def create_app(spark_session, dataset_path):
    global recommendation_engine

    recommendation_engine = RecommendationEngine(spark_session, dataset_path)

    app = Flask(__name__)
    app.register_blueprint(main)
    return app