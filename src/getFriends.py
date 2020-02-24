from pymongo import MongoClient
from errorHandler import jsonErrorHandler
from doubleCheck import findIdMax, checkName, checkEpisode, getMySentMatrix
from bson.json_util import dumps
import pandas as pd
import re
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from scipy.spatial.distance import pdist, squareform

# nltk model form
nltk.download("punkt")
nltk.download("vader_lexicon")
sia = SentimentIntensityAnalyzer()

# Connect to the database
client = MongoClient("mongodb://localhost/friends")

# PARA CONECTAR A MONGODB ATLAS METER API KEY

# Asignar variables a las colecciones para evitar que se reinicien en cada función.
db = client.get_database()
coll_users = db["users"]
coll_conver = db["conversations"]


# Get todos los usuarios registrados --> API @app.route("/get/users")
@jsonErrorHandler
def getUsers():
    q = {}
    query = coll_users.find(q, projection={"_id": 0, "user_name": 1, "user_id": 1})
    if not query:
        raise ValueError("No se han encontrado usuarios")
    return dumps(query)


# Get todas las conversaciones de un usuario --> API @app.route("/get/conver/user/<name>")
@jsonErrorHandler
def getUserConversation(name):
    name = name.capitalize()
    q = {"user_name": name}
    query = coll_conver.find(
        q,
        projection={
            "_id": 0,
            "user_order": 1,
            "episode_number": 1,
            "quote_order": 1,
            "quote": 1,
        },
    )
    if not query:
        raise ValueError("No se han encontrado usuarios")
    return dumps(query)


# get sentimientos de un usuario --> API @app.route("/get/sentiment/user/<name>")
@jsonErrorHandler
def getUserSentiment(name):
    name = name.capitalize()
    q = {"user_name": name}
    query = coll_conver.find(q, projection={"_id": 0, "quote": 1})
    if not query:
        raise ValueError("No se han encontrado usuarios")

    lst = list(query)
    count = 0
    df = pd.DataFrame(columns=["neg", "neu", "pos", "compound"]).T
    for elem in lst:
        for quote in elem.values():
            sent = sia.polarity_scores(quote)
            df[count] = sent.values()
            count += 1
    df = df.T
    negative = df.neg.mean()
    neutral = df.neu.mean()
    positive = df.pos.mean()

    return {
        "user_name": name,
        "negative": (negative * 100).round(2),
        "neutral": (neutral * 100).round(2),
        "positive": (positive * 100).round(2),
    }


# get sentimientos de todos los usuarios --> API @app.route("/get/conversations")
@jsonErrorHandler
def getAllConver():
    q = {}
    query = coll_conver.find(
        q,
        projection={
            "_id": 0,
            "msg_id": 1,
            "user_name": 1,
            "user_order": 1,
            "episode_number": 1,
            "quote_order": 1,
            "quote": 1,
        },
    ).sort("msg_id")
    if not query:
        raise ValueError("No se han encontrado usuarios")
    return dumps(query)


# get sentimientos de todos los usuarios --> API @app.route("/get/sentiments")
@jsonErrorHandler
def getAllSentiment():
    df = getMySentMatrix()
    df_json = df.to_json(orient="records")

    return df_json


# get recommendation --> API @app.route("/get/recommendation/<name>")
@jsonErrorHandler
def getMyReco(name):
    name = name.capitalize()
    df = getMySentMatrix()

    # check name in the matrix
    if name not in list(df.user_name):
        raise ValueError(
            "No se han encontrado usuario o no tiene registrada conversación"
        )

    df = df.set_index("user_name")
    df = df.T
    distances = pd.DataFrame(
        1 / (1 + squareform(pdist(df.T, "euclidean"))),
        index=df.columns,
        columns=df.columns,
    )
    similarities = distances[name].sort_values(ascending=False)[1:].reset_index()
    similarities = similarities.to_json(orient="records")
    return similarities

