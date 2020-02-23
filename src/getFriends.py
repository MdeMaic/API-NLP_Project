from pymongo import MongoClient
from errorHandler import jsonErrorHandler
from doubleCheck import findIdMax, checkName, checkEpisode
from bson.json_util import dumps
import pandas as pd
import re
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

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


# Get todos los usuarios registrados --> API
@jsonErrorHandler
def getUsers():
    q = {}
    query = coll_users.find(q, projection={"_id": 0, "user_name": 1, "user_id": 1})
    if not query:
        raise ValueError("No se han encontrado usuarios")
    return dumps(query)


# Get todas las conversaciones de un usuario --> API
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


# get sentimientos de un usuario --> API
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
    lg, cl = df.shape
    negative = df.neg.sum() / lg
    neutral = df.neu.sum() / lg
    positive = df.pos.sum() / lg

    return {
        "user_name": name,
        "ngeative": negative,
        "neutral": neutral,
        "positive": positive,
    }


# get sentimientos de todos los usuarios --> API
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


# get sentimientos de todos los usuarios --> API
@jsonErrorHandler
def getAllSentiment():
    q = {}
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
    lg, cl = df.shape
    negative = df.neg.sum() / lg
    neutral = df.neu.sum() / lg
    positive = df.pos.sum() / lg

    return {
        "ngeative": negative,
        "neutral": neutral,
        "positive": positive,
    }


'''
# Para insertar una quote distinta de la de los personajes de FRIENDS
####PREGUNTAR COMO HACER ESTO.


@jsonErrorHandler
def insertConversation(name):

    name = name.capitalize()
    chk_name = checkName(name, "conversation")

    if chk_name == "OK":
        query = coll_conver.insert_one(
            {
                "user_name": name,
                "msg_id": findIdMax("conversation") + 1,
                "quote": "Esto es una prueba de texto",  ####PREGUNTAR COMO HACER ESTO.
            }
        )
        if not query:
            raise ValueError("Can't insert conversation")
        return f"Conversación añadida al usuario {name}"
    else:
        # raise NameError(chk_name)
        return chk_name


# Para insertar episodios y conversaciones de Friends.

"""
def checkEpisode():

    q = {"episode_number": {"$exists": True}}
    query = coll_conver.find(q, projection={"episode_number": 1, "_id": 0})
    findepisode = list(set([episode for i in query for episode in i.values()]))
    return findepisode
"""


@jsonErrorHandler
def insertEpisode(chapter):

    chapter = int(chapter)
    if type(chapter) != int:  #######PREGUNTAR!
        raise ValueError("Debes introducir un numero de episodio")

    df = pd.read_csv("../input/friends_s1.csv", index_col=0)

    # Check que están registrados los usuarios #######REVISAR
    user_names = list(set(df.user_name))
    for name in user_names:
        if checkName(name, "conversation") != "OK":
            return f"Se van a insertar {user_names}, pero no todos están registrados. Revisalo por favor :)"

    # Check que el episodio está fuera de rango.
    episodes = list(set(df.episode_number))
    max_ep = max(episodes)
    if chapter not in episodes:
        return f"Número de episodio no encontrado en temporada 1. Solo hay {max_ep} episodios"

    # Check que el episodio no está ya en la bbdd.
    if chapter not in checkEpisode():
        df = df[df.episode_number == chapter]
        df_dict = df.to_dict(orient="records")
        query = coll_conver.insert_many(df_dict)
        if not query:
            raise ValueError("No se puede añadir el episodio")
        return f"Conversación del episodio {chapter} correctamente añadida"
    else:
        return f"El episodio {chapter} ya estaba insertado en la base de datos."
'''

