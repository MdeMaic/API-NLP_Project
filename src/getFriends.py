from pymongo import MongoClient
from errorHandler import jsonErrorHandler
from bson.json_util import dumps
import re
import pandas as pd

# Connect to the database
client = MongoClient(
    "mongodb://localhost/friends"
)  # PARA CONECTAR A MONGODB ATLAS METER API KEY
db = client.get_database()
coll_users = db["users"]
coll_conver = db["conversations"]


def findIdMax(tipo):
    if tipo == "user":
        collect = coll_users
        q = {"user_id": {"$exists": True}}
        query = collect.find(q, projection={"user_id": 1, "_id": 0})
    elif tipo == "conversation":
        collect = coll_conver
        q = {"msg_id": {"$exists": True}}
        query = collect.find(q, projection={"msg_id": 1, "_id": 0})

    findmax = [n for i in query for n in i.values()]
    if findmax == []:
        return 0
    else:
        return max(findmax)


def checkName(name, tipo="user"):

    q = {"user_name": {"$exists": True}}
    query = coll_users.find(q, projection={"user_name": 1, "_id": 0})
    findname = [name for i in query for name in i.values()]

    if name in findname:
        if tipo == "user":
            return f"{name} ya existe en la base de datos. Por favor elige otro nombre."
        elif tipo == "conversation":
            return "OK"
    else:
        if tipo == "user":
            return "OK"
        elif tipo == "conversation":
            return f"{name} NO existe en la base de datos. Utiliza 'insert/user/<user_name>' para crearlo."


@jsonErrorHandler
def insertUser(name):

    name = name.capitalize()
    chk_name = checkName(name, "user")
    if chk_name == "OK":
        query = coll_users.insert_one(
            {"user_name": name, "user_id": findIdMax("user") + 1}
        )
        if not query:
            raise ValueError("No se ha podido insertar el usuario")
        return f"\n\n:)\nGenial!\n\nEl usuario {name} se ha ingresado correctamente"
    else:
        # raise NameError(chk_name)
        return chk_name


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


def checkEpisode():

    q = {"episode_number": {"$exists": True}}
    query = coll_conver.find(q, projection={"episode_number": 1, "_id": 0})
    findepisode = list(set([episode for i in query for episode in i.values()]))
    return findepisode


@jsonErrorHandler
def insertEpisode(chapter):

    chapter = int(chapter)
    if type(chapter) != int:  #######PREGUNTAR!
        raise ValueError("Debes introducir un numero de episodio")
    else:
        df = pd.read_csv("../input/friends_s1.csv", index_col=0)
        episodes = list(set(df.episode_number))
        max_ep = max(episodes)

        if chapter not in episodes:
            return f"Número de episodio no encontrado en temporada 1. Solo hay {max_ep} episodios"
        else:
            if chapter not in checkEpisode():
                df = df[df.episode_number == chapter]
                df_dict = df.to_dict(orient="records")
                query = coll_conver.insert_many(df_dict)
                if not query:
                    raise ValueError("No se puede añadir el episodio")
                return f"Conversación del episodio {chapter} correctamente añadida"
            else:
                return f"El episodio {chapter} ya estaba insertado en la base de datos."


"""
@jsonErrorHandler
def getConversation(name):
    conversation = []

    return dumps(conversation)
"""

