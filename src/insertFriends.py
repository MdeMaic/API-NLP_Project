from pymongo import MongoClient
from errorHandler import jsonErrorHandler
from doubleCheck import findIdMax, checkName, checkEpisode
from bson.json_util import dumps
import pandas as pd

# Connect to the database
client = MongoClient("mongodb://localhost/friends")
######### PARA CONECTAR A MONGODB ATLAS METER API KEY

# Asignar variables a las colecciones para evitar que se reinicien en cada función.
db = client.get_database()
coll_users = db["users"]
coll_conver = db["conversations"]


@jsonErrorHandler
def insertUser(name):
    """
    Para insertar un usuario en la colección users. @app.route("/insert/user/<name>")
    """
    name = name.capitalize()
    chk_name = checkName(name, "user")
    if chk_name == "OK":
        query = coll_users.insert_one(
            {"user_name": name, "user_id": findIdMax("user") + 1}
        )
        if not query:
            raise ValueError("No se ha podido insertar el usuario")
        return f"Perfecto! El usuario {name} se ha ingresado correctamente"
    else:
        # raise NameError(chk_name)
        return chk_name


@jsonErrorHandler
def insertEpisode(chapter):
    """
    Para insertar episodios y conversaciones de Friends. @app.route("/insert/conver/episode/<episode_number>")
    """
    chapter = int(chapter)
    if type(chapter) != int:
        raise ValueError("Debes introducir un numero de episodio")

    df = pd.read_csv("../input/friends_s1.csv", index_col=0)

    # Check que están registrados los usuarios
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
        return f"Conversación del episodio {chapter} añadida correctamente"
    else:
        return f"El episodio {chapter} ya estaba insertado en la base de datos."


@jsonErrorHandler
def insertConversation(name):
    """
    Para insertar quotes de los personajes de Star Wars. @app.route("/insert/conver/user/<name>")
    """
    name = name.capitalize()
    chk_name = checkName(name, "conversation")

    if chk_name == "OK":

        df = pd.read_csv("../input/starwars.csv", index_col=0)
        df = df[df.user_name == name]
        df_dict = df.to_dict(orient="records")

        query = coll_conver.insert_many(df_dict)

        if not query:
            raise ValueError("Can't insert conversation")
        return f"Conversación añadida al usuario {name}"
    else:
        # raise NameError(chk_name)
        return chk_name
