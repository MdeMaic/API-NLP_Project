from pymongo import MongoClient
import pandas as pd
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# nltk model form
nltk.download("punkt")
nltk.download("vader_lexicon")
sia = SentimentIntensityAnalyzer()

# Connect to the database
client = MongoClient(
    "mongodb://localhost/friends"
)  # PARA CONECTAR A MONGODB ATLAS METER API KEY
db = client.get_database()
coll_users = db["users"]
coll_conver = db["conversations"]


def findIdMax(tipo):
    """
    Encuentra el ID máximo para no duplicar iDs. 
    Se usa para añadir el maxID+1 al siguiente documento.
    """
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
    """
    Hace check del nombre en función de si es la colección "user" o "conversacion".
    Se usa para evitar inconsistencias insertando usuarios que ya están.
    """
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


def checkEpisode():
    """
    Hace check del episodio y devuelve una lista de los episodios insertados. 
    Se usa para asegurar que no se incluyen en la bbdd epiosdios repetidos.
    """
    q = {"episode_number": {"$exists": True}}
    query = coll_conver.find(q, projection={"episode_number": 1, "_id": 0})
    findepisode = list(set([episode for i in query for episode in i.values()]))
    return findepisode


def getMySentMatrix():
    """
    Devuelve la matriz de sentimientos de los usuarios registrados. 
    Útil para analizar sentimientos y realizar recomendaciones.
    """
    q = {}
    query_quote = coll_conver.find(q, projection={"_id": 0, "quote": 1})
    query_user = coll_conver.find(q, projection={"_id": 0, "user_name": 1})

    lst_quote = list(query_quote)
    lst_user = list(query_user)

    count = 0
    df = pd.DataFrame(columns=["neg", "neu", "pos", "compound"]).T

    for elem in lst_quote:
        for quote in elem.values():
            sent = sia.polarity_scores(quote)
            df[count] = sent.values()
            count += 1
    df = df.T

    names = [user for elem in lst_user for user in elem.values()]

    df["user_name"] = names

    df = (
        df.groupby("user_name")
        .mean()
        .round(4)
        .sort_values("compound", ascending=False)
        .reset_index()
    )

    return df

