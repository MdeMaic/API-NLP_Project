from pymongo import MongoClient

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


def checkEpisode():
    q = {"episode_number": {"$exists": True}}
    query = coll_conver.find(q, projection={"episode_number": 1, "_id": 0})
    findepisode = list(set([episode for i in query for episode in i.values()]))
    return findepisode

