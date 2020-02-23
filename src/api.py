from flask import Flask, request
from tas import queryTas, tas
from companies import getCompanyWithName
from insertFriends import insertUser, insertConversation, insertEpisode
import random

app = Flask(__name__)


####### SALUDOS #######


@app.route("/")
def hello():
    hello = "Bienvenido a la API de Análisis de Sentimientos. Juega con Friends y descubre su personalidad"
    return hello


def controllerFn():
    return "Samanté para tí"


app.route("/hola")(controllerFn)


@app.route("/ta")
def taChooser():
    return random.choice(tas)


@app.route("/ta/<name>")
def taChooserWithName(name):
    print(f"Getting TA data for {name}")
    return queryTas(name)


@app.route("/homer")
def homer():
    return """
    <img src="https://www.grupoblc.com/wp-content/uploads/2013/10/images_curiosita_homer.jpg">
    <body "Yuhuuuuuu" >
    """


"""
@app.route("/company/<name>")
def getCompany(name):
    return getCompanyWithName(name)
"""

####### INSERT #######

# Insertar usuario
@app.route("/insert/user/<name>")
def insertFriend(name):
    return insertUser(name)


# Insertar converación de un único usuario
@app.route("/insert/conver/user/<name>")
def insertChat(name):
    return insertConversation(name)


# Insertar conversación de todos los usuarios de un episodio
@app.route("/insert/conver/episode/<episode_number>")
def insertConverEpisode(episode_number):
    return insertEpisode(episode_number)


####### GET #######

"""
@app.route("/get/conver/user/<name>")
def getConver(name):
    return getConversation(name)
"""

app.run("0.0.0.0", 5000, debug=True)
