from flask import Flask, request
from insertFriends import insertUser, insertConversation, insertEpisode
from getFriends import (
    getUsers,
    getUserConversation,
    getUserSentiment,
    getAllConver,
    getAllSentiment,
    getMyReco,
)
import random


app = Flask(__name__)


####### SALUDOS #######
@app.route("/")
def hello():
    hello = "Bienvenido a la API de Análisis de Sentimientos. Juega con FRIENDS y descubre su personalidad"
    return hello


def controllerFn():
    return "Samanté para tí"


app.route("/hola")(controllerFn)


@app.route("/homer")
def homer():
    return """
    <img src="https://www.grupoblc.com/wp-content/uploads/2013/10/images_curiosita_homer.jpg">
    <body "Yuhuuuuuu" >
    """


####### INSERT ####### insertUser, insertConversation, insertEpisode
# Insertar usuario
@app.route("/insert/user/<name>")
def insertFriend(name):
    return insertUser(name)


# Insertar conversación de todos los usuarios de un episodio
@app.route("/insert/conver/episode/<episode_number>")
def insertConverEpisode(episode_number):
    return insertEpisode(episode_number)


# Insertar converación de un único usuario de Star Wars
@app.route("/insert/conver/user/<name>")
def insertChat(name):
    return insertConversation(name)


####### GET ####### getUsers, getUserConversation, getUserSentiment, getAllConver, getAllSentiment, getMyReco
# Get all users registrados
@app.route("/get/users")
def getAllUsers():
    return getUsers()


# Get one user conversation
@app.route("/get/conver/user/<name>")
def getConversationUser(name):
    return getUserConversation(name)


# Get one user sentiments
@app.route("/get/sentiment/user/<name>")
def getSentimentUser(name):
    return getUserSentiment(name)


# Get all users conversations
@app.route("/get/conversations")
def getConverAll():
    return getAllConver()


# Get all users sentiments
@app.route("/get/sentiments")
def getSentimentAll():
    return getAllSentiment()


# Get recommendation for a user given
@app.route("/get/recommendation/<name>")
def getReco(name):
    return getMyReco(name)


app.run("0.0.0.0", 5000, debug=True)
