from flask import Flask, request
from tas import queryTas, tas
from companies import getCompanyWithName
import random
app = Flask(__name__)


@app.route('/')
def hello():
    pepe = {
        "nombre": "Luis",
        "edad": 30
    }
    return pepe


def controllerFn(): return {"hola": "pepe"}


app.route('/hola')(controllerFn)


@app.route('/ta')
def taChooser():
    return random.choice(tas)


@app.route('/ta/<name>')
def taChooserWithName(name):
    print(f"Getting TA data for {name}")
    return queryTas(name)


@app.route('/homer')
def homer():
    return """
    <img src="https://www.grupoblc.com/wp-content/uploads/2013/10/images_curiosita_homer.jpg">
    """


@app.route('/company/<name>')
def getCompany(name):
    return getCompanyWithName(name)


app.run("0.0.0.0", 5000, debug=True)