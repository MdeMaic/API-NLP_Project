from errorHandler import jsonErrorHandler

tas = [
    {"nombre": "Marc"},
    {"nombre": "Felipe"},
    {"nombre": "Blanca"},
    {"nombre": "Clara"},
    {"nombre": "Ovi"}
]


@jsonErrorHandler
def queryTas(name):

    s = [ta for ta in tas if ta["nombre"].lower() == name.lower()]
    if len(s) > 0:
        return s[0]
    raise NameError(f"Not found ta with name {name}")