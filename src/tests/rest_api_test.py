
from flask import Flask, request, jsonify

app = Flask(__name__)

countries = [
    {"id": 1, "name": "Thailand", "capital": "Bangkok", "area": 513120},
    {"id": 2, "name": "Australia", "capital": "Canberra", "area": 7617930},
    {"id": 3, "name": "Egypt", "capital": "Cairo", "area": 1010408},
]

@app.get("/countries")
def get_countries():
    return jsonify(countries)


@app.get("/countries/<int:id>")
def get_country_by_id(id: int):
    return jsonify(countries[id - 1])

@app.get("/countries/<string:name>")
def get_country_by_name(name: str):
    return jsonify([country for country in countries if country["name"] == name])
