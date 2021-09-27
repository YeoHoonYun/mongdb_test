# $ pip install Flask-PyMongo
# python -m pip install "pymongo[srv]" Flask-PyMongo
from flask import Flask, render_template
from flask_pymongo import PyMongo

app = Flask(__name__)
app.config["MONGO_URI"] = "mongodb+srv://admin:admin@cluster0.8y00i.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
mongo = PyMongo(app)

@app.route("/")
def home_page():
    online_users = mongo.db.users.find({"online": True})
    return render_template("index.html",online_users=online_users)

if __name__ == '__main__':
    app.run()
