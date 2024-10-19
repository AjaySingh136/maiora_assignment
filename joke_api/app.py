from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
import requests
import os
from sqlalchemy import text
from flasgger import Swagger
from etl_sales_data.meta.config import Jokes_DB_PATH, Jokes_DB_tab_name

# Ensure the database directory exists
db_directory = os.path.dirname(Jokes_DB_PATH)
if not os.path.exists(db_directory):
    os.makedirs(db_directory)

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{os.path.abspath(Jokes_DB_PATH)}'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# Initialize Swagger
swagger = Swagger(app)

print(f"Database will be created at: {os.path.abspath(Jokes_DB_PATH)}")

class Joke(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    category = db.Column(db.String(50))
    joke_type = db.Column(db.String(10))
    joke = db.Column(db.String(255))
    setup = db.Column(db.String(255))
    delivery = db.Column(db.String(255))
    nsfw = db.Column(db.Boolean)
    political = db.Column(db.Boolean)
    sexist = db.Column(db.Boolean)
    safe = db.Column(db.Boolean)
    lang = db.Column(db.String(5))

    def __repr__(self):
        return f'<Joke {self.id}>'

@app.route('/')
def index():
    return "Welcome to the Joke API! Use /fetch_jokes to fetch jokes or /jokes to view them."

@app.route('/fetch_jokes', methods=['POST'])
def fetch_jokes():
    """
    Fetch jokes from an external API and store them in the database.
    ---
    responses:
      201:
        description: Jokes fetched and stored successfully
    """
    for _ in range(10):
        response = requests.get('https://v2.jokeapi.dev/joke/Any?amount=10')
        data = response.json()
        for joke in data.get('jokes', []):
            new_joke = Joke(
                category=joke['category'],
                joke_type=joke['type'],
                nsfw=joke['flags']['nsfw'],
                political=joke['flags']['political'],
                sexist=joke['flags']['sexist'],
                safe=joke['safe'],
                lang=joke['lang'],
                joke=joke['joke'] if joke['type'] == 'single' else None,
                setup=joke['setup'] if joke['type'] == 'twopart' else None,
                delivery=joke['delivery'] if joke['type'] == 'twopart' else None,
            )
            db.session.add(new_joke)
    db.session.commit()
    return jsonify({"message": "Jokes fetched and stored successfully!"}), 201

@app.route('/jokes', methods=['GET'])
def get_jokes():
    """
    Get all stored jokes.
    ---
    responses:
      200:
        description: A list of jokes
        schema:
          type: array
          items:
            properties:
              id:
                type: integer
              category:
                type: string
              type:
                type: string
              joke:
                type: string
              setup:
                type: string
              delivery:
                type: string
              nsfw:
                type: boolean
              political:
                type: boolean
              sexist:
                type: boolean
              safe:
                type: boolean
              lang:
                type: string
    """
    jokes = Joke.query.all()
    return jsonify([{
        "id": joke.id,
        "category": joke.category,
        "type": joke.joke_type,
        "joke": joke.joke,
        "setup": joke.setup,
        "delivery": joke.delivery,
        "nsfw": joke.nsfw,
        "political": joke.political,
        "sexist": joke.sexist,
        "safe": joke.safe,
        "lang": joke.lang,
    } for joke in jokes]), 200

if __name__ == '__main__':
    with app.app_context():
        try:
            print(f"Database path: {Jokes_DB_PATH}")
            db.session.execute(text('SELECT 1'))  # Test the database connection
            db.create_all()  # Create the database tables
        except Exception as e:
            print(f"Database connection error: {e}")
    app.run(debug=True)
