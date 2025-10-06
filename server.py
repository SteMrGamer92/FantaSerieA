from flask import Flask
app = Flask(__name__)  # 'app' is the variable name

@app.route('/')
def home():
    return "Benvenuto in FantaSerieA!"