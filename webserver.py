from flask import Flask
from flask_httpauth import HTTPBasicAuth
import os

app = Flask(__name__)
auth = HTTPBasicAuth()

@auth.verify_password
def authenticate(username, password):
    if username and password:
        if username == os.environ["WEBAPP_USERNAME"] and password==os.environ["WEBAPP_PASSWORD"]
            return True
    return False

@app.route("/test-auth")
@auth.login_required
def test_auth():
    return "You're in"

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001)

