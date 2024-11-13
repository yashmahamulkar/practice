from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from .config import Config
from flask_migrate import Migrate

# Initialize db without tying it to the app just yet
db = SQLAlchemy()

def create_app():
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_object(Config)


    with app.app_context():
        db.init_app(app)
    

    migrate = Migrate(app, db)
  
    with app.app_context():
        from .routes.api import api_blueprint
        app.register_blueprint(api_blueprint)

    # Import models inside app context as well
    with app.app_context():
        from . import models

    return app
