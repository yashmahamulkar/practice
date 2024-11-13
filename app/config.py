import os

class Config:
    SQLALCHEMY_DATABASE_URI = 'sqlite:///../instance/image_processing.db'
    SQLALCHEMY_TRACK_MODIFICATIONS = True
    UPLOAD_FOLDER = 'static/temp/'
    COMPRESSED_FOLDER = 'static/compressed/'
    OUTPUT_FOLDER = 'static/output/'
    MAX_WORKERS = 10
    # Number of threads in thread pool
    