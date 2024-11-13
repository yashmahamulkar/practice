import os
import uuid
import logging
from flask import Blueprint, request, jsonify, current_app
from werkzeug.utils import secure_filename
from concurrent.futures import ThreadPoolExecutor
from ..models import db, ImageProcessingJob
from ..services.image_processor import process_images  # Import the image processing logic

api_blueprint = Blueprint('api', __name__)

logging.basicConfig(level=logging.INFO)

executor = ThreadPoolExecutor(max_workers=4)  
UPLOAD_FOLDER = 'uploads'
COMPRESSED_FOLDER = 'static/compressed'
OUTPUT_FOLDER = 'static/output'

for folder in [UPLOAD_FOLDER, COMPRESSED_FOLDER, OUTPUT_FOLDER]:
    os.makedirs(folder, exist_ok=True)

# Allowed file extensions
ALLOWED_EXTENSIONS = {'csv'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@api_blueprint.route('/upload', methods=['POST'])
def upload_file():
    try:

        if 'file' not in request.files:
            return jsonify({"error": "No file part in the request."}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "No file selected for uploading."}), 400

        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            
            job_id = str(uuid.uuid4())

            # Define where the file will be stored
            input_csv_filename = os.path.join(UPLOAD_FOLDER, f'{job_id}_input.csv')

            # Save the file in a separate thread (non-blocking)
            executor.submit(save_file, file, input_csv_filename)

            # Start image processing in the background with app context
            executor.submit(run_process_images, job_id, input_csv_filename)

            # Save job to the database (using app context)
            with current_app.app_context():  # Ensure app context is available here as well
                job = ImageProcessingJob(
                    id=job_id,
                    input_csv_filename=input_csv_filename,
                    status='pending'
                )
                db.session.add(job)
                db.session.commit()

            return jsonify({"message": "File uploaded and processing started.", "id": job_id}), 200
        else:
            return jsonify({"error": "Allowed file types are CSV."}), 400

    except Exception as e:
        logging.error(f"Error during file upload: {e}")
        return jsonify({"error": "Internal server error."}), 500


def run_process_images(job_id, input_csv_filename):
    try:
        logging.info(f"Processing job {job_id}.")

        # Push the application context manually before accessing Flask features
        with current_app.app_context():  # This will ensure the context is pushed
            # Manual session management is not needed
            job = ImageProcessingJob.query.filter_by(id=job_id).first()
            if job:
                job.status = 'in_progress'
                db.session.commit()  # Commit job status change

            # Call the image processing function
            process_images(job_id, input_csv_filename)

            # After processing, update the job status to 'completed'
            job = ImageProcessingJob.query.filter_by(id=job_id).first()
            if job:
                job.status = 'completed'
                db.session.commit()
            logging.info(f"Finished processing job {job_id}")

    except Exception as e:
        logging.error(f"Error in background processing for job_id {job_id}: {e}")
        db.session.rollback()
def save_file(file, filename):
    try:
        
        with current_app.app_context():
            file.save(filename)
            logging.info(f"File saved to {filename}")
    except Exception as e:
        logging.error(f"Error saving file {filename}: {e}")
