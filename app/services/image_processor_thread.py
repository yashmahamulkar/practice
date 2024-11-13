import os
import requests
from PIL import Image
from io import BytesIO
import pandas as pd
import uuid
from flask import current_app
from ..models import db, ImageProcessingJob
from .webhook import trigger_webhook
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG)

def process_images_threaded(app, request_id):
    try:
        # Ensure the Flask app context is explicitly set inside the thread
        with app.app_context():  # Start application context in the thread
            logging.info(f"Starting image processing for request_id: {request_id}")
            
            # Retrieve the job from the database
            job = ImageProcessingJob.query.get(request_id)
            
            if not job:
                logging.error(f"Job with request_id {request_id} not found.")
                return

            # Set the job status to 'processing'
            job.status = 'processing'
            db.session.commit()
            
            # Ensure necessary directories exist
            if not os.path.exists('static/compressed'):
                os.makedirs('static/compressed')
            
            if not os.path.exists('static/output'):
                os.makedirs('static/output')

            # Read input CSV file for image processing
            input_data = pd.read_csv(job.input_csv_filename)
            output_data = []

            for _, row in input_data.iterrows():
                serial_number = row['Serial Number']
                product_name = row['Product Name']
                input_urls = row['Input Image Urls'].split(',')

                output_urls = []
                for url in input_urls:
                    try:
                        url = url.strip()  # Clean up the URL
                        response = requests.get(url)

                        if response.status_code != 200:
                            logging.warning(f"Failed to download image from {url}")
                            continue
                        
                        # Open the image from the URL
                        img = Image.open(BytesIO(response.content))

                        # Compress the image
                        compressed_image = BytesIO()
                        img.save(compressed_image, format='JPEG', quality=50)
                        compressed_image.seek(0)

                        # Save the compressed image locally
                        output_image_path = os.path.join('static/compressed', f'{request_id}_{serial_number}_{uuid.uuid4()}.jpg')
                        with open(output_image_path, 'wb') as f:
                            f.write(compressed_image.getbuffer())

                        output_urls.append(output_image_path)
                    except Exception as e:
                        logging.error(f"Error processing image from {url}: {str(e)}")

                # Collect output data for the CSV
                output_data.append({
                    'Serial Number': serial_number,
                    'Product Name': product_name,
                    'Input Image Urls': ','.join(input_urls),
                    'Output Image Urls': ','.join(output_urls)
                })

            # Save processed data to an output CSV
            output_csv_path = os.path.join('static/output', f'{request_id}_output.csv')
            pd.DataFrame(output_data).to_csv(output_csv_path, index=False)

            # Update job status and add the output CSV path
            job.status = 'completed'
            job.output_csv_filename = output_csv_path
            db.session.commit()

            # Trigger the webhook if provided
            if job.webhook_url:
                trigger_webhook(job.webhook_url, {"request_id": request_id, "status": "completed"})
            
            logging.info(f"Image processing completed for request_id {request_id}")

    except Exception as e:
        logging.error(f"Error during image processing for {request_id}: {str(e)}")