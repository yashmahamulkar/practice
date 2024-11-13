import os
import uuid
import csv
import logging
import re
import asyncio
import aiohttp
from PIL import Image
from io import BytesIO
from flask import current_app
from flask_executor import Executor
from ..models import db, ImageProcessingJob
from .webhook import trigger_webhook

logging.basicConfig(level=logging.INFO)

executor = Executor()

def is_valid_url(url):
    regex = re.compile(
        r'^(?:http|ftp)s?://'  
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.))'  
        r'(?::\d+)?'
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return re.match(regex, url) is not None


def write_csv(output_data, request_id):
    try:
        output_csv_path = os.path.join('static/output', f'{request_id}_output.csv')
        os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)

        with open(output_csv_path, 'w', newline='', encoding='utf-8') as out_file:
            fieldnames = ['Serial Number', 'Product Name', 'Input Image Urls', 'Output Image Urls']
            writer = csv.DictWriter(out_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(output_data)
        
        logging.info(f"Output CSV saved to {output_csv_path}")
    except Exception as e:
        logging.error(f"Error writing CSV for request_id {request_id}: {e}")

async def process_images(request_id):
    try:
        logging.info(f"Starting image processing for request_id: {request_id}")

        # Ensure the Flask app context is available before database interaction
        with current_app.app_context():  # Push the app context explicitly
            job = ImageProcessingJob.query.get(request_id)
            
            if not job:
                logging.error(f"Job with request_id {request_id} not found.")
                return

            job.status = 'processing'
            db.session.commit()

        compressed_dir = 'static/compressed'
        output_dir = 'static/output'
        os.makedirs(compressed_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)

        try:
            with open(job.input_csv_filename, 'r', newline='', encoding='utf-8') as file:
                reader = csv.reader(file)
                header = next(reader) 
                output_data = []
                tasks = []
                for row in reader:
                    tasks.append(process_row(row, request_id, output_data))

                await asyncio.gather(*tasks)

            if output_data:
                write_csv(output_data, request_id)
            else:
                logging.warning(f"No valid output data to save for request_id {request_id}.")

        except FileNotFoundError:
            logging.error(f"Input CSV file {job.input_csv_filename} not found.")
            with current_app.app_context():
                job.status = 'failed'
                db.session.commit()
            return
        except Exception as e:
            logging.error(f"Error reading CSV file for request_id {request_id}: {e}")
            with current_app.app_context():
                job.status = 'failed'
                db.session.commit()
            return

       
        with current_app.app_context():  
            job = ImageProcessingJob.query.get(request_id)
            if job:
                job.status = 'completed'
                job.output_csv_filename = os.path.join(output_dir, f'{request_id}_output.csv')
                db.session.commit()

        # Trigger webhook if specified
        if job.webhook_url:
            try:
                trigger_webhook(job.webhook_url, {"request_id": request_id, "status": "completed"})
            except Exception as e:
                logging.error(f"Error triggering webhook for job_id {request_id}: {e}")

        logging.info(f"Image processing completed for request_id {request_id}")

    except Exception as e:
        logging.error(f"Error during image processing for {request_id}: {e}")
       
        with current_app.app_context(): 
            job = ImageProcessingJob.query.get(request_id)
            if job:
                job.status = 'failed'
                db.session.commit()


async def process_row(row, request_id, output_data):
    try:
        serial_number = row[0].strip()
        product_name = row[1].strip()
        input_urls = [url.strip() for url in row[2:]]

        logging.debug(f"Extracted URLs for Serial Number {serial_number}: {input_urls}")

        if not input_urls or any(not is_valid_url(url) for url in input_urls):
            logging.error(f"Invalid URLs found in 'Input Image Urls' for Serial Number {serial_number}. Skipping row.")
            return

        output_urls = []
        logging.info(f"Processing URLs for Serial Number {serial_number}: {input_urls}")

        tasks = [process_image(url, request_id, serial_number) for url in input_urls]
        results = await asyncio.gather(*tasks)

        for output_path in results:
            if output_path:
                output_urls.append(output_path)

        if output_urls:
            output_data.append({
                'Serial Number': serial_number,
                'Product Name': product_name,
                'Input Image Urls': ','.join(input_urls),
                'Output Image Urls': ','.join(output_urls)
            })
    except Exception as e:
        logging.error(f"Error processing row for Serial Number {row[0]}: {e}")


async def process_image(url, request_id, serial_number):
    try:
        if not is_valid_url(url):
            logging.warning(f"Skipping invalid URL: {url}")
            return None

        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as response:
                if response.status != 200:
                    logging.warning(f"Failed to download image from {url}, status code: {response.status}")
                    return None

                img_data = await response.read()
                img = Image.open(BytesIO(img_data))

                # Compress and save the image
                output_image_filename = f"{request_id}_{serial_number}_{uuid.uuid4().hex}.jpg"
                output_image_path = os.path.join('static/compressed', output_image_filename)
                img.save(output_image_path, format='JPEG', quality=50)

                logging.info(f"Image saved to {output_image_path}")
                return output_image_path

    except aiohttp.ClientError as e:
        logging.error(f"Request error downloading image from {url}: {e}")
        return None
    except IOError as e:
        logging.error(f"I/O error processing image from {url}: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error processing image from {url}: {e}")
        return None


def trigger_image_processing(request_id):
    with current_app.app_context(): 
        executor.submit(process_images, request_id)
