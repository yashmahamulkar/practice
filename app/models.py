from app import db

class ImageProcessingJob(db.Model):
    id = db.Column(db.String, primary_key=True)
    status = db.Column(db.String, nullable=False)
    input_csv_filename = db.Column(db.String, nullable=False)
    output_csv_filename = db.Column(db.String, nullable=True)
    webhook_url = db.Column(db.String, nullable=True)
    def __repr__(self):
        return f"<ImageProcessingJob {self.id}>"
