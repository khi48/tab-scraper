FROM python:3.13-slim

# Set working directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY app.py .

# Expose the port the app runs on
EXPOSE 3000

# Use gunicorn with host 0.0.0.0 to allow external access
# CMD ["gunicorn", "--bind", "0.0.0.0:3000", "--workers", "1", "app:app"]
CMD ["python", "app.py"]