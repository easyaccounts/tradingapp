FROM python:3.12-slim

WORKDIR /app

# Install required packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY *.py .

# Run the signal generator
CMD ["python", "-u", "main.py"]
