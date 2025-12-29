FROM python:3.12-slim

WORKDIR /app

# Install required packages
RUN pip install --no-cache-dir \
    websocket-client \
    psycopg2-binary \
    pytz \
    redis

# Copy application
COPY dhan_200depth_websocket.py .

# Run the collector
CMD ["python", "-u", "dhan_200depth_websocket.py"]
