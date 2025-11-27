FROM python:3.8-slim

WORKDIR /app

# Environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Install gunicorn for production
RUN pip install gunicorn

# Copy project files
COPY . .

# Create static directory and collect static files
RUN mkdir -p /app/staticfiles
ENV STATIC_ROOT=/app/staticfiles
RUN python manage.py collectstatic --noinput || true

# Expose port (Render uses PORT env variable)
EXPOSE 8000

# Use gunicorn for production instead of runserver
CMD gunicorn plmp_backend.wsgi:application --bind 0.0.0.0:$PORT --workers 3 --timeout 360 --threads 2
