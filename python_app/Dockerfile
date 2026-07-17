FROM python:3.10-slim

# No system build deps needed — pure-sasl is pure Python, no C compilation required.
WORKDIR /app

# Copy requirements first for layer caching
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# App code will be volume-mounted at /app
EXPOSE 8501

CMD ["python", "/app/main.py"]