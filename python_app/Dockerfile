FROM python:3.10-slim

# Install system build deps for sasl/pyhive
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends gcc g++ libsasl2-dev libsasl2-modules && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first for layer caching
COPY requirements.txt .

# التعديل هنا: زيادة الـ timeout لـ 1000 ثانية وإضافة محاولات إعادة الاتصال
RUN pip install --no-cache-dir --default-timeout=1000 --retries 10 -r requirements.txt

# App code will be volume-mounted at /app
CMD ["python", "/app/main.py"]