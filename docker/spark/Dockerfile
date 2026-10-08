FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PYSPARK_PYTHON=/usr/local/bin/python
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python

WORKDIR /app

RUN pip install --no-cache-dir \
    pyspark==4.2.0 \
    delta-spark==4.4.0 \
    "pandas<3" \
    pyarrow \
    "psycopg[binary]" \
    python-dotenv

CMD ["python", "databricks/silver/transform.py"]