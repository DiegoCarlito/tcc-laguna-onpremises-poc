# Camada opcional de consumo interativo (JupyterLab) sobre o cluster Spark/S3A
# Ativada exclusivamente sob demanda via: docker compose --profile notebook up -d
FROM laguna-spark:3.5.3-s3a

USER root

RUN mkdir -p /home/spark /tmp/matplotlib \
    && chown -R spark:spark /home/spark /tmp/matplotlib \
    && ln -sf /opt/spark/python/lib/py4j-*-src.zip /opt/spark/python/lib/py4j-src.zip

COPY docker/notebook/requirements-notebook.txt /tmp/requirements-notebook.txt
RUN pip install --no-cache-dir -r /tmp/requirements-notebook.txt

ENV HOME=/home/spark \
    MPLCONFIGDIR=/tmp/matplotlib \
    SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=python3 \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-src.zip

WORKDIR /opt/laguna/notebooks

USER spark
