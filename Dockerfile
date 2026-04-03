# ============================================================
# Spark Learning Environment
# Base: Python 3.11 + Java 11 + PySpark 3.5 + Jupyter
# ============================================================

FROM python:3.11-slim

LABEL maintainer="Spark Learner"
LABEL description="PySpark + Jupyter learning environment for the 7-Phase Spark course"

# ── System dependencies ───────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    wget \
    curl \
    procps \
    git \
    vim \
    && rm -rf /var/lib/apt/lists/*

# ── Java home ────────────────────────────────────────────────
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ── Spark environment ─────────────────────────────────────────
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# ── Download & install Spark ──────────────────────────────────
RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && tar -xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && mv "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" "${SPARK_HOME}" \
    && rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# ── Delta Lake JAR ────────────────────────────────────────────
RUN wget -q "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar" \
    -O "${SPARK_HOME}/jars/delta-spark_2.12-3.0.0.jar" \
    && wget -q "https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar" \
    -O "${SPARK_HOME}/jars/delta-storage-3.0.0.jar"

# ── Python packages ───────────────────────────────────────────
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# ── Working directory ─────────────────────────────────────────
WORKDIR /workspace

# ── Jupyter config ─────────────────────────────────────────────
RUN jupyter notebook --generate-config && \
    echo "c.NotebookApp.ip = '0.0.0.0'" >> /root/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.port = 8888" >> /root/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.open_browser = False" >> /root/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.token = ''" >> /root/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.password = ''" >> /root/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.allow_root = True" >> /root/.jupyter/jupyter_notebook_config.py

# ── Expose ports ──────────────────────────────────────────────
# 8888  → Jupyter Notebook
# 4040  → Spark Application UI
# 8080  → Spark Master UI
# 7077  → Spark Master (internal)
EXPOSE 8888 4040 8080 7077

CMD ["jupyter", "notebook", "--notebook-dir=/workspace", "--allow-root"]
