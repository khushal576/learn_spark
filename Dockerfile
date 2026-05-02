# Jupyter + PySpark on top of official Apache Spark image
# Master/Worker use apache/spark directly (no Dockerfile needed)

FROM apache/spark:3.5.3

USER root

RUN pip3 install --no-cache-dir \
    jupyter \
    pandas \
    pyarrow \
    delta-spark==3.0.0 \
    matplotlib \
    seaborn

RUN jupyter notebook --generate-config && \
    echo "c.NotebookApp.ip = '0.0.0.0'" >> /root/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.port = 8888" >> /root/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.open_browser = False" >> /root/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.token = ''" >> /root/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.password = ''" >> /root/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.allow_root = True" >> /root/.jupyter/jupyter_notebook_config.py

WORKDIR /workspace

EXPOSE 8888 4040

CMD ["jupyter", "notebook", "--notebook-dir=/workspace", "--allow-root"]
