FROM jupyter/pyspark-notebook:spark-3.2.1

USER root

RUN apt-get update && apt-get remove -y openjdk-* && apt-get autoremove -y && \
    apt-get install -y openjdk-8-jdk && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN conda install -y mamba -n base -c conda-forge && \
    mamba create -y -n py37 python=3.7 ipykernel matplotlib pandas tqdm pyspark=3.2.1 seaborn kafka-python fastapi "uvicorn[standard]" pydantic scikit-learn joblib && \
    conda clean -afy

RUN su - jovyan -c "/opt/conda/envs/py37/bin/python -m ipykernel install --user \
    --name py37 --display-name 'Python 3.7 (py37)'"

USER jovyan

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=/opt/conda/envs/py37/bin:$JAVA_HOME/bin:$PATH
ENV CONDA_DEFAULT_ENV=py37
ENV PYSPARK_PYTHON=/opt/conda/envs/py37/bin/python
ENV PYSPARK_DRIVER_PYTHON=/opt/conda/envs/py37/bin/python
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
