# Credit-Risk-Analysis

> Repository to reproduce a credit risk data pipeline including data ingestion to HDFS, preprocessing notebooks, feature extraction, modeling, and a streaming demo with Kafka + Spark Streaming.

---

## Table of contents

* [Project overview](#project-overview)
* [Prerequisites](#prerequisites)
* [Quick start](#quick-start)
* [Download data](#download-data)
* [Upload data to HDFS](#upload-data-to-hdfs)
* [Run notebooks (preprocessing, EDA, modeling)](#run-notebooks-preprocessing-eda-modeling)
* [Build preprocessor for new data](#build-preprocessor-for-new-data)
* [Streaming (Kafka + Spark Streaming)](#streaming-kafka--spark-streaming)
* [Testing the streaming pipeline](#testing-the-streaming-pipeline)
* [Stopping and cleanup](#stopping--cleanup)
* [Troubleshooting & tips](#troubleshooting--tips)
* [Contact & Credits](#contact--credits)

---

## Project overview

This repository contains code and notebooks for a credit risk analysis pipeline: data ingestion, preprocessing, exploratory data analysis (EDA), feature extraction, model training, and a streaming demo that simulates events and predictions via Kafka and Spark Streaming.

The instructions below describe how to get the project running locally using Docker and HDFS, run the notebooks, create the preprocessor artifact used by the model, and start the streaming demo.

---

## Prerequisites

* Git
* Docker & Docker Compose
* Enough disk space for the dataset and containers
* `bash` / a POSIX-compatible shell for running provided scripts

Ports used (default):

* Jupyter Notebook: `localhost:8888`
* Web dashboard (monitoring / demo): `localhost:5000`

---

## Quick start

1. Clone the repository and enter the project directory:

```bash
git clone https://github.com/nctrinh/Credit-Risk-Analysis.git
cd Credit-Risk-Analysis
```

2. Start required services with Docker Compose (both variants are accepted):

```bash
# either
docker compose up -d
# or
docker-compose up -d
```

---

## Download data

Download the data files referenced in `data/links_to_data.txt` into the `data/` folder. Ensure the downloaded file expected below is named `loan.csv` and placed in `./data/`.

> The repo contains `data/links_to_data.txt` — use it as the source of truth for data URLs.

---

## Upload data to HDFS

After starting containers, upload the dataset to HDFS so the notebooks and Spark jobs can access it.

Run the following commands (these are meant to be executed on your host machine):

```bash
# Leave safe mode on the namenode
docker exec -it namenode hdfs dfsadmin -safemode leave

# Create a temporary directory on the namenode container and copy the CSV there
docker exec -it namenode mkdir -p /opt/bigdata/data/
docker cp ./data/loan.csv namenode:/opt/bigdata/data/

# Create target directories in HDFS
docker exec -it namenode hdfs dfs -mkdir -p /bigdata/data
docker exec -it namenode hdfs dfs -mkdir -p /bigdata/notebooks
docker exec -it namenode hdfs dfs -mkdir -p /bigdata/data/splitted_data
docker exec -it namenode hdfs dfs -mkdir -p /bigdata/data/processed_data

# Put the CSV into HDFS
docker exec -it namenode hdfs dfs -put /opt/bigdata/data/loan.csv /bigdata/data/
```

---

## Run notebooks (preprocessing, EDA, modeling)

Open Jupyter Notebook at `http://localhost:8888`.

Navigate to `work/notebooks` and run in order (use **Run All** for each notebook):

1. `data_preprocessing.ipynb`
2. `data_splitting.ipynb`
3. `EDA.ipynb`
4. `feature_extraction.ipynb`
5. `modeling.ipynb`

After notebooks finish you can stop the Jupyter Notebook service if you like.

---

## Build preprocessor for new data

To generate the `preprocessor` artifact (used to preprocess new incoming data), run the following inside the `jupyter_notebook` container:

```bash
# open a shell inside the jupyter container
docker exec -it jupyter_notebook bash

# activate conda environment and run preprocessor builder
conda activate py37
python3 work/model/preprocessor.py
```

This will produce the preprocessor file used by streaming consumers / model inference.

---

## Streaming (Kafka + Spark Streaming)

To enable streaming for the demo, run the scripts in the `scripts/` folder. **Run each command in its own terminal** (they are long-running processes):

```bash
bash scripts/create_topics.sh
bash scripts/run_spark_streaming.sh
bash scripts/run_consumer.sh
```

When streaming is running you can observe the demo/dashboard at `http://localhost:5000`.

---

## Testing the streaming pipeline

To generate test events and see end-to-end behavior, open a new terminal and run the test client:

```bash
python3 test_client.py
```

Follow the on-screen menu and choose a number to select a test to run (the script will indicate available choices).

---

## Stopping & cleanup

To stop the Docker Compose stack:

```bash
# from repo root
docker compose down
# or
docker-compose down
```

(Optional) Remove volumes if you want a clean start (be careful — this deletes persisted container data):

```bash
docker compose down -v
# or
docker-compose down -v
```

---

## Troubleshooting & tips

* If Jupyter fails to start, check container logs: `docker logs jupyter_notebook`.
* If HDFS commands fail, ensure the `namenode` container is healthy and `hdfs dfsadmin -safemode leave` has been executed.
* For streaming issues, check Kafka topics exist: run `docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092`.
* If ports are already taken, either stop the conflicting service or change the port mapping in `docker-compose.yml`.

---

## Contact & Credits

This project is maintained by the development team: **Nguyễn Công Trình**, **Phạm Thế Trung**, and **Nguyễn Công Vinh**.

If you have any questions regarding the codebase, please open an issue in this repository.

---

If you want, I can also:

* Add badges (build / license)
* Generate a short usage GIF or a screenshot guide
* Create a `Makefile` or simplified scripts to automate the above steps
