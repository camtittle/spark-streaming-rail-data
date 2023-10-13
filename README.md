# Spark Streaming with Rail Data

This project uses Spark Streaming to process realtime railway data and generate performance metrics.

Currently, it generates the average delay (in minutes) of trains departing from each station. It aggregates this data per station for hourly windows and outputs to the console in this format:

```
+--------------------+-------+-------------+------------------+----------+------------+--------------------+
|              window|    tpl|planned_count|avg_dep_delay_mins|late_count|actual_count|total_avg_delay_mins|
+--------------------+-------+-------------+------------------+----------+------------+--------------------+
|{2023-10-13 15:45...|MNCRPIC|           38|               1.0|         0|           1|                 0.0|
```

In the future, I'd like to sink the output into a database or other datastore, to allow it to be queried or visualised.

## Setup

There are 2 application to run simultaneously

1. The PySpark application which receives streaming rail data and processes it to produce aggregated performance statistics
2. A simple Python application `darwin-listener.py` which connects to the National Rail Darwin data feed via STOMP protocol and pipes the events into PySpark via a local socket

### Darwin Event Listener Setup

You will need access keys for the National Rail Darwin data feed. [Register here.](https://opendata.nationalrail.co.uk/)

Once you have access to Darwin, set up the credentials as described below. Once you have set up the credentials you can run

#### STOMP Credentials

Realtime train events are received via the STOMP protocol. 

1. Gather your Pushport username and password from the National Rail data portal
2. Create a copy of the `.env-template` file inside the `darwin-listener` directory, call it `.env`
3. Populate the file with your username and password

### PySpark Setup

You must have Python 3 and PySpark installed on your machine. [Instructions here](https://spark.apache.org/downloads.html).

Ensure you have Python 3 installed. To set up the dependencies for the project using virtualenv:

1. Create a venv: `python3 -m venv .venv`
2. Activate the venv: `source .venv/bin/activate`
3. Install dependencies: `pip3 install -r requirements.txt`
4. Package dependencies: `venv-pack -o pyspark_venv.tar.gz -o pyspark_venv.tar.gz`

#### AWS Credentials

Daily rail schedules are made available via an AWS S3 bucket. The application fetches the latest schedule on startup. To setup AWS access:

1. [Install the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) if you don't have it already
2. Run `aws configure` and create a profile called `darwin`
   1. Provide the Access key and Secret key which can be found in the National Rail data portal
3. Alternatively, manually create the `darwin` profile in your `~/.aws/credentials` file

### Run the app

Once the credentials are setup, run both apps. Run the darwin listener first:

```shell
python3 ./darwin-listener/darwin-listener.py
```

Then in a separate terminal, run:

```shell
python3 ./streaming.py
```