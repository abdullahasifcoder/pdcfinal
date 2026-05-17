# Code Walkthrough: How the Stock Anomaly Project Works

This guide explains the code for a beginner who isn't very familiar with Python or the tools used. It breaks down the flow of data, how the files talk to each other, and what each function actually does.

---

## The Big Picture: How the Files Connect

The project works like a relay race. The baton (data) is passed from one file to the next in this exact order:

1. **`config.py`**: The rulebook. It tells everyone which 12 stocks to look at and where Kafka is.
2. **`producer/replay_producer.py`**: The sender. It reads the raw data file, cleans it up, and sends it to Kafka.
3. **`flink_job/flink_job.py`**: The brain. It reads the data from Kafka, does the math to find anomalies, and saves the results to Cassandra.
4. **`cassandra/schema.cql`**: The blueprint. It just tells the Cassandra database how to set up its tables to receive the data.
5. **`setup_grafana.py`**: The painter. It tells the Grafana dashboard how to draw the charts using the data from Cassandra.

Here is the exact flow of data through the functions:

```text
[replay_producer.py] reads a line from the file
      ↓
[replay_producer.py] sends data to Kafka
      ↓
[flink_job.py] receives data from Kafka
      ↓
[flink_job.py] waits 10 seconds (windowing)
      ↓
[flink_job.py] runs detect_anomalies() function
      ↓
[flink_job.py] runs insert_trade() and insert_anomaly() to save to Cassandra
```

---

## File 1: `config.py`
**Language:** Python
**Purpose:** Stores settings that multiple other files need to use so we don't have to write them twice.

**What's inside:**
*   `SYMBOLS = ["AAPL", "MSFT", ...]` → A simple list of the 12 stock tickers we care about.
*   `KAFKA_BROKER = "localhost:9092"` → The "address" of our Kafka post office.
*   `KAFKA_TOPIC = "stock-trades"` → The specific mailbox inside Kafka where we will drop our data.

---

## File 2: `producer/replay_producer.py`
**Language:** Python
**Purpose:** Reads the massive 3.4 GB `.gz` file of stock trades line-by-line and sends them to Kafka.

### Functions in this file:

1. **`create_kafka_producer()`**
   *   **What it does:** Tries to connect to Kafka. If Kafka isn't ready yet, it waits 5 seconds and tries again.
   *   **Where it's called:** At the very beginning of the `main()` function.

2. **`taq_time_to_epoch_ms(time_str, date_epoch_s)`**
   *   **What it does:** The raw dataset gives time in a weird format like `"093015123456"` (meaning 09:30:15 and 123456 microseconds). This function converts it into "Epoch time" (the number of milliseconds since Jan 1, 1970). Computers prefer Epoch time because it's just a number, making it easy to sort and compare.
   *   **Where it's called:** Inside the `main()` loop for every single trade record.

3. **`filename_to_date_epoch(filename)`**
   *   **What it does:** Looks at the filename (e.g., `EQY_US_ALL_TRADE_20260102.gz`), extracts the date "2026-01-02", and figures out what time that day started. 
   *   **Where it's called:** Inside `main()` right before reading a file.

4. **`main()`**
   *   **What it does:** The main engine. 
       *   Opens the `.gz` file (without extracting it, to save space).
       *   Reads it line by line (`for raw_line in f:`).
       *   Splits the line by the pipe character `|`.
       *   Checks if the stock symbol is in our `config.py` list. If it's not (e.g., a random small company), it skips it (`continue`).
       *   Extracts the price and volume.
       *   Packages it all into a neat JSON dictionary: `{"symbol": "AAPL", "price": 150.0, "volume": 100}`
       *   Sends it to Kafka using `producer.send()`.

---

## File 3: `flink_job/flink_job.py`
**Language:** Python
**Purpose:** The core logic of the project. It consumes data from Kafka, groups it into 10-second time windows, checks for anomalies, and saves the data to the Cassandra database.

### Functions in this file:

1. **`connect_cassandra()`**
   *   **What it does:** Logs into the Cassandra database. Retries if the database isn't ready.
   *   **Where it's called:** At the start of `main()`.

2. **`insert_trade(session, symbol, price, volume, trade_time_ms)`**
   *   **What it does:** Takes a single stock trade and writes a SQL-like `INSERT` command to save it into Cassandra's `trades` table.
   *   **Where it's called:** Inside `main()`, every time a single trade arrives from Kafka.

3. **`insert_anomaly(session, symbol, anomaly_type, price, volume)`**
   *   **What it does:** Takes the details of a detected anomaly and saves it into Cassandra's `anomalies` table.
   *   **Where it's called:** Inside `detect_anomalies()`, but only if an anomaly is actually found.

4. **`detect_anomalies(symbol, ticks, session)`**
   *   **What it does:** The most important math function in the project. It looks at all the trades (`ticks`) that happened for one stock in the last 10 seconds.
       *   It calculates the highest price, lowest price, starting price, ending price, and total volume.
       *   **Check 1 (Volume Spike):** Is the total volume 3x higher than the average of the last 5 windows? If yes, call `insert_anomaly()`.
       *   **Check 2 (Flash Crash):** Did the price drop by more than 2% between the start and end of the 10 seconds? If yes, call `insert_anomaly()`.
       *   **Check 3 (Price Spike):** Did the price rise by more than 2%? If yes, call `insert_anomaly()`.
   *   **Where it's called:** Inside `main()`, exactly once every 10 seconds per stock.

5. **`create_consumer()`**
   *   **What it does:** Connects to Kafka as a "Consumer" (a reader) to listen to the `stock-trades` topic.
   *   **Where it's called:** At the start of `main()`.

6. **`main()`**
   *   **What it does:** The infinite loop that drives the processing.
       *   It listens to Kafka (`for message in consumer:`).
       *   For every message, it checks the clock (`now = time.time()`).
       *   It stores the trade in a temporary list (RAM).
       *   If 10 seconds have passed since the list was created, it "closes the window": it sends the list to `detect_anomalies()`, clears the list, and restarts the 10-second timer.

---

## File 4: `cassandra/schema.cql`
**Language:** CQL (Cassandra Query Language, looks exactly like SQL)
**Purpose:** Sets up the empty database tables before any Python code runs.

**What it does:**
*   `CREATE KEYSPACE stock_market` → Creates the database.
*   `CREATE TABLE trades` → Creates a table with columns for symbol, time, price, and volume. It sorts the data by time (`CLUSTERING ORDER BY (trade_time DESC)`).
*   `CREATE TABLE anomalies` → Creates a table with columns for symbol, time, anomaly type, price, and volume.

**Note:** There are no functions here. It is just a script of database commands executed once during setup.

---

## File 5: `setup_grafana.py`
**Language:** Python
**Purpose:** Automates the clicking around you would normally do in the Grafana website to build a dashboard.

**What it does:**
*   It doesn't process stock data at all.
*   Instead, it talks to Grafana's "API" (a way for code to talk to websites).
*   It sends a massive JSON packet to Grafana that says: *"Create a dashboard called 'Stock Anomaly Monitor', make 3 panels, and here is the exact SQL code you should use to get data from Cassandra for those panels."*
*   Once run, it exits. Its job is done.

---

## Summary of the "Windowing" Flow (The Hardest Concept)

Since you don't know the language well, here is a plain-English explanation of how the 10-second "Windowing" works inside `flink_job.py`:

1. Trade #1 arrives for Apple at 10:00:00. The script says: *"Okay, the Apple window has started. I will hold this trade in my memory."*
2. Trade #2 arrives for Apple at 10:00:02. Script: *"Put it in the memory list."*
3. Trade #3 arrives for Apple at 10:00:09. Script: *"Put it in the memory list."*
4. Trade #4 arrives for Apple at 10:00:11. Script: *"Wait! It's been more than 10 seconds since 10:00:00. Pause!"*
5. The script takes the list of Trades 1, 2, and 3, hands them to `detect_anomalies()`, and says: *"Look at these and tell me if anything weird happened."*
6. Once `detect_anomalies()` is done, the script deletes Trades 1, 2, and 3 from memory.
7. It starts a brand new 10-second window for Trade #4.
