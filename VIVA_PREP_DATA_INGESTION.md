# Viva Preparation Guide: Data Ingestion & Messaging (The Producer)

**Role Owner:** Abdullah (or whoever claimed the Data Ingestion part)

## Your Mission in the Project
Your job was to build the front-door of the system. You took a massive, raw dataset (the NYSE TAQ file), cleaned it up on the fly, and securely transmitted it into the distributed pipeline using Apache Kafka. 

Without your part, the rest of the system has no data to process. Your biggest challenge was handling a "Big Data" file without crashing the computer's RAM.

---

## The Files You Own

### 1. `producer/replay_producer.py`
This is your main script. It does the heavy lifting of reading data and pushing it to Kafka.

**How the code works (Your Script's Flow):**
1.  **Connect to Kafka:** The `create_kafka_producer()` function tries to connect to Kafka at `localhost:9092`. If Kafka isn't awake yet, it retries every 5 seconds.
2.  **Streaming the File:** In the `main()` function, you use `gzip.open()`. This is crucial. Instead of unzipping the 3.4 GB file into a 14 GB text file, `gzip.open()` unzips and reads the file *one single line at a time*. This is called "lazy reading" or "streaming I/O", and it means your script only uses a few Megabytes of RAM, never Gigabytes.
3.  **Parsing & Filtering:** You take a raw string like `093015123456|N|AAPL|@   |100|142.5500|...` and split it using `parts = raw_line.split("|")`. 
4.  **Symbol Check:** You check if the symbol (column 2) is in your list of 12 allowed stocks. If it's a random stock we don't care about, you skip it using `continue` to save processing power.
5.  **Data Packaging:** You take the time, price, and volume, and pack them into a nice JSON dictionary: `{"symbol": "AAPL", "price": 142.55, ...}`.
6.  **Sending to Kafka:** You use `producer.send(KAFKA_TOPIC, value=record)` to hand the data to Kafka.

### 2. `config.py` (Shared)
You share this with the team. It just stores the `KAFKA_BROKER` address and the `SYMBOLS` list so you don't have to hardcode them.

---

## Likely Viva Questions for Your Part

**Q: How did you handle a file that is 14GB uncompressed without running out of RAM?**
**A:** "I used Python's `gzip` module to read the compressed `.gz` file as a stream. Instead of loading the whole file into an array, my code reads exactly one line, processes it, sends it to Kafka, and then discards it. The memory footprint stays tiny no matter how big the file is."

**Q: Why send data to Kafka? Why not send it directly to Flink or save it to a database yourself?**
**A:** "Kafka acts as a shock-absorber. My script can read and parse the file extremely fast (thousands of rows per second). If I sent that directly to Flink, Flink might get overwhelmed. By sending it to Kafka, Kafka buffers the data safely. Flink can then consume the data from Kafka at its own comfortable pace."

**Q: What is a Kafka Topic?**
**A:** "A topic is like a specific mail folder or category in Kafka. I send all my trades to the `stock-trades` topic. Any consumer that wants trade data just subscribes to that specific topic."

**Q: How do you control the speed of the replay?**
**A:** "I added a `MESSAGES_PER_SECOND` setting. If it's set to 1000, my script uses `time.sleep()` to throttle the loop so it only sends 1000 messages per second. If I set it to 0, it sends data as fast as the CPU allows, which is a great stress-test for Kafka."

**Q: The TAQ time format is weird (`093015123456`). How did you handle it?**
**A:** "I wrote a helper function `taq_time_to_epoch_ms`. It breaks down the string into hours, minutes, seconds, and microseconds. It then calculates how many seconds into the day that trade happened, adds it to the midnight timestamp of that specific date, and converts it into standard Epoch Milliseconds so the rest of the team has a standard timestamp to work with."
