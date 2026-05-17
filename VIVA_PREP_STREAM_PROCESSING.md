# Viva Preparation Guide: Stream Processing & Logic (The Brain)

**Role Owner:** Waqas (or whoever claimed the Flink / Stream Processing part)

## Your Mission in the Project
Your job was to build the "brain" of the operation. You built an application that constantly listens to the data coming from Kafka, holds it in temporary memory, calculates statistics, and flags dangerous market behavior in real-time. 

Your biggest challenge was dealing with continuous, never-ending data (streaming) and figuring out how to group it into time-based chunks to perform math on it.

---

## The Files You Own

### 1. `flink_job/flink_job.py`
This is the core logic engine of the project. It runs *inside* the Flink Docker container.

**How the code works (Your Script's Flow):**
1.  **Connecting the Pipes:** In `main()`, you first connect to Cassandra (`connect_cassandra()`) so you have a place to save your results. Then you connect to Kafka (`create_consumer()`) to start receiving the live stream of trades.
2.  **The Infinite Loop:** You use `for message in consumer:`. This loop runs forever. As soon as a trade arrives in Kafka, your code grabs it.
3.  **The Windowing Logic (The hardest part):** You can't calculate a "price drop" using just one trade; you need a group of trades over time. 
    *   You use a dictionary called `current_window`. 
    *   When a trade arrives, you look at the clock. Has it been 10 seconds since the window for this stock started?
    *   **If NO:** You just append the trade to the `current_window` list and wait for more.
    *   **If YES:** The window is "closed". You send the list of trades to `detect_anomalies()`, clear the list, and start a new 10-second timer.
4.  **Anomaly Math:** Inside `detect_anomalies()`, you analyze the 10 seconds of trades:
    *   *Volume Spike:* You compare the total volume of this 10-second window to the average of the last 5 windows (stored in `volume_history`). If it's > 3x, it's a spike!
    *   *Flash Crash / Price Spike:* You subtract the `first_price` from the `last_price`. If the percentage change is less than -2%, it's a crash. If it's greater than +2%, it's a spike.
5.  **Saving Data:** Whether an anomaly is found or not, you always use `insert_trade()` to save the raw data to Cassandra. If an anomaly is found, you *also* call `insert_anomaly()`.

---

## Likely Viva Questions for Your Part

**Q: What is a Tumbling Window, and why did you use it?**
**A:** "A tumbling window is a fixed, non-overlapping chunk of time. I used 10-second tumbling windows. From 10:00:00 to 10:00:10, all trades are grouped together. At 10:00:10, the window closes, I do my calculations, and a brand new, empty window starts. I used this because you can't detect a 'trend' from a single message; you have to aggregate data over a time period."

**Q: How did your Flink job consume data from Kafka?**
**A:** "I used the `kafka-python` library to create a Consumer. I subscribed to the `stock-trades` topic. The consumer runs in an infinite loop, constantly polling Kafka. As soon as Abdullah's producer pushes a message to Kafka, my consumer instantly receives it."

**Q: What is the `KAFKA_GROUP` / Consumer Group?**
**A:** "A consumer group is an ID I give my script (I named it `flink-anomaly-group`). Kafka tracks what messages this specific group has read. If my Flink script crashes and I restart it, Kafka knows exactly where I left off based on my group ID, so I don't process the same trades twice."

**Q: How does your anomaly detection actually work?**
**A:** "It's a rule-based algorithm. For volume spikes, my code remembers the total volume of the last 5 windows in a rolling list. If the current window's volume is more than 3 times the average of that list, I flag it. For price crashes, I just take the very first trade in the 10-second window, and the very last trade. If the price dropped by more than 2% in that 10-second span, it's flagged as a flash crash."

**Q: Why did you write a standard Python script instead of using the PyFlink API?**
**A:** "To keep the project lightweight and reliable. I used the Flink Docker container to provide the isolated, scalable execution environment, but I wrote the logic using standard Python dictionaries for windowing. This made the code much easier to debug and understand for our academic project, while still simulating a distributed stream-processing environment."
