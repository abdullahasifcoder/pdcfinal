# Viva Preparation Guide: Distributed Storage & UI (The Database & Dashboard)

**Role Owner:** Ramzan (or whoever claimed the Database, Docker, and Grafana part)

## Your Mission in the Project
Your job was to build the infrastructure that houses the entire project, stores the results securely, and makes the data visible to human users. 

You were responsible for the NoSQL database design (Cassandra), orchestrating all the servers to talk to each other (Docker Compose), and building a real-time dashboard (Grafana). Your biggest challenge was ensuring the database could handle high-speed continuous writes without slowing down.

---

## The Files You Own

### 1. `docker-compose.yml`
This is your master infrastructure blueprint.
*   **What it does:** It tells Docker to download and run 6 different servers (Zookeeper, Kafka, Flink-Jobmanager, Flink-Taskmanager, Cassandra, and Grafana).
*   **The Network:** You placed them all on a virtual network called `stock-net`. This allows the containers to talk to each other using their names (e.g., Flink can connect to `cassandra:9042` without knowing its IP address).

### 2. `cassandra/schema.cql`
This is your database design file.
*   **The Keyspace:** You create `stock_market` with a `SimpleStrategy` replication factor of 1 (because we are running a single Cassandra node, not a cluster).
*   **The Tables:** You designed two tables: `trades` and `anomalies`. 
*   **The Primary Keys (Crucial!):** You designed the primary key as `PRIMARY KEY (symbol, trade_time)`. 
    *   `symbol` is the **Partition Key**. Cassandra uses this to group all trades for Apple on the same hard drive sector.
    *   `trade_time` is the **Clustering Key**. It tells Cassandra to sort the trades by time.
*   **TTL (Time to Live):** You added `default_time_to_live`. Cassandra will automatically delete raw trades after 24 hours so the hard drive doesn't fill up.

### 3. `setup_grafana.py`
This is your automation script for the UI.
*   **What it does:** Instead of manually clicking through Grafana to build charts, this script uses Python's `requests` library to talk to Grafana's HTTP API. It automatically creates the Cassandra data connection and builds the "Stock Anomaly Monitor" dashboard with 3 pre-configured panels.

---

## Likely Viva Questions for Your Part

**Q: Why did you choose Cassandra instead of a normal SQL database like MySQL?**
**A:** "Cassandra is a distributed NoSQL database designed specifically for extremely fast, high-volume write operations. Since our pipeline is streaming thousands of stock trades per second, a traditional SQL database would bottleneck and crash. Cassandra handles continuous time-series data streams perfectly."

**Q: Explain your Primary Key design in Cassandra.**
**A:** "I used a composite primary key: `(symbol, trade_time)`. The first part, `symbol`, is the Partition Key. It ensures that all data for a specific stock (like AAPL) is stored together on the same node for fast querying. The second part, `trade_time`, is the Clustering Key. It sorts the data on the disk chronologically, which makes Grafana's time-series charts load instantly."

**Q: What is Docker Compose and why did you use it?**
**A:** "Docker Compose is a tool for defining and running multi-container applications. Our project requires 6 different software servers to run simultaneously. Instead of making the user manually install Kafka, Flink, Cassandra, and Grafana on their Windows machine, I wrote a `docker-compose.yml` file. With one command (`docker-compose up`), it boots up all 6 servers in isolated, pre-configured environments."

**Q: How does Cassandra clean up old data? Do you have a script running to delete old trades?**
**A:** "No manual scripts are needed! I used a native Cassandra feature called TTL (Time-To-Live). In the schema, I set `default_time_to_live = 86400` for the trades table. This tells Cassandra to automatically delete any row exactly 24 hours after it was inserted. This prevents our hard drive from filling up with old stock ticks."

**Q: How does the Grafana dashboard get its data?**
**A:** "Grafana connects directly to our Cassandra database using a Cassandra Data Source plugin. I wrote CQL (Cassandra Query Language) queries for each panel. Every 5 seconds, Grafana asks Cassandra for the latest trades and anomalies that fall within the current time window, and redraws the graphs."

**Q: Why did you write `setup_grafana.py` instead of just building the dashboard manually?**
**A:** "For reproducibility. By using Grafana's REST API, I codified the dashboard as JSON. Now, anyone can run the project on a fresh computer, run my script, and the dashboard is instantly configured without any manual setup."
