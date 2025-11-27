
#  Project #2 - Databases II

---

###  Description:

This project consists of a Music Analyzer.... The goal is to ...

---  

###  Project Initialization

To manage the project containers, use the following Docker Compose commands inside the main folder (`Analisis_de_Musica`):

| Command | Description |
|----------|--------------|
| `docker-compose build --no-cache` | Rebuilds all Docker images **from scratch**, ignoring any cached layers. Use this when dependencies or configurations have changed. |
| `docker-compose up -d` | Starts all containers **in detached mode** (runs in the background). This is the main command to launch the project. |
| `docker-compose down -v` | Stops and removes all containers **and their volumes**.  This will **delete all stored data**, so use it only if you need a clean environment. |
| `docker compose down --volumes --remove-orphans` | Stops and removes all containers, associated **volumes**, and **orphaned containers** (those not defined in the current `docker-compose.yml`). Useful for full cleanup. |

---

### Flask application (`flask-app`)

The project runs in a small web application built with **Flask**, which runs inside the `flask-app` container.


---

### Accessing the application

Once all the services are up, you can access the different parts of the system through these URLs:

- **Spark Master Web UI**:
  Used to monitor the Spark cluster, jobs, stages and executors:
  - `http://localhost:8080/`

- **Web application (Flask front-end)**:
  - `http://localhost:5000/`

- **Loader status endpoint**  
  The result of the data loading process (success, errors, files processed, etc.) can be checked at:
  - `http://localhost:5001/status`

These endpoints make it easy to verify that the stack is running, inspect the web UI, and confirm that the CSV files were successfully loaded into HDFS in Parquet format.

---


### Data loader (`data_loader.py`)

The `data_loader.py` script is responsible **only for loading the CSV datasets into HDFS in Parquet format**, leaving them ready for later analysis.

The flow is as follows:

1. **Input files (local CSVs)**  
   - The source files are stored in the local project folder:
     ```text
     data/raw/
       ├─ user_top_albums.csv
       ├─ user_top_artists.csv
       ├─ user_top_tracks.csv
       └─ users.csv
     ```
   - This folder is mounted inside the containers (loader, master and workers) at:
     ```text
     /app/data/raw
     ```

2. **Running the loader with Spark**  
   - The `loader` container creates a Spark session pointing to the cluster.
   - For each CSV in `/app/data/raw`, Spark:
     - Reads the CSV file.
     - Applies basic transformations/cleaning (depending on the dataset).
     - Writes the result in **Parquet format** into HDFS.
     - It takes approximately 7 minutes to process the data.

3. **Output in HDFS (Parquet format)**  
   - The files are stored in HDFS under the prefix:
     ```text
     /processed_data/parquet/
     ```
   - The typical structure looks like:
     ```text
     /processed_data/parquet/user_top_albums/
     /processed_data/parquet/user_top_artists/
     /processed_data/parquet/user_top_tracks/
     /processed_data/parquet/users/
     ```
   - **Parquet** is used because it is a columnar, compressed format that is much more efficient for reading and analytics in Spark than the original CSV files.

4. **Validating the load in HDFS**  
   - After running the loader, you can verify that the Parquet files were created from the NameNode container:
     ```bash
     docker compose exec namenode bash
     hdfs dfs -ls /processed_data/parquet
     hdfs dfs -ls /processed_data/parquet/user_top_albums
     hdfs dfs -ls /processed_data/parquet/user_top_artists
     hdfs dfs -ls /processed_data/parquet/user_top_tracks
     hdfs dfs -ls /processed_data/parquet/users
     ```
   - If you see several `part-0000x-...snappy.parquet` files with a size greater than 0, it means the load from the loader to HDFS completed successfully.
---

### Useful commands and quality of life

| Objective | Command |
|----------|---------|
| **View service logs** | `docker compose logs -f analytics` |
| **Lift the containers for analysis and consultation** | `docker compose up -d namenode datanode spark-master spark-worker-1 mariadb` |
| **Access the Spark container** | `docker compose exec spark-master bash` |
| **View files in HDFS** | `docker compose exec namenode hdfs dfs -ls /processed_data/parquet` |
| **Connect to MariaDB** | `docker compose exec -it mariadb mariadb -u sparkuser -psparkpass music_analysis` |
| **Rebuild analytics only** | `docker compose build analytics` |
| **Run analytics only** | `docker compose run analytics` |
| **Shut down containers without deleting data** | `docker compose down` |
| **View top artist chart** | `SELECT * FROM top_20_artists;` |
| **View top tracks chart** | `SELECT * FROM top_20_tracks;` |
| **View top albums chart** | `SELECT * FROM top_20_albums;` |
| **Shut down containers and delete all data** | `docker compose down -v` ⚠️ |

--- 
