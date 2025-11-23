
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
| `docker-compose restart app` | Restarts only the container named `app`. Use this after code changes without rebuilding all images. |

---