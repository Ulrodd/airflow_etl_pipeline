[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.5.1-blue)](https://airflow.apache.org/)  
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-blue)](https://www.postgresql.org/)  
[![Docker](https://img.shields.io/badge/Docker-latest-blue)](https://www.docker.com/)

# ETL Online Retail Pipeline

## Description
Ce projet met en place une **pipeline ETL** avec Apache Airflow qui lit, nettoie, transforme puis charge les données du jeu de données **Online Retail** dans une base PostgreSQL, en organisant les tables en staging, dimensions et faits, et en fournissant un contrôle qualité et une notification Slack.

## Stack
- **Apache Airflow** 2.5.1  
- **PostgreSQL** 13  
- **Docker & Docker Compose**  
- **Pandas** pour le traitement CSV  
- **Slack Webhook** pour alertes de succès

## Prérequis
- Docker & Docker Compose (v1 ou v2) installés sur votre machine  
- (Sous Windows) Docker Desktop en mode WSL 2 ou PowerShell  
- Fichier `data/online_retail.csv` : à placer manuellement dans `./data`

## Installation & démarrage
```bash
git clone https://github.com/votre-utilisateur/airflow_etl_pipeline.git
cd airflow_etl_pipeline

# 1) Construire l’image custom Airflow (avec vos dépendances)
docker-compose down
docker-compose build

# 2) Lancer les services en arrière-plan
docker-compose up -d

# 3) Initialiser la base Airflow et créer l’utilisateur Admin
docker-compose run --rm webserver airflow db init
docker-compose run --rm webserver airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin
