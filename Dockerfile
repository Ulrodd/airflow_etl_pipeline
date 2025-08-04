# Dockerfile
FROM apache/airflow:2.5.1

# Copier le fichier de dépendances
COPY requirements.txt /tmp/requirements.txt

# Passer à l'utilisateur 'airflow' avant d'installer les paquets Python
USER airflow

# Installer les dépendances Python
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Revenir à l'utilisateur airflow (déjà en cours)
USER airflow
