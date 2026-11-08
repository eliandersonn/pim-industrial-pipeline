FROM apache/airflow:3.1.6-python3.12
USER airflow
WORKDIR /opt/pipeline
COPY --chown=airflow:root constraints/ constraints/
COPY --chown=airflow:root requirements.txt pyproject.toml ./
COPY --chown=airflow:root src/ src/
COPY --chown=airflow:root sql/ sql/
# Constraints oficiais da mesma versão do Airflow; não instalar em containers ativos.
RUN pip install --no-cache-dir "apache-airflow==3.1.6" "psycopg2-binary==2.9.11" -r requirements.txt \
    --constraint constraints/airflow-3.1.6-python3.12.txt \
    && pip install --no-cache-dir --no-deps --no-build-isolation -e . \
    && pip check
WORKDIR /opt/airflow
