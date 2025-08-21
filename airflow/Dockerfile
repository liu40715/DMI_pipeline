FROM apache/airflow:2.9.2

COPY dags /opt/airflow/dags
COPY data /opt/airflow/data
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
