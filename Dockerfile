FROM apache/airflow:2.8.1-python3.10

# Copia e instala as dependências listadas no requirements.txt
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
