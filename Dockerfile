# Usamos uma imagem oficial do Python, versão slim (mais leve)
FROM python:3.11-slim

# Define o diretório de trabalho dentro do container
WORKDIR /app

# Instala dependências de sistema necessárias para o psycopg2 e limpeza de cache
RUN apt-get update && \
    apt-get install -y --no-install-recommends libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Copia apenas o requirements.txt primeiro para aproveitar o cache do Docker
COPY requirements.txt .

# Instala as dependências do Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copia o código fonte para o container
COPY src/ ./src/

# Cria as pastas de dados (elas serão sobrescritas pelo volume do docker-compose)
RUN mkdir -p data/external data/raw data/trusted data/quality_checks

# Define o python como entrypoint.
# Assim, podemos passar o script como argumento: docker run ... src/run_pipeline.py
ENTRYPOINT ["python"]