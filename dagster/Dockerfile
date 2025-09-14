FROM python:3.11-slim
WORKDIR /opt/dagster
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY dagster.yaml workspace.yaml repo.py assets ./
ENV DAGSTER_HOME=/opt/dagster
EXPOSE 3000
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "workspace.yaml"]
