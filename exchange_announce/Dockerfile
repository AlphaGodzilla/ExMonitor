FROM python:3.12.5
WORKDIR /data/exchange_announce
COPY . .
RUN apt update && apt install sqlite3 -y && pip install -r requirements.txt
ENTRYPOINT ["gunicorn", "-b", "0.0.0.0:7001", "app:app"]