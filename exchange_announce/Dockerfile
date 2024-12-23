FROM python:3.12.5
WORKDIR /data
COPY . .
RUN pip install -r requirements.txt
ENTRYPOINT ["gunicorn", "-b", "0.0.0.0:7001", "app:app"]