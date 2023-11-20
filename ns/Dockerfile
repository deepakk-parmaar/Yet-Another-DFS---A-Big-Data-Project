FROM python:3.11

WORKDIR /app
COPY ./ns /app

RUN pip install PyYAML


EXPOSE 8888
CMD ["python3", "./name_server.py"]

