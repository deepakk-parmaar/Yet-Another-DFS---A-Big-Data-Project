FROM python:3.11

WORKDIR /app
COPY ./ns /app

RUN pip install PyYAML

RUN pip install --no-cache-dir \
    os \
    random \
    sys \
    PyYAML \
    _thread \
    socket \
    xmlrpc.server \
    xmlrpc.client \
    time

EXPOSE 8888
CMD ["python3", "./name_server.py"]

