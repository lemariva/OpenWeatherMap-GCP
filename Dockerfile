FROM python:3.7-buster

WORKDIR /home
COPY resources/requirements/requirements.txt /tmp/requirements.txt
COPY resources/credentials/service_account.json /home/service_account.json

RUN apt-get update \
    && apt-get install -y python-pip \
    && pip install -r /tmp/requirements.txt \
    && rm -rf /var/lib/apt/lists/*

COPY resources/main.py /home/main.py
COPY resources/run-collect.sh /home/run-collect.sh

ENTRYPOINT [ "/home/run-collect.sh" ]

