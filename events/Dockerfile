# syntax=docker/dockerfile:1

FROM python:3.8-slim-buster


WORKDIR .

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY . .

ENV HOME /home/app

CMD ["python", "main.py"]