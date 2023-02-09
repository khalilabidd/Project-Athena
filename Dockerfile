FROM python:3.9-slim

WORKDIR /project

COPY ./project/requirements.txt .

RUN pip install -r requirements.txt

COPY ./project ./project