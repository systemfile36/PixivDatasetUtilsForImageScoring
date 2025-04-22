#Environment for BatchDownloadPixiv.py
FROM python:latest

COPY ./gallery-dl.conf /etc/gallery-dl.conf

COPY ./requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

