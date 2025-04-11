#Environment for BatchDownloadPixiv.py
FROM python:latest

COPY ./gallery-dl.conf /etc/gallery-dl.conf

RUN pip install --no-cache-dir gallery-dl

