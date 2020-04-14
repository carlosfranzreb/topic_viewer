FROM python
ADD aggregator.py /
RUN pip install --upgrade pip
RUN pip install redis
ENTRYPOINT ["python", "/aggregator.py"]
