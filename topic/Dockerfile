FROM python
ADD topic.py /
ADD twitter_credentials.json /
RUN pip install --upgrade pip
RUN pip install python-twitter
RUN pip install redis
ENTRYPOINT ["python", "/topic.py"]
