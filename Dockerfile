FROM python:3
ADD . /code/
WORKDIR /code/
RUN pip install -r requirements.txt
CMD pytest --looponfail tests.py
