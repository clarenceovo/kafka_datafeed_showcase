FROM python:3.8

EXPOSE 8443
WORKDIR /code


COPY requirements.txt .

RUN pip install -r requirements.txt

COPY /central_market_data_feed . 

CMD [ "python", "main.py" ]
