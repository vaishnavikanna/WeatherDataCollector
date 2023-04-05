FROM python:3.10
WORKDIR /WeatherDataCollectorScheduler
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD [ "python", "./scheduler.py" ]
