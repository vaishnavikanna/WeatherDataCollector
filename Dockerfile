FROM python:3.10
WORKDIR /WeatherDataCollectorScheduler
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
EXPOSE 9090
CMD [ "python", "./scheduler.py" ]
