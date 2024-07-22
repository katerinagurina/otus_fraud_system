FROM python:3.8.11

WORKDIR /workdir/

COPY . .
COPY requirements.txt ./

RUN apt-get install default-jdk -y
RUN pip install --upgrade pip
RUN pip install findspark pyspark mlflow boto3 fastapi pydantic pandas numpy uvicorn


# Run the application
CMD ["python", "-m", "uvicorn", "app.app:app", "--host", "0.0.0.0", "--port", "8000"]