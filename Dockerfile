FROM ubuntu:22.04


# install packages
RUN apt-get update && \
    apt-get install -y curl \
    wget \
    openjdk-8-jdk

RUN apt-get install -y build-essential python3-pip
RUN pip3 -q install pip --upgrade 

WORKDIR /workdir/

COPY . .

USER root

RUN pip install findspark pyspark mlflow boto3 fastapi pydantic pandas numpy uvicorn
RUN pip install -U -e .

# Run the application
CMD ["python3", "-m", "uvicorn", "app.app:app","--host", "0.0.0.0", "--port", "80"]

