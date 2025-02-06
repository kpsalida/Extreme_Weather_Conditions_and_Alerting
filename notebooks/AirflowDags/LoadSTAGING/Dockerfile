FROM python:3.12-slim

RUN apt-get update -y \
    && apt-get install -y python3-dev python3-pip build-essential \
    && apt-get install gcc -y \
    && apt-get install sudo -y \ 
    && apt-get clean
# set up working directory inside the container
WORKDIR /app

# copy and run the requirements.txt file to install the required packages.
COPY requirements.txt .
#COPY Weather_Codes_severity.json .
COPY City_coordinates.json .
COPY FinalProject_FullUpload_StagingHistory.py .
RUN pip install -r requirements.txt
