FROM apache/airflow:2.10.4


# Copy the requirements.txt file into the airflow container to add some
#Operators
COPY requirements.txt /requirements.txt
# Install dependencies from requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt


#COPY /home/katerina/AIRFLOW/entrypoint.sh /entrypoint.sh
#RUN chmod +x entrypoint.sh
#RUN adduser --disabled-password --gecos '' airflow
#USER airflow
#CMD ["/bin/bash", "entrypoint.sh"]
