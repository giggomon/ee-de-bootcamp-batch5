# Base image
FROM apache/airflow:2.11.0

## Install additional Python packages
#RUN pip install --no-cache-dir apache-airflow-providers-google==10.15.0
#RUN pip install --no-cache-dir snowflake-connector-python>=3.0.0
#RUN pip install --no-cache-dir apache-airflow-providers-snowflake


# Copy your requirements.txt file to the container
COPY requirements.txt ./

# Install the required Python packages
RUN pip install --no-cache-dir -r requirements.txt




