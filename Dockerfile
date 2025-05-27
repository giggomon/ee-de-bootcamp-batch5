# Start from the official Airflow image
FROM apache/airflow:2.11.0

# Install additional Python packages
RUN pip install --no-cache-dir apache-airflow-providers-google==10.15.0

# Copy your requirements.txt file to the container
# COPY requirements.txt ./

# Install the required Python packages
# RUN pip install -r requirements.txt




