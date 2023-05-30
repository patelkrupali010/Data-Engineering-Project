# This is a Dockerfile that builds a container for a Python application.

# Use an official latest Python base image
FROM python:latest

# Set the working directory in the container
WORKDIR /app

# Copy requirements.txt file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . . 

# Set the default command to run when a container is started
CMD ["python","./roofs_processing.py"]