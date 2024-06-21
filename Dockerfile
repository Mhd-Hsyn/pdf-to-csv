FROM python:3.8.10

# Install necessary system dependencies
RUN apt-get update && \
    apt-get install -y libgl1-mesa-glx ghostscript

# Set up the working directory
RUN mkdir /pdf_to_csv
WORKDIR /pdf_to_csv

# Copy the requirements file and install dependencies
COPY requirements.txt /pdf_to_csv/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /pdf_to_csv/requirements.txt

# Copy the rest of the application code
COPY . /pdf_to_csv/

# Specify the command to run the application
CMD ["python", "main.py"]
