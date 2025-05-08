# Use official Python image as a base
FROM python:3.13-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . /app/



# Run the application
CMD ["python", "etl_extract.py","etl_load.py", "main.py"]
