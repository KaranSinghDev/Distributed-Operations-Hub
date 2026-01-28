# Use a slim, modern Python base image.
FROM python:3.11-slim

# Set the working directory inside the container.
WORKDIR /app

# Set environment variables for best practices.
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

#  install system dependencies like the protobuf compiler.
RUN apt-get update && apt-get install -y protobuf-compiler

# Copy the requirements file and install Python dependencies.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code.
COPY . .

# Generate the gRPC/protobuf code inside the container.
# This ensures the build is self-contained.
RUN python -m grpc_tools.protoc -I./proto --python_out=. --grpc_python_out=. ./proto/cache.proto

# Expose the gRPC port.
EXPOSE 50051

# The command to run when the container starts.
# It will be provided by Docker Compose.
CMD ["python", "server.py"]