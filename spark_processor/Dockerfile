    # Use a Bitnami Spark image as the base
    FROM bitnami/spark:3.3.0

    # Switch to the root user to install dependencies
    USER root

    # Copy requirements and our script
    WORKDIR /app
    COPY requirements.txt .
    COPY processor.py .

    # Install Python dependencies
    RUN pip install --no-cache-dir -r requirements.txt

    # Switch back to the non-root user
    USER 1001