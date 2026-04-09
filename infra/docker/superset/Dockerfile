FROM apache/superset:3.1.3

# Ensure we are in the correct environment
USER root

# Install Trino
RUN pip3 install --no-cache-dir trino>=0.320.0

# Verify
RUN python3 -c "import trino; print('Trino installed:', trino.__version__)"

USER superset
WORKDIR /app