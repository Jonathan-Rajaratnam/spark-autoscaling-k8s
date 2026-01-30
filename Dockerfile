FROM apache/spark:3.5.7

USER root

# Copy application
COPY app.py /opt/spark/app.py
#COPY requirements.txt /opt/spark/requirements.txt

# Install Python dependencies
#RUN pip install --no-cache-dir -r /opt/spark/requirements.txt

# Download JMX Prometheus exporter
RUN mkdir -p /opt/jmx && \
    curl -L -o /opt/jmx/jmx_prometheus_javaagent.jar \
    https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar && \
    chmod 644 /opt/jmx/jmx_prometheus_javaagent.jar

COPY k8s/monitoring/jmx-exporter-config.yaml /opt/jmx/config.yaml

# Switch back to spark user
USER spark