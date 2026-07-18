FROM prom/prometheus:latest

COPY prometheus.yml /etc/prometheus/prometheus.yml

# Re-state the image's default CMD to append the retention flag (CMD is
# replaced wholesale, so config file and storage path must be repeated).
CMD [ \
    "--config.file=/etc/prometheus/prometheus.yml", \
    "--storage.tsdb.path=/prometheus", \
    "--storage.tsdb.retention.time=60d" \
]
