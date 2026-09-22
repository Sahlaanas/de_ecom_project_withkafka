FROM apache/airflow:3.3.2

USER root

# Confluent's own librdkafka is current; Debian's is too old for
# confluent-kafka's admin-client features. Detect the Debian codename
# so the repo line matches whatever release this base image uses.
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl gnupg ca-certificates \
    && CODENAME=$(. /etc/os-release && echo "$VERSION_CODENAME") \
    && curl -fsSL https://packages.confluent.io/clients/deb/archive.key \
        | gpg --dearmor -o /usr/share/keyrings/confluent-clients.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/confluent-clients.gpg] https://packages.confluent.io/clients/deb ${CODENAME} main" \
        > /etc/apt/sources.list.d/confluent-clients.list \
    && apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        librdkafka-dev \
        libssl-dev \
        libsasl2-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Create the dbt venv while still root (airflow can't write to /opt),
# then hand ownership to the airflow user so it can install into it.
RUN python -m venv /opt/dbt_venv && \
    chown -R airflow:root /opt/dbt_venv

USER airflow

RUN pip install --no-cache-dir --upgrade pip setuptools wheel

RUN pip install --no-cache-dir \
    confluent-kafka==2.5.3 \
    pyarrow==18.1.0 \
    boto3==1.35.0 \
    python-dotenv==1.0.1 \
    faker==30.0.0

RUN /opt/dbt_venv/bin/pip install --no-cache-dir --upgrade pip setuptools wheel && \
    /opt/dbt_venv/bin/pip install --no-cache-dir dbt-snowflake==1.7.1