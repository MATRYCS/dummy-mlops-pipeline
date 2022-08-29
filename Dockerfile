FROM apache/airflow:2.3.3-python3.9

USER root
RUN : \
    && sudo apt-get update \
    && sudo apt-get install -y git \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && :

USER airflow