#!/bin/bash

set -e

if [ -e "/opt/airflow/requirements.txt" ]; then
    $(command -v pip) install --user -r /opt/airflow/requirements.txt
fi

airflow db upgrade &

exec airflow scheduler