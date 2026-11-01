#!/bin/bash
set -e

if [ $# -ne 1 ]; then
    echo "Uso: bash actualizar_ip.sh <IP_PUBLICA_AWS>"
    exit 1
fi

IP="$1"

if [[ ! "$IP" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]]; then
    echo "Error: formato de IP incorrecto"
    exit 1
fi

sed -i -E \
  "s#PLAINTEXT_HOST://[^,[:space:]]+:9092#PLAINTEXT_HOST://${IP}:9092#g" \
  compose.yaml

echo "IP de Kafka actualizada a: $IP"
echo "Recuerda recrear Kafka y utilizar esta IP en los PCs."