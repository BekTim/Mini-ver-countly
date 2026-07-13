#!/bin/bash

GREEN='\e[32m'
RED='\e[31m'
NC='\e[0m'

RESPONSE=$(curl --silent localhost:8123)
cnt=0

if [[ "$RESPONSE" == *"Ok."* ]]; then
    ((cnt++))
else
    echo "ClickHouse is not available or returned unexpected response: '$RESPONSE'"
fi

if nc -z localhost 9092 &> /dev/null; then
    ((cnt++))
else
    echo 'Kafka is not available'
fi

if [[ "$cnt" -eq 2 ]]; then
    echo -e "${GREEN}OK${NC}"
else
    echo -e "${RED}FAIL${NC}"
fi