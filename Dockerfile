FROM python:3.7.7

WORKDIR /kraken

COPY ./process_transactions.py /kraken
COPY json_transaction_data /kraken/json_transaction_data