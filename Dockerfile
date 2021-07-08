FROM python:3.8.11-slim-buster

WORKDIR /app

COPY . .

RUN pip install -r requirements.txt

RUN python -m spacy download en_core_web_sm

ENTRYPOINT ["python", "main.py"]