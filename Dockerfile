FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV LLM_PROVIDER=anthropic

EXPOSE 8080

CMD ["python", "-m", "mapping_generator", "--serve", "--port", "8080"]
