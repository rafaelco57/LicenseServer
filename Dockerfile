FROM python:3.9-slim
WORKDIR /app

# Copie apenas os arquivos necessários primeiro (para cache eficiente)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Agora copie o resto do código
COPY . .

EXPOSE 8000
CMD ["uvicorn", "src.License_Server:app", "--host", "0.0.0.0", "--port", "8000"]
