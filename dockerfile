# Imagem base
FROM python:3.11-slim

# Diretório de trabalho
WORKDIR /app

# Copiar arquivos
COPY . .

# Instalar dependências
RUN pip install --upgrade pip && pip install -r requirements.txt

# Expor a porta que a FastAPI usará
EXPOSE 5000

# Comando para iniciar o servidor
CMD ["uvicorn", "aws_service_fastapi:app", "--host", "0.0.0.0", "--port", "5000"]
