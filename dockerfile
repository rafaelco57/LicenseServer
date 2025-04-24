FROM python:3.9
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
CMD ["python", "-m", "uvicorn", "src.License_Server:app", "--host", "0.0.0.0", "--port", "8000"]
