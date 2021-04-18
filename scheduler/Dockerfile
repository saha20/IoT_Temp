FROM python:3-alpine
WORKDIR /myapp
COPY . /myapp
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt
EXPOSE 13337
CMD ["python3","-u","scheduler_main.py"]
