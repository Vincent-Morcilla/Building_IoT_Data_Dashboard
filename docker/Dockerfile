FROM python:3.12

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY src .
COPY datasets/bts_site_b_train/Brick_v1.2.1.ttl .
COPY datasets/bts_site_b_train/mapper_TrainOnly.csv .
COPY datasets/bts_site_b_train/Site_B.ttl .
COPY datasets/bts_site_b_train/train.zip .

EXPOSE 8050

# CMD [ "python", "app.py", "-t" ]
CMD [ "python", "app.py" ]
