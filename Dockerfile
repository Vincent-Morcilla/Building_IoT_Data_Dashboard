FROM python:3.12

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY src .
COPY datasets/bts_site_b_train/train.zip dataset/
COPY datasets/bts_site_b_train/mapper_TrainOnly.csv dataset/
COPY datasets/bts_site_b_train/Site_B.ttl dataset/
COPY datasets/bts_site_b_train/Brick_v1.2.1.ttl dataset/

EXPOSE 8050

# Run the app, binding to all interfaces to expose it to the outside world
# and filtering the dataset to just building B.
CMD [ "python", "app.py", \
      "--host", "0.0.0.0", \
      "--building", "B", \
      "dataset/train.zip", \
      "dataset/mapper_TrainOnly.csv", \
      "dataset/Site_B.ttl", \
      "dataset/Brick_v1.2.1.ttl" ]
