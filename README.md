# Building Time Series Visualization

## Installation Manual

### Git Large File Storage

This repository uses [Git Large File Storage](https://git-lfs.com/) to manage 
its large datasets.  Download and install Git LFS from [https://git-lfs.com/](https://git-lfs.com/).  Once downloaded and installed, set up GitLFS for your user account by 
running:

```sh
git lfs install
```

### Clone the Repository

Clone the repository, then to retrieve the datasets, in the root directory of 
the repository run:

```sh
git lfs pull
```

### Run in Docker

In the root directory of the repository run:

```sh
docker compose up
```

It may take a minute to ingest the dataset and run the analyses.  You'll see 
a progress meter in the terminal, but once complete you should see that `Dash is running on http://0.0.0.0:8050/`.

Open [http://127.0.0.1:8050](http://127.0.0.1:8050) in your browser.


### Run Natively

Create a virtual environment for the app:

```sh
python -m venv .venv
```

Activate the virtual environment (see [Python Docs](https://docs.python.org/3/library/venv.html#how-venvs-work) for commands on other platforms):

```sh
source .venv/bin/activate
```

Install the dependencies:

```sh
pip install -r requirements.txt
```

Run the app:

```sh
python src/app.py \
    datasets/bts_site_b_train/train.zip \
    datasets/bts_site_b_train/mapper_TrainOnly.csv \
    datasets/bts_site_b_train/Site_B.ttl \
    datasets/bts_site_b_train/Brick_v1.2.1.ttl
```

It may take a minute to ingest the dataset and run the analyses.  You'll see 
a progress meter in the terminal, but once complete you should see that `Dash is running on http://0.0.0.0:8050/`.

Open [http://127.0.0.1:8050](http://127.0.0.1:8050) in your browser.

### App Usage

Help is available by passing the `-h` or `--help` options:

```sh
usage: app.py [-h] [-d] [-a HOST] [-p PORT] [-t] [data] [mapper] [model] [schema]

Building Time Series Visualization

positional arguments:
  data                  Path to the timeseries zip file (default: None)
  mapper                Path to the mapper csv file (default: None)
  model                 Path to the model ttl file (default: None)
  schema                Path to the schema ttl file (default: None)

options:
  -h, --help            show this help message and exit
  -d, --debug           Enable Flask debug mode (default: False)
  -a HOST, --host HOST  Host address used to serve the application (default: 0.0.0.0)
  -p PORT, --port PORT  Port used to serve the application (default: 8050)
  -t, --test-mode       Enable test mode (no file paths required) (default: False)
```

---


Much more TODO


---

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
