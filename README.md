# Building Time Series Visualization
[![Python: 3.12](https://img.shields.io/badge/python-3.12-yellow?logo=python&logoColor=yellow
)](https://www.python.org/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

- [Building Time Series Visualization](#building-time-series-visualization)
  - [Installation Manual](#installation-manual)
    - [Git Large File Storage](#git-large-file-storage)
    - [Clone the Repository](#clone-the-repository)
    - [Run in Docker](#run-in-docker)
    - [Run Natively](#run-natively)
    - [App Usage](#app-usage)
  - [Test Suite](#test-suite)
  - [Demos](#demos)
    - [Dataset Abstraction](#dataset-abstraction)
    - [Running the App in Test Mode](#running-the-app-in-test-mode)


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

Ensure you have the [Docker Engine](https://docs.docker.com/engine/install/) 
installed and that Docker is running.

In the root directory of the repository run:

```sh
docker compose up
```

It may take up to 5 minutes to build the image for the first time.  Once built, 
it may take a minute to ingest the dataset and run the analyses.  You'll see a 
progress meter in the terminal, but once complete you should see that 
`Dash is running on http://0.0.0.0:8050/`.

Open [http://127.0.0.1:8050](http://127.0.0.1:8050) in your browser.

### Run Natively

The app was developed and tested with Python 3.12, and it is recommended to use 
the same version for compatibility.

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
python src/app.py --building B \
    datasets/bts_site_b_train/train.zip \
    datasets/bts_site_b_train/mapper_TrainOnly.csv \
    datasets/bts_site_b_train/Site_B.ttl \
    datasets/bts_site_b_train/Brick_v1.2.1.ttl
```

It may take a minute to ingest the dataset and run the analyses.  You'll see 
a progress meter in the terminal, but once complete you should see that 
`Dash is running on http://127.0.0.1:8050/`.

Open [http://127.0.0.1:8050](http://127.0.0.1:8050) in your browser.

### App Usage

Help is available by passing the `-h` or `--help` options, e.g.:

```sh
python src/app.py -h
```

## Test Suite

A comprehensive test suite is provided, requiring additional dependencies to be 
installed:

```sh
pip install -r src/tests/requirements.txt
```

Once installed, from the root directory all tests can be run with:

```sh
pytest
```

A test coverage report may be generated while running the tests with:

```sh
pytest --cov=src
```

Please note, end-to-end tests assume [Chrome](https://www.google.com/intl/en_au/chrome/dr/download/) is installed.


## Demos

### Dataset Abstraction

TODO

### Running the App in Test Mode

TODO

---


Much more TODO


---


