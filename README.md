<p align="center">
<img src="assets/cashmere.png" width="128" height="128" />
</p>

# Summary
Cashmere is an async-python library built to enable a light-weight event driven communication architecture.

The current implementation leverages AWS SNS and AWS SQS.

Event names have corresponding topics created in AWS SNS.
Event subscribers have corresponding queues created in AWS SQS.
These SQS queues are then subscribed to SNS topics for the corresponding events.

When an event is emitted, all subscribers will receive a copy of the event.

All subscribers are expected to be Async functions.

# What we do have
- Clear decoupling between clients and consumers.
- An interface that can support other back-ends in the future (e.g. RMQ, Redis.)
- A simple dependency injection system for subscribing handlers.

# What we don't have today
- Memory and Concurrency management options.

# What's planned next:
1. Add a graceful shutdown solution for ctrl+c and sig-term moments.
2. Support metrics via open-telemetry. 

--

# Tooling Setup for Local Development
All instructions are for MacOS

You will need
- Brew
- Python 3.11
- Poetry

## Install Brew

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

## Install Pyenv

```bash
brew install pyenv
```

## Install Python 3.11

```bash
pyenv install 3.11
```

## Install Poetry

Set your python version before you install poetry
```bash
pyenv shell 3.11
```

Install Poetry
```bash
pip install poetry
```

## Install Pre-commit

```bash
brew install pre-commit
```

# Install Project

## Initialize python environment

You will run these two commands every time you open a new shell environment for this project.
The commands must be run in the project's root directory.

```bash
pyenv shell 3.11
poetry shell
```
## Install project dependencies

```bash
poetry install --sync --with=dev
```

## Setup Pre-commit

```bash
pre-commit install
```
