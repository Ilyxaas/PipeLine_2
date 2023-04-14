FROM python:3.9.7 as python-base

WORKDIR /pipelines
RUN pip install --upgrade pip

ENV POETRY_VERSION=1.4.0
ENV POETRY_HOME=/opt/poetry
ENV POETRY_BIN=$POETRY_HOME/bin/poetry

# Create stage for Poetry installation
FROM python-base as poetry-base

# Creating a virtual environment just for poetry and install it with pip
RUN python3 -m venv $POETRY_HOME \
    && $POETRY_HOME/bin/pip install poetry==${POETRY_VERSION}

# Copy Dependencies
COPY poetry.lock pyproject.toml ./

# Install Dependencies
RUN $POETRY_BIN config --local virtualenvs.create false
RUN $POETRY_BIN install --no-root

# Copy Application
COPY ./example_pipeline/data /pipelines/example_pipeline/data
COPY ./example_pipeline/pipeline.py /pipelines/example_pipeline/
COPY ./pipelines /pipelines/pipelines
COPY ./README.md /pipelines
RUN pip install .

# Run Application
CMD ["python", "./example_pipeline/pipeline.py"]