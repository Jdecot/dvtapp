FROM python:3.10-slim-buster

COPY pyproject.toml poetry.lock ./
RUN pip install poetry
RUN poetry config virtualenvs.create false \
    && poetry install --no-dev

WORKDIR /dvtapp
COPY dvtapp/ ./dvtapp/

EXPOSE 8080

CMD ["poetry", "run", "python", "dvtapp/main.py"]