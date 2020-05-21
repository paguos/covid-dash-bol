FROM python AS builder

RUN pip install pipenv==2018.11.26

COPY Pipfile Pipfile
COPY Pipfile.lock Pipfile.lock

RUN pipenv install --system

FROM builder AS covid-crawler

RUN apt-get update && \
    apt-get install -y locales


# Set the locale
RUN sed -i -e 's/# es_ES/es_ES/' /etc/locale.gen && \
    locale-gen
ENV LANG es_ES.UTF-8
ENV LANGUAGE es_ES:en
ENV LC_ALL es_ES.UTF-8
ENV LC_TIME es_ES
# ENV LC_TIME es_ES.UTF-8

WORKDIR /app

COPY /apps/covid-crawler/covid-crawler.py .

CMD ["python", "covid-crawler.py"]