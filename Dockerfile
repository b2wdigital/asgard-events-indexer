FROM python:3.7-alpine

#Version: 0.2.1

WORKDIR /opt/app

RUN pip install -U pip \
    && pip install pipenv

COPY . /opt/app

RUN apk -U add --virtual .deps gcc g++ make python3-dev \
&& pipenv install --system --deploy --ignore-pipfile \
&& apk --purge del .deps

CMD ["python", "-m", "indexer"]
