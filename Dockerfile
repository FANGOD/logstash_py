
FROM python:3.11.4-alpine3.17

WORKDIR /pylogstash

COPY pylogstash/ .

ENV TZ=Asia/Shanghai

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# RUN echo "0 10 * * * python check_mongo.py" > /etc/crontabs/root
RUN echo "*/5 * * * * python check_mongo.py" > /etc/crontabs/root

COPY requirements.txt /tmp/requirements.txt

RUN pip install --upgrade pip && \
    pip install -r /tmp/requirements.txt -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com && \
    rm -rf /root/.cache/pip /tmp/requirements.txt

ENV PYTHONIOENCODING = utf-8

CMD ["sh", "-c", "crond && python workers.py"]
