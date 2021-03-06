FROM tiangolo/uvicorn-gunicorn-fastapi:python3.7

RUN apt update && \
    apt install -y vim certbot net-tools nmap htop

ARG SRC="/usr/src/brick-server"

COPY ./ $SRC

RUN rm -rf /app  && \
    mv $SRC /app

RUN pip install --upgrade pip && \
    pip install -r /app/requirements.txt && \
    pip uninstall -y fastapi && \
    pip install git+https://github.com/jbkoh/fastapi.git@fix-bodyparsing

ENV BRICK_CONFIGFILE "/app/configs/configs.json"

CMD /app/docker/start.sh
