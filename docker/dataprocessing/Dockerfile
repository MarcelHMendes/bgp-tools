FROM python:3.9-slim-bullseye

LABEL maintainer="Marcel Mendes <marcelmendes@dcc.ufmg.br>"

# Install dumb-init to support our entrypoint script and other dependencies
RUN apt-get update && apt-get install -y \
    dumb-init \
    netcat \
    sqlite3 \
    jq

# Setup application home
WORKDIR /usr/source/app

# Copy the application files
COPY scripts/ .
COPY entrypoint.sh .
COPY requirements.txt .

RUN pip install -r requirements.txt
RUN chmod u+x ./entrypoint.sh
RUN chmod 777 ./process-ripe-mesurements.py

RUN mkdir data
RUN chmod 777 data

RUN groupadd -g 10000 mmendes && \
   useradd -u 10000 -g mmendes mmendes \
   && chown -R mmendes:mmendes /usr/source/app

RUN usermod -aG sudo mmendes && \
    echo "mmendes ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

USER mmendes:mmendes

ENTRYPOINT ["/usr/source/app/entrypoint.sh"]
