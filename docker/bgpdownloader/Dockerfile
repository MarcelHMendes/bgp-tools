FROM caida/bgpstream:latest

LABEL maintainer="Marcel Mendes <marcelmendes@dcc.ufmg.br>"

# Install dumb-init to support our entrypoint script
RUN apt-get update && \
    apt-get install -y wget jq
RUN wget https://github.com/Yelp/dumb-init/releases/download/v1.2.5/dumb-init_1.2.5_amd64.deb \
    && dpkg -i dumb-init_*.deb

WORKDIR /usr/src/app

COPY lib.py .
COPY bgpstream-downloader.py .
COPY entrypoint.sh .

RUN chmod u+x ./entrypoint.sh

RUN mkdir -p /usr/src/app/data/

RUN groupadd -g 1001 mmendes && \
    useradd -u 1001 -g mmendes -m mmendes && \
    mkdir -p /usr/src/app/data && \
    chown -R 1001:1001 /usr/src/app/data && \
    chown -R 1001:1001 /usr/src/app

USER mmendes

ENTRYPOINT ["./entrypoint.sh"]
