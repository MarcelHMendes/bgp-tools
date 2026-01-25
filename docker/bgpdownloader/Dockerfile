FROM caida/bgpstream:latest

LABEL maintainer="Marcel Mendes <marcelmendes@dcc.ufmg.br>"

# Install dumb-init to support our entrypoint script
RUN apt-get update && /
    apt-get install -y wget jq
RUN wget https://github.com/Yelp/dumb-init/releases/download/v1.2.5/dumb-init_1.2.5_amd64.deb \
    && dpkg -i dumb-init_*.deb

WORKDIR /usr/source/app

COPY lib.py .
COPY bgpstream-downloader.py .
COPY entrypoint.sh .

RUN chmod u+x ./entrypoint.sh

RUN groupadd -g 10001 caida && \
   useradd -u 10000 -g caida caida \
   && chown -R caida:caida /usr/source/app

USER caida:caida

ENTRYPOINT ["./entrypoint.sh"]
