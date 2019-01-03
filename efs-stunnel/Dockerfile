FROM registry.access.redhat.com/rhel7:latest
ARG STUNNEL_ARCHIVE_URL=https://www.stunnel.org/downloads/stunnel-5.50.tar.gz

USER 0

RUN yum install -y \
      gcc \
      make \
      python \
      python-requests \
      python-setuptools \
      openssl-devel \
      tcp_wrappers-devel && \
    yum clean all

RUN cd /usr/local/src && \
    curl -s -o stunnel.tar.gz $STUNNEL_ARCHIVE_URL && \
    tar xzvf stunnel.tar.gz && \
    cd stunnel-*/ && \
    ./configure && \
    make && \
    make install && \
    cd /usr/local/src && \
    rm -rf stunnel-*/

RUN easy_install pip && \
    pip install --ignore-installed \
      awscli \
      kubernetes

COPY efs-utils.crt /etc/amazon/efs/efs-utils.crt
COPY efs-stunnel.py /usr/local/bin/efs-stunnel.py

CMD /usr/local/bin/efs-stunnel.py
