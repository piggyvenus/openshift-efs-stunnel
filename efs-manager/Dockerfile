FROM registry.access.redhat.com/rhel7:latest

USER 0

RUN yum install -y \
      python \
      python-requests \
      python-setuptools && \
    yum clean all

RUN easy_install pip && \
    pip install setuptools --upgrade && \
    pip install --ignore-installed \
      awscli \
      boto3 \
      kubernetes

COPY efs-manager.py /usr/local/bin/efs-manager.py

CMD /usr/local/bin/efs-manager.py
