FROM lsstsqre/centos:7-stack-lsst_distrib-d_2021_11_13 as lsststack

USER root
RUN yum -y update
RUN yum -y install python3
RUN python3 -V
RUN yum -y install mlocate
RUN updatedb
RUN yum -y install glibc-static

RUN mkdir -p /opt/lsst/software/server
WORKDIR /opt/lsst/software/server
COPY . .

# # Install production dependencies.
RUN pip3 install --upgrade pip setuptools wheel pep517
RUN pip3 install --no-cache-dir -r requirements.txt

WORKDIR /opt/lsst/software/server/src

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
 