# VERSION:	0.1
# AUTHOR:	Xiaotian Wu
# DESCRIPTION:	Image of matrix ,
#               matrix is a framework similar as Marathon

FROM centos
MAINTAINER Xiaotian Wu <xiaotian.wu@chinacache.com>

# install mesos python lib
RUN yum install -y wget
RUN wget http://downloads.mesosphere.io/master/centos/7/mesos-0.21.0-py2.7-linux-x86_64.egg
RUN wget --no-check-certificate https://bootstrap.pypa.io/ez_setup.py
RUN python ez_setup.py --insecure
RUN yum install -y subversion-devel
RUN easy_install mesos-0.21.0-py2.7-linux-x86_64.egg

# kazoo for zookeeper api
RUN easy_install kazoo

# python Red-Black Tree
RUN yum install -y gcc
RUN yum install -y python-devel
RUN easy_install rbtree

# web framework
RUN easy_install flask

# for restful service
RUN easy_install pip
RUN pip install flask-restful

# matrix itself
ADD . /matrix
ENV PYTHONPATH /matrix

# for dev
RUN yum install -y vim
RUN yum install -y git

# a temprory entrypoint
RUN chmod +x /matrix/matrix/service/leader.py
ENTRYPOINT ["/matrix/matrix/service/leader.py"]
