FROM ubuntu
RUN echo "deb http://archive.ubuntu.com/ubuntu precise main universe" > /etc/apt/sources.list
RUN echo "deb http://www.rabbitmq.com/debian/ testing main" >> /etc/apt/sources.list
RUN echo "deb http://www.rabbitmq.com/debian/ testing main" >> /etc/apt/sources.list
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ precise-pgdg main" > /etc/apt/sources.list.d/pgdg.list

RUN apt-get update

# workaround rabbitmq install problem with upstart
RUN dpkg-divert --local --rename --add /sbin/initctl
RUN ln -s /bin/true /sbin/initctl

RUN apt-get install -y --force-yes python3.2 vim make python3-amqplib python3-psycopg2
RUN apt-get -y --force-yes install rabbitmq-server postgresql-9.3 postgresql-client-9.3 postgresql-contrib-9.3

ADD . jobqueue

EXPOSE 5672 8314

CMD cd jobqueue; ./container-run.sh 
