taskcluster-jobqueue
====================

Job queue implementation

Run `make docker-build` to create docker image, run `make docker-run` to run
project from within docker. Make sure to configure docker first, see
instructios below.

Docker Configuration
--------------------
You need linux kernel >= 3.8, then on most distros you can install and configure
docker for development as follows:
 1. Install docker: `curl -sL https://get.docker.io/ | sh` or go to:
    http://www.docker.io/gettingstarted/ 
 2. Make your self a member of the docker group: `sudo groupadd <user> docker`
 3. Enable ipv4 forwarding with:
    `sudo echo 'net.ipv4.ip_forward = 1' > /etc/sysctl.d/20-docker-ipv4-forwarding.conf`

