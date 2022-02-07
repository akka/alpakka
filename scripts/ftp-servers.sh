#!/bin/bash

set -x

export FTP_USER_UID=$(id -u)
export FTP_USER_GID=$(id -g)

docker-compose up -d ftp sftp squid
