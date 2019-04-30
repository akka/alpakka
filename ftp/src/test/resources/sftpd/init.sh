#!/bin/bash

cp /tmp/ssh_host_ed25519_key /etc/ssh/
cp /tmp/ssh_host_rsa_key /etc/ssh/
chmod 600 /etc/ssh/ssh_host_ed25519_key
chmod 600 /etc/ssh/ssh_host_rsa_key
