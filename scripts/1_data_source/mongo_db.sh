#!/bin/bash

mongoimport --db tpa_13 --collection clients --file /vagrant/TPA_13/data/Clients_3.csv --type csv --headerline
mongoimport --db tpa_13 --collection clients --file /vagrant/TPA_13/data/Clients_12.csv --type csv --headerline