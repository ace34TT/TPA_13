#!/bin/bash

mongoimport --db tpa_13 --collection clients --file /vagrant/tpa_13/data/Clients_3.csv --type csv --headerline --drop
mongoimport --db tpa_13 --collection clients --file /vagrant/tpa_13/data/Clients_12.csv --type csv --headerline