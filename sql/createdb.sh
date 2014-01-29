#!/bin/bash

# create database
dropdb jobqueue
createdb jobqueue

# populate tables
psql jobqueue -f schema.sql
