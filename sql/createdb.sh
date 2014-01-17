#!/bin/bash

dropdb jobqueue
createdb jobqueue

psql jobqueue -f schema.sql
