#!/bin/bash
docker build ./worker/ -t worker
docker build ./master/ -t master
