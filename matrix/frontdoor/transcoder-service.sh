#!/bin/bash

nohup /matrix/matrix/frontdoor/transcoder-service.py &
/matrix/matrix/service/start.py
