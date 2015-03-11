#!/bin/bash

nohup /matrix/matrix/frontdoor/transcoder/transcoder-service.py &
/matrix/matrix/service/start.py
