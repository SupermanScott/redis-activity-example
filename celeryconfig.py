# -*- coding: utf-8 -*-
# Copyright (c) 2012, Scott Reynolds
# All rights reserved.

# List of modules to import when celery starts.
CELERY_IMPORTS = ("tasks", )

## Result store settings.
CELERY_RESULT_BACKEND = "redis"
CELERY_REDIS_HOST = "localhost"
CELERY_REDIS_PORT = 6379
CELERY_REDIS_DB = 1

## Broker settings.
BROKER_URL = "redis://localhost:6379/0"
