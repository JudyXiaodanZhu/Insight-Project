#!/usr/bin/env bash
gunicorn --bind 0.0.0.0:5000 --workers=8 --access-logfile access.log --error-logfile error.log server:app
