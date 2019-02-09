#!/usr/bin/env bash
gunicorn --bind 0.0.0.0:5001 --workers=8 --access-logfile access.log --error-logfile error.log server:app
