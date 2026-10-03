#!/bin/sh
set -eu
superset db upgrade
superset fab create-admin --username admin --firstname Student --lastname Student --email student@example.local --password admin12345 || superset fab reset-password --username admin --password admin12345
superset init
exec gunicorn --bind 0.0.0.0:8088 --workers 2 --timeout 120 'superset.app:create_app()'
