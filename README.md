# front
gdelt analytics "front" == "gateway"

## usage
gunicorn --reload -b 0.0.0.0:8000 main:api