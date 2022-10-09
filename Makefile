dev:
	export FLASK_DEBUG=true && python3 main.py
start:
	export FLASK_DEBUG=true && gunicorn --bind 0.0.0.0:${PORT} ${WORKERS} main:app --access-logfile logs/access.log --capture-output
install:
	python3 -m pip install -r requirements.txt
