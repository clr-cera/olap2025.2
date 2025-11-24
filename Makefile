.PHONY: clean package test-etl test-quality run-local

clean:
	rm -rf dist/
	rm -rf *.zip
	find . -type d -name "__pycache__" -exec rm -rf {} +

package: clean
	mkdir -p dist
	zip -r dist/etl.zip etl/ -x "etl/__pycache__/*"

test-etl:
	uv run pytest tests/etl

test-quality:
	uv run pytest tests/quality --dist-dir=dist/

submit-local: package
	spark-submit --py-files dist/etl.zip main.py

run-local:
	uv run python main.py --config config/local_pqt_config.json