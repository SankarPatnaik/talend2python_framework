.PHONY: install test convert
install:
	pip install -e .
	pip install pytest flake8
test:
	pytest -q
convert:
	talend2py convert --input examples/jobs/sample_talend_job/sample_job.item --out build/sample --engine pyspark
