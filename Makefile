tests:
	python test.py

clean:
	rm -rf .pytest_cache/ build/ dist/ pdw_convoy_core.egg-info/ venv/

install:
	echo "Installing virtual environment..."
	test -d venv || virtualenv venv -p python3
	echo "Installing python requirements..."
	./venv/bin/pip install -e ".[dev]"
	echo "Installing git hooks..."
	./venv/bin/pre-commit install
	echo "All done!"

build: install
	rm -rf dist/
	./venv/bin/python  setup.py sdist bdist_wheel
	./venv/bin/python  setup.py sdist bdist_egg

release: build
	./venv/bin/twine upload dist/*
