tests:
	python test.py

install:
	python setup.py build
	python setup.py install

release:
	python setup.py sdist
	twine upload dist/*