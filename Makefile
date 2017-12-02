tests:
	python test.py

install:
	python setup.py build
	python setup.py install

release:
	twine upload dist/*