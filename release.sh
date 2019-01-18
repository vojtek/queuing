#!/bin/bash
pip3 install twine --upgrade \
	&& python3 setup.py sdist bdist_wheel \
	&& twine upload dist/*
