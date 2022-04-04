set shell := ["powershell.exe", "-c"]

test:
 coverage run -m unittest discover
 coverage html
 Start-Process "http://localhost:8000/htmlcov/"
 python -m http.server

bump:
 bump2version patch

publish:
 python setup.py sdist bdist_wheel
 twine check dist/*
 twine upload dist/*
