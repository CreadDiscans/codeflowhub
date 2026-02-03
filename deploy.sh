rm -rf dist
rm -rf build
python -m build
python -m twine upload dist/*