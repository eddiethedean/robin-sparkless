.PHONY: build test clean install dev

build:
	maturin build --release

dev:
	maturin develop

test:
	pytest tests/ -v

clean:
	cargo clean
	rm -rf target/
	rm -rf *.egg-info/
	rm -rf dist/

install:
	pip install -e .
