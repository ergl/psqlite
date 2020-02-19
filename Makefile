all: test

test:
	ponyc -d psqlite_test -o _build -b test_sqlite
	./_build/test_sqlite

clean:
	rm -rf _build
