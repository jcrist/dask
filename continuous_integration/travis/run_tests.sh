# Need to make test order deterministic when parallelizing tests, hence PYTHONHASHSEED
# (see https://github.com/pytest-dev/pytest-xdist/issues/63)
if [[ $PARALLEL == 'true' ]]; then
    export XTRATESTARGS="-n3 $XTRATESTARGS"
    export PYTHONHASHSEED=42
fi

if [[ $COVERAGE == 'true' ]]; then
    coverage run `which py.test` dask -s --runslow --doctest-modules --verbose $XTRATESTARGS
else
    py.test dask -s --runslow --verbose $XTRATESTARGS
fi
