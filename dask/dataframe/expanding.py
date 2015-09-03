from ..base import tokenize
import pandas as pd
from toolz import merge


def take_last(x):
    return x.iloc[-1]


def expanding_reduction(x, chunk, agg1, agg2, token=None):
    token_key = tokenize(x, token or (chunk, agg1, agg2))
    token = token or 'expanding'

    # Map chunk across all blocks
    a = '{0}--chunk-{1}'.format(token, token_key)
    dsk1 = dict(((a, i), (chunk, (x._name, i))) for i in range(x.npartitions))

    # Get last value across all blocks
    b = '{0}--take_last{1}'.format(token, token_key)
    dsk2 = dict(((b, i), (take_last, (a, i))) for i in range(x.npartitions))
    last_vals = [(b, i) for i in range(x.npartitions)]

    # Apply agg1 to all chunks, with previous values
    c = '{0}--agg1-{1}'.format(token, token_key)
    dsk3 = dict(((c, i), (agg1, (a, i), last_vals[:i]))
                for i in range(x.npartitions))

    # Apply agg2 to all chunks, if there
    if agg2:
        d = '{0}--agg2-{1}'.format(token, token_key)
        dsk4 = dict(((d, i), (agg2, (c, i))) for i in range(x.npartitions))
        dsk = merge(x.dask, dsk1, dsk2, dsk3, dsk4)
        out = d
    else:
        dsk = merge(x.dask, dsk1, dsk2, dsk3)
        out = c
    return type(x)(dsk, out, x.column_info, x.divisions)


def expanding_sum(x):
    return expanding_reduction(x, pd.expanding_sum,
                               lambda x, priors: x + sum(priors),
                               None, 'expanding_sum')


def expanding_count(x):
    return expanding_reduction(x, pd.expanding_count,
                               lambda x, priors: x + sum(priors),
                               None, 'expanding_count')


def expanding_mean(x):
    def chunk(x):
        sums = pd.expanding_sum(x)
        cnts = pd.expanding_count(x)
        cnts.columns = ['__counts__' + str(c) for c in cnts.columns]
        return pd.concat([sums, cnts], 1)
    agg1 = lambda x, priors: x + sum(priors)
    def agg2(x):
        n = x.columns.size/2
        sums = x.ix[:, :n]
        cnts = x.ix[:, n:]
        return sums / cnts.rename(columns=dict(zip(cnts.columns, sums.columns)))
    return expanding_reduction(x, chunk, agg1, agg2, 'expanding_mean')
