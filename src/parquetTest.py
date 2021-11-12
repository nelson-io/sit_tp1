import pandas
import pyarrow
from pyarrow import parquet

df = pandas.DataFrame({'one': [-1, 3, 2.5]},  index=list('abc'))
table =  pyarrow.Table.from_pandas(df)
parquet.write_table(table, 'testfile')
