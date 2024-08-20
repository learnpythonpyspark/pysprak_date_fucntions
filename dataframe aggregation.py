import pandas as pd
import time
import random
import string
from datetime import datetime

df1 = pd.DataFrame(data1)
df2 = pd.DataFrame(data2)

df_combined = pd.concat([df1, df2])

# Group by all relevant columns
df_grouped = df_combined.groupby(['component_type', 'component_name', 'source']).agg(
    min_record_count=('record_count', 'min'),
    max_record_count=('record_count', 'max')
).reset_index()

df_grouped['status'] = df_grouped.apply(lambda row: 'pass' if row['min_record_count'] == row['max_record_count'] else 'fail', axis=1)

# Merge back to get status for each row
final_df = pd.merge(df_combined, df_grouped[['component_type', 'component_name', 'source', 'status']], 
                    on=['component_type', 'component_name', 'source'])

# Sort the dataframe
final_df = final_df.sort_values(by=['status', 'component_type', 'component_name', 'source', 'replica_number'])

agg_funcs = {
    'status': lambda x: 'fail' if 'fail' in x.values else 'pass',
    'component_type': "first",
    'source': lambda x: ','.join(x.unique()),
    'replica_number': lambda x: ','.join(map(str, x.unique())),
    'record_count': lambda x: ','.join(map(str, x.unique()))
}

final_df = final_df.groupby('component_name').agg(agg_funcs).reset_index()

def split_record_counts(row):
    counts = row['record_count'].split(',')
    replica_numbers = row['replica_number'].split(',')
    
    if len(counts) == 1:
        return pd.Series({
            'replica1_record_count': counts[0],
            'replica2_record_count': counts[0]
        })
    else:
        replica1_count = next((count for num, count in zip(replica_numbers, counts) if num == '1'), None)
        replica2_count = next((count for num, count in zip(replica_numbers, counts) if num == '2'), None)
        return pd.Series({
            'replica1_record_count': replica1_count,
            'replica2_record_count': replica2_count
        })


final_df[['replica1_record_count', 'replica2_record_count']] = final_df.apply(split_record_counts, axis=1)
cols = final_df.columns.tolist()
cols.insert(cols.index('record_count') + 1, cols.pop(cols.index('replica1_record_count')))
cols.insert(cols.index('replica1_record_count') + 1, cols.pop(cols.index('replica2_record_count')))
final_df['AsOfDateTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
final_df['application_id'] = final_df['AsOfDateTime'].str.replace('-', '').str.replace(' ', '').str.replace(':', '')
cols = final_df.columns.tolist()
cols.insert(cols.index('AsOfDateTime') + 1, cols.pop(cols.index('application_id')))
report_status = 'pass' if (final_df['status'] == 'pass').all() else 'fail'

final_df['report_status'] = report_status


cols = final_df.columns.tolist()
cols.append(cols.pop(cols.index('report_status')))

final_df = final_df[cols]


final_df
