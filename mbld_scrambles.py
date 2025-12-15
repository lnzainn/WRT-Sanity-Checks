import pandas as pd
from datetime import date

scrambles = pd.read_csv('WCA_export344_20251210T004812Z.tsv/WCA_export_Scrambles.tsv', sep = '\t')
results = pd.read_csv('WCA_export344_20251210T004812Z.tsv/WCA_export_Results.tsv', sep = '\t')
comps = pd.read_csv('WCA_export344_20251210T004812Z.tsv/WCA_export_Competitions.tsv', sep = '\t')


comps['end_date'] = pd.to_datetime(
    comps[['year', 'endMonth', 'endDay']].rename(
        columns={'year': 'year', 'endMonth': 'month', 'endDay': 'day'}
    )
)

cutoff = pd.Timestamp.today() - pd.DateOffset(months=3)
recent_comps = comps[comps['end_date'] >= cutoff]

recent_ids = recent_comps['id']

recent_results = results[results['competitionId'].isin(recent_ids)]
recent_scrambles = scrambles[scrambles['competitionId'].isin(recent_ids)]

recent_results[recent_results['eventId'] == '333mbf']


def mbf_scrambles_count(scramble):
    if scramble == '':
        return 0
    else:
        scrambles = scramble.split('|')
    
    return len(scrambles)



def max_cubes_attempted(result_value: int) -> int:
    
    if result_value in (0, -1, -2):
        return 0
    
    else:
        missed = int(str(result_value)[-2:])
        DD = int(str(result_value)[0:2])
        difference = 99 - DD

        solved = difference + missed
        attempted = solved + missed

    return attempted


mbf_results = recent_results[recent_results['eventId'] == '333mbf']
mbf_scrambles = recent_scrambles[recent_scrambles['eventId'] == '333mbf']

value_cols = ['value1', 'value2', 'value3']

long_results = mbf_results.melt(
    id_vars=['competitionId', 'personName'],
    value_vars=value_cols,
    var_name='attempt_col',
    value_name='result_value'
)

long_results['attempt_num'] = long_results['attempt_col'].str.extract(r'(\d)').astype(int)

long_results = long_results.dropna(subset=['result_value'])
long_results = long_results[long_results['result_value'] > 0]

# Compute cubes attempted per solve
long_results['attempted'] = long_results['result_value'].apply(max_cubes_attempted)



attempts_per_person = (
    long_results
    .sort_values('attempted', ascending=False)
    .groupby(['competitionId', 'personName'], as_index=False)
    .first()
)

max_attempts_per_comp = (
    attempts_per_person
    .sort_values('attempted', ascending=False)
    .groupby('competitionId')
    .first()
    .reset_index()
)


mbf_scrambles['scramble_count'] = mbf_scrambles['scramble'].apply(mbf_scrambles_count)

scramble_counts = (
    mbf_scrambles
    .merge(
        max_attempts_per_comp[['competitionId', 'attempt_num']],
        left_on=['competitionId', 'scrambleNum'],
        right_on=['competitionId', 'attempt_num'],
        how='inner'
    )
    .groupby(['competitionId', 'groupId'], as_index=False)['scramble_count']
    .sum()
)


comparison = scramble_counts.merge(
    max_attempts_per_comp,
    on='competitionId',
    how='inner'
)


comparison['relation'] = comparison.apply(
    lambda r: (
        'more scrambles than cubes attempted'
        if r['scramble_count'] > r['attempted']
        else 'less scrambles than cubes attempted'
        if r['scramble_count'] < r['attempted']
        else 'equal'
    ),
    axis=1
)

final_cases = comparison[comparison['relation'] != 'equal']


group_counts = (
    mbf_scrambles
    .groupby('competitionId')['groupId']
    .nunique()
    .reset_index(name='group_count')
)

final_cases = final_cases.merge(
    group_counts,
    on='competitionId',
    how='left'
)

final_cases['notes'] = final_cases['group_count'].apply(
    lambda x: 'Competition had multiple scramble groups' if x > 1 else ''
)

final_cases = final_cases.drop(columns='group_count')


final_cases[
    [
        'competitionId',
        'groupId',
        'personName',
        'attempted',
        'scramble_count',
        'relation', 
        'notes'
    ]
].to_csv('Check.csv', sep = ',')
