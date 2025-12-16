import pandas as pd
from datetime import date

scrambles = pd.read_csv('WCA_export_v2_350_20251216T004759Z.tsv/WCA_export_Scrambles.tsv', sep = '\t')
results = pd.read_csv('WCA_export_v2_350_20251216T004759Z.tsv/WCA_export_Results.tsv', sep = '\t')
comps = pd.read_csv('WCA_export_v2_350_20251216T004759Z.tsv/WCA_export_Competitions.tsv', sep = '\t')


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


mbf_scrambles = recent_scrambles[recent_scrambles['eventId'] == '333mbf'].copy()

scramble_counts = (
    mbf_scrambles
    .assign(scramble_count=mbf_scrambles['scramble'].apply(mbf_scrambles_count))
    .groupby(['competitionId', 'scrambleNum', 'groupId'], as_index=False)['scramble_count']
    .sum()
)

comparison = scramble_counts.merge(
    max_attempts_per_comp[['competitionId', 'attempt_num', 'attempted']],
    left_on=['competitionId', 'scrambleNum'],
    right_on=['competitionId', 'attempt_num'],
    how='inner'
)

comparison['diff'] = comparison['scramble_count'] - comparison['attempted']
comparison['abs_diff'] = comparison['diff'].abs()

def pick_best_group(df):
    exact = df[df['diff'] == 0]

    if not exact.empty:
        row = exact.iloc[0]
        row['relation'] = 'equal'
        row['used_closest'] = False
        return row


    closest = df.loc[df['abs_diff'].idxmin()].copy()

    closest['relation'] = (
        'more scrambles than cubes attempted'
        if closest['diff'] > 0
        else 'less scrambles than cubes attempted'
    )
    closest['used_closest'] = True

    return closest

final_cases = (
    comparison
    .groupby('competitionId', as_index=False)
    .apply(pick_best_group)
    .reset_index(drop=True)
)



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

final_cases['notes'] = final_cases.apply(
    lambda r: (
        'Exact match found'
        if not r['used_closest']
        else f"Closest match used (difference = {r['diff']})"
    ) + ('; multiple scramble groups' if r['group_count'] > 1 else ''),
    axis=1
)

final_cases = final_cases.drop(columns=['group_count', 'abs_diff', 'used_closest']).to_csv('Check.csv', sep = ',')
