import sqlite3

DB_FILENAME = 'db/tiny_etl.db'

def game_gender_ratio():
    print('Gender ratio in each game:')
    sql = '''select
             game, 
             cast(sum(case when gender = 'female' then 1 else 0 end) as float)/count(*) as female_ratio,
             cast(sum(case when gender = 'male' then 1 else 0 end) as float)/count(*) as male_ratio 
             from accounts group by game;'''

    with sqlite3.connect(DB_FILENAME) as conn:
        conn.row_factory=sqlite3.Row
        c = conn.cursor()
        c.execute(sql)
        for r in c.fetchall():
            print('{0} female: {1}, male: {2}'.format(r['game'], 
                                                      r['female_ratio'], 
                                                      r['male_ratio']))

def country_age_span():
    print('\nThe youngest and oldest player per country:')
    with sqlite3.connect(DB_FILENAME) as conn:
        conn.row_factory=sqlite3.Row
        sql = '''select country, min(age) as min_age,max(age) as max_age
                 from accounts  
                 where country is not null 
                 group by country'''
        c = conn.cursor()
        c.execute(sql)
        for r in c.fetchall():
            print('{0}: {1} {2}'.format(r['country'],r['min_age'],r['max_age']))

def data_exists():
    with sqlite3.connect(DB_FILENAME) as conn:
        # check that table exists
        c = conn.cursor()
        sql = '''SELECT count(*) FROM sqlite_master 
                WHERE type='view' AND name=?'''
        c.execute(sql, ('accounts',))
        result = c.fetchone()
        return result[0] > 0

def create_view():
    with sqlite3.connect(DB_FILENAME) as conn:
        sql = '''create view if not exists accounts 
                 as 
                 select * from hb_accounts 
                 union 
                 select * from wwc_accounts'''
        conn.execute(sql)

if __name__ == '__main__':
    create_view()
    if data_exists():
        game_gender_ratio()
        country_age_span()
    else:
        print('Data is missing. Run etl.py for both games first!')
