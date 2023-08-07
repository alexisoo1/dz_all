def dz3():
    import config
    import logging 
    from datetime import datetime
    import tools

    print()
    print(f"dz3 started {datetime.now()}")

    con = tools.getDbConnection()
    sql = '''
    select count(key_skill), key_skill from key_skills ks
    join telecom_companies tc on tc.search_name = ks.search_name
    order by count(key_skill) desc
    limit 10
'''    
    results = con.execute(sql).fetchall()
    print('Top 10 key skills:')
    for res in results:
        print(f'{res[1]} - {res[0]}')
