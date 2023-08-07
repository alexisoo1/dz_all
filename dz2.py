def dz2():
    import config
    import logging 
    from datetime import datetime
    import requests
    from bs4 import BeautifulSoup
    import tools

    print()
    print(f"dz2 started {datetime.now()}")

    if not tools.checkExistsTable(_table = 'vacancies'):
        tools.createVacanciesTable(_recreateTable = False)
    
    if not tools.checkExistsTable(_table = 'key_skills'):
        tools.createKey_skillsTable(_recreateTable = False)
    
    urlBase = f'https://hh.ru/search/vacancy?text={config.searchText}&area=1'

    i = 0
    j = 0
    lines = []
    while True:
        if i >=1:
            url = f'{urlBase}&page={i}'
        else:
            url = urlBase

        result = requests.get(url, headers=config.user_agent)
        i+=1

        print(result.status_code)

        with open(f'page.txt{i}', 'w', encoding='UTF-8') as f:
            f.write(result.content.decode())
        # Создаем объект soup на основе загруженной Web-страницы
        soup = BeautifulSoup(result.content.decode(), 'lxml')
        
        names = soup.find_all('a', attrs={'data-qa': 'serp-item__title'})
        ln = len(names)
        if ln == 0:
            break
        print(ln)
        if i > 10:
            break
        for name in names:
            j+=1
            lines.append([j, '', name.text, '', name.get('href'), ''])

    print('!!!!!!!!!!! end of loop')
    print(len(lines))

    con = tools.getDbConnection()
    cursor = con.cursor()

    if len(lines) > 0:
        sql = "insert into vacancies (id,company_name,position,job_description,link, key_skills) values(?,?,?,?,?,?)"
        con.executemany(sql, lines)                
    con.commit()

    #con = tools.getDbConnection()
    sql = "select id, link from vacancies"
    results = con.execute(sql).fetchall()

    for row in results:
        result = requests.get(row[1], headers=config.user_agent)
        print(result.status_code)
        # Создаем объект soup на основе загруженной Web-страницы
        soup = BeautifulSoup(result.content.decode(), 'lxml')
        
        pos = soup.find('h1')
        if pos:
            print(pos.text)
            print(pos.attrs)
        
        com = soup.find('a', attrs={'data-qa': 'vacancy-company-name'})
        if com:
            print(com.text)

        com = soup.find('div', {"class": "vacancy-company-details"})
        if com:
            print(com.text)

        des = soup.find('div', {"class": "g-user-content"}) 
        if des != None:
            descr = des.text
            if len(descr)>0:
                descr = descr.replace("'","")
            if descr:
                print(descr)
        else:
            descr = ""

        ski = soup.find('div', {"class": "bloko-tag-list"}) 
        skills = []
        key_skills = []
        if ski:
            for s in ski:
                print(s.text)
                skills.append(s.text)
                key_skills.append([row[0], com.text, s.text, tools.search_name(com.text)])
            print(ski.text)
        
        sql = f"update vacancies set company_name = '{com.text}', position = '{pos.text}', job_description = '{descr}', key_skills = '{','.join(skills)}' where id = {row[0]}"
        con.execute(sql)
        con.commit()

        table = 'key_skills'    
        sql = f"insert into {table}(id, company_name, key_skill, search_name) values(?,?,?,?)"
        con.executemany(sql, key_skills)
        con.commit()
        
    print('end')