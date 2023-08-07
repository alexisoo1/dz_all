def dz1():
    import config
    import json
    import sqlite3 as sl
    import logging 
    import timeit
    import pandas as pd
    import zipfile
    from pathlib import Path
    import csv
    import sys
    import tools
    from datetime import datetime
    logger = logging.getLogger(__name__)
    logger.setLevel(config.loggerLevel)
    lfm = logging.Formatter(config.formatter)
    lsh = logging.StreamHandler()
    lsh.setLevel(config.streamLogHandlerLevel)
    lsh.setFormatter(lfm)
    lfh = logging.FileHandler(filename='log.log', mode='w')
    lfh.setFormatter(lfm)
    lfh.setLevel(config.fileLogHandlerLevel)
    logger.addHandler(lsh)
    logger.addHandler(lfh)

    print(f"dz1 started {datetime.now()}")
    logger.info(f"dz1 started {datetime.now()}")

    okvedZip = Path(config.dataDir, config.okvedZip)
    okved = Path(config.dataDir, Path(config.okvedZip).stem, config.okved)
    okvedPath = Path(config.dataDir, Path(config.okvedZip).stem)
    with zipfile.ZipFile(file=okvedZip, mode='r') as zipObj:
        files = zipObj.namelist()
        print(files)
        zipObj.extract(config.okved, path=okvedPath)

    with open(okved, 'r', encoding='UTF-8') as f:
        data = json.load(f)
        print(type(data))

    tools.createDB(_db = config.dbName, _recreateDB = False)

    if not tools.checkExistsTable(_table = 'status'):
        tools.createStatusTable(_recreateTable = False)
    else:
        logger.info('Table status already exists')

    if not tools.checkExistsTable(_table = 'journal'):
        tools.createJournalTable(_recreateTable = False)
    else:
        logger.info('Table journal already exists')

    if not tools.checkExistsTable(_table = 'okved'):
        tools.createOkvedTable(_recreateTable = False)
    else:
        logger.info('Table okved already exists')

    if not tools.checkExistsTable(_table = 'teltecom_companies'):
        tools.createTelecom_companiesTable(_recreateTable = False)
    else:
        logger.info('Table teltecom_companies already exists')

    # if not tools.checkExistsTable(_table = 'vacancies'):
    #     tools.createVacanciesTable(_recreateTable = False)
    # else:
    #     logger.info('Table vacancies already exists')

    # if not tools.checkExistsTable(_table = 'key_skills'):
    #     tools.createKey_skillsTable(_recreateTable = False)
    # else:
    #     logger.info('Table key_skills already exists')

    con = tools.getDbConnection()
    cursor = con.cursor()
    try:
        table = 'okved'    
        sql = f"insert into {table}(code, parent_code, section, name, comment) values(:code, :parent_code, :section, :name, :comment)"
        con.executemany(sql, data)
        con.commit()
        pass
    except Exception as err:
        print(f"{err}")
        con.rollback()
    finally:
        con.close()

    egrulZip = Path(config.dataDir, config.egrulZip)
    egrulPath = Path(config.dataDir, Path(config.egrulZip).stem)
    try:
        con = tools.getDbConnection()
        sqlCheck = "select started, unzipped, processed from status"
        data = con.execute(sqlCheck).fetchone()
        (started, unzipped, processed) = tuple(data)
        if started == 0:
            sql = f"update status set started = 1"
            con.execute(sql)        
            zipObj = zipfile.ZipFile(file=egrulZip, mode='r')
            files = zipObj.namelist()
            files = [[x] for x in files]
            sqlNames = f"insert into journal (id) values(?)"
            con.executemany(sqlNames, files)
            con.commit()
    except Exception as err:
        print(err)
        con.rollback()
        sys.exit()
    finally:
        con.close()

    if processed == 1:
        print('Уже все обработано')
        sys.exit()

    print(f"Начата разархивация {datetime.now()}")
    logger.info(f"Начата разархивация {datetime.now()}")

    if unzipped == 0:
        try:        
            zipObj = zipfile.ZipFile(file=egrulZip, mode='r')
            con = tools.getDbConnection()
            sql = "select id from journal where unzipped = 0"
            files = con.execute(sql).fetchall()
            files = [x[0] for x in files]
            ff = zipObj.namelist()
            for file in files:
                res = zipObj.extract(file, path=egrulPath)
                sql = f"update journal set unzipped = 1 where id = '{file}'"
                con.execute(sql)
                con.commit()
            sql = "select id from journal where unzipped = 0"
            files = con.execute(sql).fetchall()
            if len(files) == 0:
                sqlEnd = "update status set unzipped = 1"
                con.execute(sqlEnd)
                con.commit()
        except Exception as err:
            print(err)
            con.rollback()
            sys.exit()
        finally:            
            con.close()

    print(f"Завершена разархивация {datetime.now()}")
    logger.info(f"Завершена разархивация {datetime.now()}")

    print(f"Начата обработка {datetime.now()}")
    logger.info(f"Начата обработка {datetime.now()}")

    try:
        con = tools.getDbConnection()
        sql = "select id from journal where processed = 0"
        files = con.execute(sql).fetchall()
        files = [x[0] for x in files]
        for file in files:
            ff = Path(egrulPath, file)
            with open(Path(egrulPath, file), 'r', encoding='UTF-8') as f:
                data = json.load(f)
                print(f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!   {file}")
                insertList = []
                print(type(data))
                for d in data:
                    print(d.get('name',''))
                    isTelecom = False
                    dt = d.get('data',{})
                    svOkved = dt.get('СвОКВЭД',{})
                    svOkvedOsn = svOkved.get('СвОКВЭДОсн',{})
                    code = svOkvedOsn.get('КодОКВЭД', '')    
                    osn = False            
                    if code != '' and code[:2] == '61':
                        print('!!!!!!!!!!1')
                        isTelecom = True
                        osn = True
                    else:
                        codeDopList = svOkved.get('СвОКВЭДДоп', [])
                        aa = type(codeDopList)
                        if isinstance(codeDopList, list):
                            codes = []
                            for l in codeDopList:
                                code = l.get('КодОКВЭД', '')
                                st = code[:2]
                                if code != '' and code[:2] == '61':
                                    print('!!!!!!!!!!1')
                                    isTelecom = True                            
                                    break
                        elif isinstance(codeDopList, dict):
                            code = codeDopList.get('КодОКВЭД', '')
                            st = code[:2]
                            if code != '' and code[:2] == '01':
                                print('!!!!!!!!!!1')
                                isTelecom = True        
                    if isTelecom and osn:
                        insertList.append([d.get('ogrn',''),d.get('inn',''),d.get('kpp',''),d.get('name',''),d.get('full_name',''),code, tools.search_name(d.get('name',''))])
                if len(insertList) > 0:
                    sql = "insert into telecom_companies (ogrn,inn,kpp,name,full_name, okved, search_name) values(?,?,?,?,?,?,?)"
                    con.executemany(sql, insertList)                
                sql = f"update journal set processed = 1 where id = '{file}'"
                con.execute(sql)
                con.commit()
        sql = "select id from journal where processed = 0"
        files = con.execute(sql).fetchall()
        if len(files) == 0:
            sqlEnd = "update status set processed = 1"
            con.execute(sqlEnd)
            con.commit()
    except Exception as err:
        print(err)
        con.rollback()
        sys.exit()
    finally:            
        con.close()
        print(f"Завершено {datetime.now()}")
        logger.info(f"Завершено {datetime.now()}")