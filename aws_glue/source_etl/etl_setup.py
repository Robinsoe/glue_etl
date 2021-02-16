# glue_etl setup

def endpoints():
    return {
        "EKCREDOP": {
            "engine_name": "oracle",
            "server_name": "10.15.105.15",
            "port": 1526,
            "uid": 'XXCRC_BDA_AWS',
            "pwd": 'XXCRC_BDA_AWS'
        },
        "EKPSPP": {
            "engine_name": "oracle",
            "server_name": "10.15.105.17",
            "port": 1526,
            "uid": 'XXCRC_BDA_AWS',
            "pwd": 'XXCRC_BDA_AWS'
        },
        "HOMAX2P": {
            "engine_name": "oracle",
            "server_name": "10.15.105.15",
            "port": 1527,
            "uid": 'XXCRC_BDA_AWS',
            "pwd": 'XXCRC_BDA_AWS'
        },
        "LGMAX1P": {
            "engine_name": "oracle",
            "server_name": "10.17.105.10",
            "port": 1525,
            "uid": 'XXCRC_BDA_AWS',
            "pwd": 'XXCRC_BDA_AWS'
        },
        "LGPSPP": {
            "engine_name": "oracle",
            "server_name": "10.17.105.10",
            "port": 1526,
            "uid": 'XXCRC_BDA_AWS',
            "pwd": 'XXCRC_BDA_AWS'
        },
        "OW5KHOIL": {
            "engine_name": "oracle",
            "server_name": "10.15.105.47",
            "port": 1525,
            "uid": 'XXCRC_BDA_AWS',
            "pwd": 'XXCRC_BDA_AWS'
        },
        "OWR5KELK": {
            "engine_name": "oracle",
            "server_name": "10.15.105.46",
            "port": 1525,
            "uid": 'XXCRC_BDA_AWS',
            "pwd": 'XXCRC_BDA_AWS'
        },
        "OWR5KLG": {
            "engine_name": "oracle",
            "server_name": "10.17.42.48",
            "port": 1550,
            "uid": 'XXCRC_BDA_AWS',
            "pwd": 'XXCRC_BDA_AWS'
        },
        "OWR5KVPC": {
            "engine_name": "oracle",
            "server_name": "10.15.105.48",
            "port": 1526,
            "uid": 'XXCRC_BDA_AWS',
            "pwd": 'XXCRC_BDA_AWS'
        },
        "USOXYBIP": {
            "engine_name": "oracle",
            "server_name": "10.15.105.18",
            "port": 1528,
            "uid": 'XXCRC_BDA_AWS',
            "pwd": 'XXCRC_BDA_AWS'
        },
        "USOXYP": {
            "engine_name": "oracle",
            "server_name": "10.15.105.15",
            "port": 1525,
            "uid": 'XXCRC_BDA_AWS',
            "pwd": 'XXCRC_BDA_AWS'
        },
        "TQPROD": {
            "engine_name": "oracle",
            "server_name": "10.17.105.10",
            "port": 1541,
            "uid": 'XXCRC_BDA_AWS',
            "pwd": 'XXCRC_BDA_AWS'
        },
        "ONECALP": {
            "engine_name": "oracle",
            "server_name": "10.15.107.15",
            "port": 1531,
            "uid": 'XXCRC_BDA_AWS',
            "pwd": 'XXCRC_BDA_AWS'
        },
       "CKCWSQLB": {
            "engine_name": "sqlserver",
            "server_name": "10.15.42.36",
            "port": 1433,
            "uid": 'XXCRC_BDA_AWS',
            "pwd": 'XXCRC_BDA_AWS'
        },
        "CKCWSQLF": {
            "engine_name": "sqlserver",
            "server_name": "10.15.41.35",
            "port": 1433,
            "uid": 'XXCRC_BDA_AWS',
            "pwd": 'XXCRC_BDA_AWS'
        },
        "CLGWSQL7": {
            "engine_name": "sqlserver",
            "server_name": "10.17.42.34",
            "port": 1433,
            "uid": 'XXCRC_BDA_AWS',
            "pwd": 'XXCRC_BDA_AWS'
        },
        "CKCWSQL11": {
            "engine_name": "sqlserver",
            "server_name": "10.15.41.247",
            "port": 1433,
            "uid": 'XXCRC_BDA_AWS',
            "pwd": 'XXCRC_BDA_AWS'
        },
        "CLGWSQL5-D": {
            "engine_name": "sqlserver",
            "server_name": "10.17.42.46",
            "port": 1433,
            "uid": 'XXCRC_BDA_AWS',
            "pwd": 'XXCRC_BDA_AWS'
        },
        "OKCWSQL1": {
            "engine_name": "sqlserver",
            "server_name": "10.15.103.70",
            "port": 1433,
            "uid": 'XXCRC_BDA_AWS',
            "pwd": 'XXCRC_BDA_AWS'
        },
        "ccslpsq01": {
            "engine_name": "postgres",
            "server_name": "ccslpsq01.calnet.ads",
            "database": "",
            "port": 5432,
            "uid": 'XXCRC_BDA_AWS',
            "pwd": 'XXCRC_BDA_AWS'
        },
        "aurora": {
            "engine_name": "postgres",
            "server_name": "bda-aurora-postgresql-cluster.cluster-c2it5k2mwyhf.us-west-2.rds.amazonaws.com",
            "database": "bda-aurora",
            "port": 5432,
            "uid": 'postgres',
            "pwd": 'Den0d0BaseView4'
        },
    }


def get_con_list():
    con_list = {}
    for key in endpoints():
        epnt = endpoints()[key]
        if epnt['engine_name'] == 'oracle':
            con = [f'jdbc:oracle:thin://@{epnt["server_name"]}:{epnt["port"]}/{key}', epnt["uid"], epnt["pwd"], epnt["engine_name"]]
        elif epnt['engine_name'] == 'sqlserver':
            con = [f'jdbc:sqlserver://{epnt["server_name"]}\\{key}:{epnt["port"]};database=;', epnt["uid"], epnt["pwd"], epnt["engine_name"]]
        elif epnt['engine_name'] == 'postgres':
            con = [f'jdbc:postgresql://{epnt["server_name"]}:{epnt["port"]}/{epnt["database"]}', epnt["uid"], epnt["pwd"], epnt["engine_name"]]
        else:
            con = ['']
        con_list[key.lower()] = con
    return con_list

# con_list2 = {'ckcwsqlb': ['jdbc:sqlserver://10.15.42.36\\CKCWSQLB:1433;database=;', 'spocread', 'spocread'],
#             'ekcredop': ['jdbc:oracle:thin://@10.15.105.15:1526/EKCREDOP', '', ''],
#             'ekpspp': ['jdbc:oracle:thin://@10.15.105.17:1526/EKPSPP', 'oxy_read', 'oxy_read'],
#             }