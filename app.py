import os
from clickhouse_driver import Client


client = Client(host='rc1a-sgns43124t1g12x8.mdb.yandexcloud.net',
                user='ReadOnly',
                password='n35v!354v785v38n',
                secure=True,
                verify=False,
                database='production',
                ca_certs='YandexInternalRootCA.crt'
                )