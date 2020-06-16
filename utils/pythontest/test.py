import os
from hdbcli import dbapi
#from dotenv import load_dotenv

#load_dotenv()

conn = None

hana_creds = {
    'host'      :  '396487cd-8ec4-4db0-8586-e830e3c58c04.hana.prod-eu10.hanacloud.ondemand.com',
    'port'      : 443,
    'user'      : 'DBADMIN',
    'password'  : 'Didhdemo23!',
    'certificate' : os.getenv('HANA_CERT'),
    'sslHostNameInCertificate' : '*'
}

try:
    conn = dbapi.connect(
        address=hana_creds.get('host'),
        port=int(hana_creds.get('port')),
        user=hana_creds.get('user'),
        password=hana_creds.get('password'),
        sslTrustStore=hana_creds.get('certificate'),
        encrypt='true',
        sslHostNameInCertificate=hana_creds.get('sslHostNameInCertificate')
    )

    cur = conn.cursor()

    schema = 'TEXTANALYSIS'
    table = 'SENTIMENTS'

    pstmt = f'SELECT * FROM "{schema}"."{table}"'

    cur.execute(pstmt)
    data = cur.fetchall()

    cur.close()

    print(data)

    conn.close()
except Exception as e:
    raise
finally:
    if conn:
        conn.close()