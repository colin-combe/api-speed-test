import psycopg2
from flask import Flask
import orjson
from time import time
from psycopg2.extras import RealDictCursor

app = Flask(__name__)


@app.route('/')
def test_pdb_dev():  # put application's code here
    conn = None
    data = {"test": "test"}
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(
            host="localhost",
            database="xitest",
            user="",
            password="",
            port="5432"
        )

        # create a cursor
        cur = conn.cursor(cursor_factory=RealDictCursor)

        sql = """SELECT si.id, u.identification_file_name as file, si.pass_threshold as pass,
pe1.dbsequence_ref as prot1, dbs1.accession as prot1_acc, (pe1.pep_start + mp1.link_site1 - 1) as pos1,
pe2.dbsequence_ref as prot2, dbs2.accession as prot2_acc, (pe2.pep_start + mp2.link_site1 - 1) as pos2
FROM spectrumidentification si INNER JOIN
modifiedpeptide mp1 ON si.pep1_id = mp1.id AND si.upload_id = mp1.upload_id INNER JOIN
peptideevidence pe1 ON mp1.id = pe1.peptide_ref AND mp1.upload_id = pe1.upload_id INNER JOIN
dbsequence dbs1 ON pe1.dbsequence_ref = dbs1.id AND pe1.upload_id = dbs1.upload_id INNER JOIN
modifiedpeptide mp2 ON si.pep2_id = mp2.id AND si.upload_id = mp2.upload_id INNER JOIN
peptideevidence pe2 ON mp2.id = pe2.peptide_ref AND mp2.upload_id = pe2.upload_id INNER JOIN
dbsequence dbs2 ON pe2.dbsequence_ref = dbs2.id AND pe2.upload_id = dbs2.upload_id INNER JOIN
upload u on u.id = si.upload_id
where u.id = ANY (array[12, 13, 14, 15]) and mp1.link_site1 > 0 and mp2.link_site1 > 0 AND pe1.is_decoy = false AND pe2.is_decoy = false
;"""

        before = time()
        cur.execute(sql)
        after = time()
        print("SQl execute time: ", after - before)

        before = time()
        data = cur.fetchall()
        after = time()
        print("Fetchall time: ", after - before)

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
        before = time()
        json_data = orjson.dumps(data)
        after = time()
        print("JSON dump time: ", after - before)
        return json_data


if __name__ == '__main__':
    app.run()
