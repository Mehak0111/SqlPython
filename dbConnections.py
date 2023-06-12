#tutorial
import psycopg2
import psycopg2.extras

hostname = 'sql-training-db.csuzplcoqc0i.ap-south-1.rds.amazonaws.com'
database = 'Mehak'
username = 'postgres'
pwd = 'Pstraining123$'
conn = None
cur = None

try:
    conn = psycopg2.connect(
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
    )
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute('DROP TABLE IF EXISTS EMP2')
    create_script = '''CREATE TABLE IF NOT EXISTS EMP2 (
                        ID INT PRIMARY KEY,
                        NAME VARCHAR(40) NOT NULL,
                        SALARY INT,
                        DEPT_ID VARCHAR(30)
                    )'''
    cur.execute(create_script)
  
    insert_script = 'INSERT INTO EMP2 (ID, NAME, SALARY, DEPT_ID) VALUES (%s, %s, %s, %s)'
    insert_values = [(1, 'Mehak', 20000, 'D21'), (2, 'Muskan', 15000, 'D22'), (3, 'Om', 30000, 'D23')]
    for record in insert_values:
        cur.execute(insert_script, record)
        
    update_script = 'UPDATE EMP2 SET SALARY = SALARY + (SALARY * 0.5)'
    cur.execute(update_script)
    
    delete_script = 'DELETE FROM EMP2 WHERE NAME = %s'
    delete_record = ('Om',)
    cur.execute(delete_script, delete_record)
    
    cur.execute('SELECT * FROM EMP2')
    records = cur.fetchall()
    for record in records:
        print(record['name'], record['salary'])
    
    conn.commit()

except Exception as error:
    print(error)
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()
