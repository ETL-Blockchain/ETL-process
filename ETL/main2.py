# coding=utf-8
from tqdm import tqdm
from SearchEntities2 import SearchEntities2
import psycopg2

def classificazione():
    conn = psycopg2.connect("dbname=provaprimo user=postgres password=postgres")
    cur = conn.cursor()

    # ___________________________________________________

    # "Main" algoritmo.
    se = SearchEntities2(conn)

    startLen = len(se.getMydataFrame())
    pbar = tqdm(total=startLen)

    cur.execute("SELECT max(entity) FROM public.addresses")
    conn.commit()
    index = cur.fetchone()
    print (index[0])
    i=0
    # indice per l'aggiornamento (numero id dell'entita)
    if index[0] is None:
        i = 1
    else:
        i = index[0]+1
    print (len(se.getMydataFrame()))
    while len(se.getMydataFrame()) != 0:
        se.setAsNull()
        se.setT()
        se.setTs()
        se.setA([se.mydataframe.loc[se.mydataframe.index[0]].address])
        # fino a quando ci sono address inesplorati o transazioni inesplorate
        while len(se.getA()) != 0 or len(se.getT()) != 0:
            se.findTransactionsInexplored()
            se.findAddressesInexplored()
        se.deleteRawExplored(pbar)  # Cancella le righe delle tx esplorate
        se.update_entities(conn, cur)
        se.update_table(conn, cur, i)

        i = i + 1

    pbar.close()

    # VERIFICA


    if conn:
        cur.close()
        conn.close()
        print("PostgreSQL connection is closed")
