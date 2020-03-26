# coding=utf-8
import pandas as pd


class SearchEntities2:

    def __init__(self, conn):
        self.mydataframe = pd.read_sql("SELECT Entities.address, txhash  FROM Entities inner join Addresses on Entities.address=Addresses.address where entity is null", conn)
        #print (self.mydataframe)
        self._ts = []  # Transazioni esplorate
        self._as = []  # Address già esplorati
        self._t = []  # Transazioni da esplorare trovate da F
        self._a = []  # Address trovati da esplorare
        self._entities = []  # Entità trovate

    def setMydataFrame(self, x):
        self.mydataframe = x

    def setTs(self):
        self._ts = []

    def setAsNull(self):
        self._as = []

    def setAs(self, old_entity):
        T1 = set([x[0] for x in old_entity]) - set(self._as)
        self._as += T1

    def setT(self):
        self._t = []

    def setA(self, x):
        self._a = x

    def getTs(self):
        return self._ts

    def getAs(self):
        return self._as

    def getT(self):
        return self._t

    def getA(self):
        return self._a

    def getMydataFrame(self):
        return self.mydataframe

    def getEntities(self):
        return self._entities

    def findTransactionsInexplored(self):

        if len(self._a) != 0:  # se ci sono altri address
            # prendo un address
            a = self._a[0]
            # cerco tutte le tx in cui è coinvolto
            T1 = list(self.mydataframe.loc[self.mydataframe["address"] == a].txhash)
            # Pulisco T1 dalle tx esplorate
            T2 = list(set(T1) - set(self._ts))
            # le metto in coda a T
            self._t += T2
            # metto a tra quelli esplorati
            self._as.append(a)
            # Tolgo a da A
            self._a = list(set(self._a) - set(self._as))


    def findAddressesInexplored(self):

        if len(self._t) != 0:
            t = self._t[0]
            A1 = list(self.mydataframe.loc[self.mydataframe["txhash"] == t].address)
            A2 = list(set(A1) - set(self._as))
            self._a += A2
            self._ts.append(t)
            self._t = list(set(self._t) - set(self._ts))



    def deleteRawExplored(self, pbar):
        # Cancella le righe delle tx esplorate
        for t in self._ts:
            pbar.update(len(self.mydataframe[(self.mydataframe.txhash == t)]))
            self.mydataframe = self.mydataframe[~(self.mydataframe.txhash == t)]


    def update_table(self, conn, cur, i):
        for j in range(0, len(self.getAs())):
            #print (i)
            #print (self.getAs()[j])
            cur.execute("UPDATE Addresses SET entity = %s WHERE address = '%s'" % (i, self.getAs()[j]))
            conn.commit()

    # aggiorno le nuove entita con quelle già definite in passato:
    # ________________________________________________________________________________
    # se gli addresses che compongono la nuova entità in questione non sono presenti in
    # entita definite in passato, si procede a memorizzare la nuova entita con un id
    # ________________________________________________________________________________
    # se gli addresses che compongono la nuova entità in questione sono presenti in
    # entita definite in passato, si crea una nuova entita con un id nuovo unendo tutti
    # gli indirizzi presenti nelle vecchie entita trovate
    def update_entities(self, conn, cur):
        j = 0
        app = ''
        lunghezza = len(self.getAs())
        while j < lunghezza:

            # si cerca nel database se l'indirizzo è già stato classificato come appartenente ad un'enetita
            cur.execute("SELECT entity FROM Addresses WHERE address = '%s'" % self.getAs()[j])
            conn.commit()
            row = cur.fetchone()
            app = self.getAs()[j]

            # se l'indirizzo non fa parte di un'entita gia definita in passato
            # (in teoria non si fa nulla)
            j = j+1
            # se invece l'indirizzo si trova gia in un altra entita
            # (quindi se l'entity è diverso da none)
            if row[0] is not None:
                # seleziono tutti gli indirizzi aventi stessa entity dell'indirizzo trovato nella nuova entita
                cur.execute("SELECT address FROM Addresses WHERE entity = '%s'" % row[0])
                conn.commit()

                # tutti gli address di un'entita
                old_entity = cur.fetchall()
                # unisco gli address delle entita
                self.setAs(old_entity)
                lunghezza = len(self.getAs())
                if app != self.getAs()[j-1]:
                    j = 0



