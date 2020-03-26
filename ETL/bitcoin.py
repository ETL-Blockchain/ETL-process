import psycopg2 as psycopg2
import requests
from datetime import datetime
from tqdm import tqdm
from requests.exceptions import ConnectionError


# connessione al database in locale
def estrazione():
    conn = psycopg2.connect("dbname=provaprimo user=postgres password=postgres")
    blockHash = "00000000000000000009d3cfaf648b48591a0d859c53ab0c0670499e3bf92fd1"

    s = requests.Session()
    # ricordiamoci di trovare un altro API piu efficente di questo
    # response = requests.get("https://blockchain.info/rawblock/" + blockHash)
    f = open("blockhash.txt", "r")

    if f.mode == 'r':
        blockHash = f.read()

    response = None
    while response is None:
        try:
            response = s.get("https://blockchain.info/rawblock/" + blockHash)
        except ConnectionError:
            pass

    cur = conn.cursor()

    f = open("blockhash.txt", "r")
    if f.mode == 'r':
        contents = f.read()

    data = response.json()

    height = data["height"]
    height1 = height

    # print (height)
    # print("\n\n")

    # stampiamo tutti i primi 150 blocchi a partire dal piu recente: Data: 2019-03-13 10:57:23, Inoltrato da SlushPool
    while height >= height1 - 150 :

        # scrivo l'hash del blocco
        f = open("blockhash.txt", "w+")
        f.write("%s" % data["hash"])
        f.close()
        # liste di tuple
        transactions = list({})
        tx_single = ()

        address = list({})
        addr_single = ()

        inputsection = list({})
        is_single = ()

        outputsection = list({})
        os_single = ()

        # print ("\n" + str(height) + "\n")
        # Dobbiamo stampare per ogni blocco le transazioni
        for count in tqdm(range(len(data["tx"])), desc="Block " + str(height), ncols=100, bar_format='{l_bar}{bar}|'):
            transaction = data["tx"][count]

            # per ogni transazione dobbiamo visualizzare gli
            # indirizzi in entrata e quelli in uscita

            # vediamo prima gli indirizzi in entrata
            addressInput = transaction["inputs"]
            lunghezza = len(addressInput)
            script = "False"
            app = 1
            while script == 'False' and app <= len(addressInput):
                app = app + 1
                if "script" in addressInput[app - 2]:
                    if addressInput[app - 2]["script"] != "":
                        script = "True"

            date = (datetime.utcfromtimestamp(int(transaction["time"])))

            tx_single = (
            transaction['hash'], date, data["hash"], transaction["relayed_by"], script,'false' ) # transaction["double_spend"]
            transactions.append(tx_single)

            isMiner = 'False'
            # se si entra nel ciclo degli indirizzo in input signfica che ci sono indirizzi in input e quindi non siamo sicuri che gli indirizzi in output siano di miner
            # quindi mettiamo la variabile isMiner come false, in alternativa gli indirizzi di output saranno miner, quindi settiamo la variabile a true
            for count1 in range(0, len(addressInput)):
                if len(addressInput) != 0:

                    if "prev_out" in addressInput[count1]:
                        if "addr" in addressInput[count1]["prev_out"]:
                            isMiner = 'False'
                            # print isMiner
                            # balance = requests.get("https://blockchain.info/q/addressbalance/" + addressInput[count1]["prev_out"]["addr"], headers={'User-agent': 'your bot 0.1'})
                            # while balance.status_code != requests.codes.ok:
                            # print "."
                            # balance = requests.get("https://blockchain.info/q/addressbalance/" + addressInput[count1]["prev_out"]["addr"], headers={'User-agent': 'your bot 0.1'})
                            # balance = requests.get("https://blockchain.info/q/addressbalance/" + addressInput[count1]["prev_out"]["addr"])
                            # data1 = balance.json()
                            adr = addressInput[count1]["prev_out"]["addr"]
                            # print(adr)
                            conflict = 'False'
                            addr_single = (adr, "", 0, isMiner)
                            # se la lista e' vuota allora inserisco l'elemento
                            if len(address) == 0:
                                address.append(addr_single)
                            else:
                                # se la lista ha almeno un elemento li scorro per verificare che non ci siano
                                for index in range(0, len(address)):

                                    if address[index][0] == addr_single[0]:
                                        conflict = 'True'
                                if conflict == 'False':
                                    address.append(addr_single)

                            is_single = (adr, transaction["hash"], addressInput[count1]["prev_out"]["value"])
                            inputsection.append(is_single)

                    else:
                        isMiner = 'True'

                # ora vediamo gli indirizzi in uscita
                ################################
            addressOutput = transaction["out"]
            for count2 in range(0, len(addressOutput)):
                if len(addressOutput) != 0:
                    balance1 = addressOutput[count2]
                    # controlliamo se l'address e' presente negli indirizzi in output
                    # se e' presente lo stampiamo, altrimenti avvisiamo che non e' presente
                    if "addr" in addressOutput[count2]:
                        adr = addressOutput[count2]["addr"]
                        # balance = requests.get("https://blockchain.info/q/addressbalance/" + addressOutput[count2]["addr"])
                        # balance = requests.get("https://blockchain.info/q/addressbalance/" + addressOutput[count2]["addr"], headers={'User-agent': 'your bot 0.1'})
                        # while balance.status_code != requests.codes.ok:
                        # balance = requests.get("https://blockchain.info/q/addressbalance/" + addressOutput[count2]["addr"], headers={'User-agent': 'your bot 0.1'})
                        # data1 = balance.json()
                        conflict = 'False'
                        addr_single = (adr, "", 0, isMiner)
                        if len(address) == 0:
                            address.append(addr_single)
                        else:
                                # se la lista ha almeno un elemento li scorro per verificare che non ci siano
                            for index in range(0, len(address)):
                                # print len(address)
                                # print '  '
                                # print address[index]
                                # print '  '
                                # print addr_single
                                if address[index][0] == addr_single[0]:
                                    conflict = 'True'
                            if conflict == 'False':
                                address.append(addr_single)

                        os_single = (
                        adr, transaction["hash"], addressOutput[count2]["value"], addressOutput[count2]["spent"])
                        outputsection.append(os_single)

        postgres_insert_queryT = " INSERT INTO transactions (txhash, timestamp, blockhash, ip, hascript, unspent ) VALUES (%s,%s ,%s, %s, %s, %s) on conflict (txhash) do nothing;"
        cur.executemany(postgres_insert_queryT, transactions)
        conn.commit()
        # for row in address:
            #if len(row[0]) > 42:
                #print (row[0])
                #print (len(row[0]))
        postgres_insert_queryA = " INSERT INTO addresses (address, tag, balance, isminer) VALUES ( %s ,%s, %s, %s)on conflict (address) do nothing;"
        cur.executemany(postgres_insert_queryA, address)
        conn.commit()

        postgres_insert_queryIS = " INSERT INTO InputSection (address, txhash, amount) VALUES (%s,%s ,%s)on conflict do nothing;"
        cur.executemany(postgres_insert_queryIS, inputsection)
        conn.commit()

        postgres_insert_queryOS = " INSERT INTO OutputSection (address, txhash, amount, isMining) VALUES (%s,%s ,%s,%s) on conflict do nothing;"
        cur.executemany(postgres_insert_queryOS, outputsection)
        conn.commit()

        prev_block = data["prev_block"]
        hash = str(prev_block)
        response = None
        while response is None:
            try:
                response = requests.get("https://blockchain.info/rawblock/" + hash)
            except ConnectionError:
                pass

        data = response.json()
        height = data["height"]  # aggiorniamo height

    postgres_insert_entities = "INSERT INTO Entities(address, txhash) SELECT address, txhash FROM inputsection ON CONFLICT DO NOTHING"
    cur.execute(postgres_insert_entities)
    conn.commit()

    if conn:
        cur.close()
        conn.close()
        print("PostgreSQL connection is closed")
