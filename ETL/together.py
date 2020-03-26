import main2
from bitcoin import estrazione

# (hash del blocco iniziale, per far partire l'analisi da capo copiarlo in blockhash.txt)
#0000000000000000002fc1ceddf6467e991acfd1dd045b7f248dd4d1ab5552c0

# while la blockchain ha ancora blocchi da registrare
f = open("blockhash.txt", "r")
if f.mode == 'r':
    contents = f.read()
while 1:
    estrazione()

    main2.classificazione()
