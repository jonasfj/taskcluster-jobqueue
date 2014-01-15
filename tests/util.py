
import os
import socket
import sqlite3
import tempfile

# Gets an open port starting with the seed by incrementing by 1 each time
def find_open_port(ip, seed):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        connected = False
        if isinstance(seed, basestring):
            seed = int(seed)
        maxportnum = seed + 5000 # We will try at most 5000 ports to find an open one
        while not connected:
            try:
                s.bind((ip, seed))
                connected = True
                s.close()
                break
            except:
                if seed > maxportnum:
                    print('Error: Could not find open port after checking 5000 ports')
                    raise
            seed += 1
    except:
        print('Error: Socket error trying to find open port')

    return seed

def make_temporary_database():
    db = tempfile.NamedTemporaryFile()

    # try to find db schema in some default locations
    paths = ['schema.sql', 'sql/schema.sql', '../sql/schema.sql']
    for path in paths:
        if os.path.isfile(path):
            with open(path) as f:
                conn = sqlite3.connect(db.name)
                conn.executescript(f.read())

    return db
