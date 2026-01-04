# -*- coding: utf-8 -*-
"""
Created on Fri Dec  5 15:30:39 2025

@author: rfaucher
"""
import pymongo as mg
import pandas as pd
connex = mg.MongoClient("mongodb://127.0.0.1:27017/")
db = connex.Paris2055



#%%a

result_a=pd.DataFrame(db.Trafic.aggregate([{"$group": {"_id": "$id_ligne", "retard_moyen": {"$avg": "$retard_minutes"}}}
    ]))
print(result_a)

#%%b pas fini
result_b=pd.DataFrame(db.Trafic.aggregate([{"$group": {"_id": "$id_ligne", "retard_moyen": {"$avg": "$passagers_estimes"}}}]))

#%%C
result_c=pd.DataFrame(db.Trafic.aggregate([{"$group": {"_id": "$id_ligne", "nb_incident": {"$avg": "$retard_minutes"}}}
    ]))
print(result_c)
