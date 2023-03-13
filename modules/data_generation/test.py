import numpy as np
import pandas as pd

def drift_sample_generator(start_time,end_time,max_val,delta):
    '''
    Cette fonction permet de générer une suite de point présentant un drift.
    Le nombre de valeur étant déterminer par les dates de début et de fin
    
    start_time : date initiale
    end_time : date de fin
    max_val : valeur maximale initiale de la courbe. Valeur de reférence pour le drift
    delta : pourcentage de la décroissance
    nb_val : nombre de point

    le programme retourne un dataframe avec en index le temps, au format AAAA-MM-JJ et les valeurs de drift sur cette période

    Créer pour la suite une variante de cette fonction, qui rajoute du bruit sur les données
    drift_sample_generator(max_val,delta,nb_val,var_noise=0.01,noise="no")

    EXAMPLE
    df = drift_sample_generator(start_time = "01/2021",
                               end_time = "03/2021",
                               max_val=0.8,
                               delta=0.2)
    plt.plot(df)

    '''
    # définition de la valeur minimal atteinte sur la période précisée
    min_val = max_val*(1-delta) 

    # Création du dataframe avec les periodes passer en argument
    df = pd.DataFrame({"timestamp":[start_time, end_time],"rendement":[max_val,min_val]})
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp")

    # Echantillonnage au jour
    df = df.resample("D").max()
    nb_val = df.resample("D").max().shape[0]
    
    x = np.linspace(1,100,num=nb_val)
    df["rendement"] = max_val - np.exp(np.sqrt(x))/np.exp(np.sqrt(x[-1]))*(max_val - min_val)
        
    return df