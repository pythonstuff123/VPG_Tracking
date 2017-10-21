import numpy as np
import pandas as pd
import glob
from pandas.tseries.offsets import BusinessHour

primerTurno = ["foceguex","ilariosx","ebarbaax","eoliva2x","jvivancx","mdiazcax","mroble2x","ogalicix","sloredox","dcoronax","abraha5x","hiramrox","jruizdox","davidivx","jlopez7x"]
coreFirst = ["foceguex","ilariosx"]
displayFirst = ["ebarbaax","eoliva2x","jvivancx","mdiazcax","mroble2x","ogalicix","sloredox","dcoronax","abraha5x","hiramrox","davidivx","gperezpx","jlopez7x"]
mediaFirst = ["jruizdox"]
statusValido = ["Passed","Failed","Warned"]
#primerTurno = ["emosquex", "ogalicix","sloredox","mroble2x","nvelalax","jochoaax","julioc6x","scasillx","jmoren5x","jvivancx","nerenipx","jflore7x","jcuautlx","jolmedox","jruizdox","pabloanx","grodri8x","llopezzx"]
#displayFirst = ["emosquex", "ogalicix","sloredox","mroble2x","nvelalax","jochoaax","julioc6x","scasillx","jmoren5x","jvivancx"]
#coreFirst = ["jflore7x","jcuautlx","nerenipx"]
#mediaFirst = ["jolmedox","jruizdox","pabloanx","grodri8x","llopezzx"]

#excel_names = ["C:/Users/aanguiax/Documents/dev/vpg_plan/data/daily/gft.xlsx", "C:/Users/aanguiax/Documents/dev/vpg_plan/data/daily/ci.xlsx","C:/Users/aanguiax/Documents/dev/vpg_plan/data/daily/gst.xlsx"]
excel_names = glob.glob("C:/Users/aanguiax/Documents/dev/vpg_plan/data/plan/*.xlsx")
print(excel_names) 
excels = [pd.ExcelFile(name) for name in excel_names]
#parseo
frames = [x.parse(x.sheet_names[0], header=0,index_col=None) for x in excels]
#drop reviewed column
for df in frames:
    try:
        df.drop('Reviewed',1)
    except ValueError:
        continue

#drop first row del primer archivo
frames[1:] = [df[1:] for df in frames[1:]]
combined = pd.concat(frames)
combined = combined.set_index('Status', drop=False) #Crea indice tester
combined = combined.loc["No Run"] #Reduce dataframe a status valido
combined = combined.drop_duplicates("InstanceId")  #Quita las columnas duplicadas
combined.to_csv("test.csv")
combined["UWD"] = combined["UWD"].apply(lambda x: x if not pd.isnull(x) else "No")
platforms = combined.groupby(["Platform","UWD","OperatingSystem"])["ExpectedDuration"].agg(['count','sum'])
platforms["sum"] = platforms["count"].apply(lambda x: x if x = 1)
print(platforms)
print(platforms.dtypes)



