import numpy as np
import pandas as pd
import glob
from pandas.tseries.offsets import BusinessHour

primerTurno = ["ammaciax","cromonex","aiarcetx","barran1x","ediazdex","fsanch5x","gaguitix","gperezpx","hpinedox","icuevaax","jorgeanx","jrmunozx","nruvalcx","vyanezgx","rmosqued","jmedinax","llopezzx","perezsmx","anamar2x"]
coreFirst = ["cromonex","ammaciax"]
displayFirst = ["aiarcetx","barran1x","ediazdex","fsanch5x","gaguitix","gperezpx","hpinedox","icuevaax","jorgeanx","jrmunozx","nruvalcx","vyanezgx","rmosqued"]
mediaFirst = ["jmedinax","llopezzx","perezsmx","anamar2x"]
statusValido = ["Passed","Failed","Warned"]

#excel_names = ["C:/Users/aanguiax/Documents/dev/vpg_plan/data/daily/gft.xlsx", "C:/Users/aanguiax/Documents/dev/vpg_plan/data/daily/ci.xlsx","C:/Users/aanguiax/Documents/dev/vpg_plan/data/daily/gst.xlsx"]
excel_names = glob.glob("C:/Users/aanguiax/Documents/dev/vpg_plan/data/daily_data/*.xlsx")
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
#Quita las instancias duplicadas de la combinacion de archivos excel
combined = combined.set_index('Status', drop=False) #Crea indice tester
combined = combined.loc[statusValido] #Reduce dataframe a status valido
combined = combined.drop_duplicates("InstanceId")  #Quita las columnas duplicadas
combined.to_csv("Parsed_Data_Total.csv")


pd.set_option('display.width', 5000) 
pd.set_option('display.max_columns', 60)
#xls = pd.ExcelFile("C:/Users/aanguiax/Documents/dev/vpg_plan/data/fullexe_ww23-ww30gft.xlsx")
#df = xls.parse("Sheet1")
combined = combined.set_index('Tester', drop=False) #se crea el indice como tester
combined['ExecutedOn'] = pd.to_datetime(combined["ExecutedOn"])
firstShift = combined.loc[primerTurno]

####descomentar #########################################
firstShift = firstShift.set_index("ExecutedOn")
###	TESTING WW41

#####FUNCION of COURSE
def mono(firstShift,combined,displayFirst,coreFirst,mediaFirst):
    result = pd.DataFrame()
	
	
    #count = 0
    for i in firstShift:
        try:
            mono = combined.loc[[i]]
        except KeyError:
            continue
  
        mono = mono.set_index(["ExecutedOn"], drop=False)
        #mono.reindex("ExecutedOn")

        #horas = mono.resample(rule="D", closed="right", base=24).sum() #funcionall resample JUST WERKS
        horas = mono.resample(rule="D", closed="left", base=24).agg({'ExpectedDuration':np.sum,'ExecutionTime':np.sum,'SetupTime':np.sum,'ReproductionTime':np.sum,'SightingTime':np.sum,'HWDebugTime':np.sum,'Platform':'nunique'})
	    
        horas["Manual"] = (horas["ExecutionTime"]+horas["SetupTime"]+horas["ReproductionTime"]+horas["SightingTime"]+horas["HWDebugTime"])/60
        horas["Tester"] = i
        #horas["PlataformaUnica"] = mono["Platform"].nunique()
		
		#mascara para operacion numpy mayor a 2
		#mascara = (horas["Plaform" > 2])
		
		#horas["Tiempo+Setup"]= horas.apply(lambda row: 0 if row )
		
		#valor_valido = horas[mascara]
		#horas["Platform"]
        horas["Tiempo+Setup"]= (horas["Platform"]*.75)
        if i in displayFirst:
            horas["Component"] = "Man Exe Display OS" or "Man Exe Display" or "Man Exe Display OS KELLY"
            horas["Turno"] = "First Shift"
        elif i in coreFirst:
            horas["Component"] = "Man Exe Core OS" or "Man Exe Core" or "Man Exe Core OS KELLY"
            horas["Turno"] = "First Shift"
        elif i in mediaFirst:
            horas["Component"] = "Man Exe Media OS" or "Man Exe Media" or "Man Exe Media OS KELLY"
            horas["Turno"] = "First Shift"
        #print(i) 
        result = result.append(horas)
        #result = result.append(plataforma)
    result.to_csv("TPT_Diario.csv")
    melate_pivote(result)
    semanal(result)
    print(result.dtypes)
    
			

def melate_pivote(result):
    #result = result[["Component","Tester","ExpectedDuration",  "Manual","ExecutionTime","HWDebugTime","ReproductionTime","SetupTime","SightingTime"]]
    pivote = result.pivot_table(result, index=["ExecutedOn","Component","Tester"])
    #print(pivote)
    pivote = pivote[["ExpectedDuration",  "Manual","ExecutionTime","HWDebugTime","ReproductionTime","SetupTime","SightingTime","Platform","Tiempo+Setup"]]
    pivote.to_csv("TPT_Diario_Pivote.csv")
    #semanal(pivote)
	
def semanal(result):
    #print(pivote)
    total = result.groupby("Tester").resample(rule="W", closed="left").agg({'ExpectedDuration':np.mean,'ExecutionTime':np.mean,'SetupTime':np.mean,'ReproductionTime':np.mean,'SightingTime':np.mean,'HWDebugTime':np.mean,'Platform':sum})
    print(result.index)
    print(total)
    total.to_csv("TPT_Semana.csv")
	


mono(primerTurno,combined,displayFirst,coreFirst,mediaFirst)
