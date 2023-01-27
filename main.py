#Install packages here
import sys #Break the system if major error is reached
import os #Shift current to a specific directory (the downloaded files)
import wget #Download the zip files from the web directly
from zipfile import ZipFile #Unzip files
import glob #To find all zip files
import pandas as pd #To create a dataframe(s)
import numpy as np #For numerical analysis

#Set directory to the current folder
path = os.getcwd()
set_path = os.chdir(path)

#Delete any previously downloaded zip files or previous output csv files before the analysis to prevent any file interference
files = os.listdir()
for file in files:
    if file.startswith('PUBLIC_DVD_') or file.startswith('output'):
        os.remove(os.path.join(file))

#Combine all URLS into one list
all_urls = ["https://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/2021/MMSDM_2021_11/MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_DVD_DUDETAILSUMMARY_202111010000.zip",
            "https://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/2021/MMSDM_2021_11/MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_DVD_DISPATCH_UNIT_SCADA_202111010000.zip",
            "https://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/2021/MMSDM_2021_11/MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_DVD_TRADINGPRICE_202111010000.zip"]

#Find any remaining zip files
all_zipfiles = glob.glob("*.zip")

#Collect a full list of key file names based on keywords
file_list = []

#List of files, these will be used to extract the csv files from the zip files much more easily. Obtain the filenames based on all characters after the last '/' symbol.
for i in all_urls:
    last = i.rfind("/")
    file_name = i[last+1:]
    file_list.append(file_name)

#Download and extract the csv files from all zip files. First check that no zip files are remaining in the folder.
for i in range(0,len(file_list)):
    if (file_list[i] not in all_zipfiles) or (not all_zipfiles):
        wget.download(all_urls[i])
        with ZipFile(file_list[i],'r') as zipObj:
            zipObj.extractall(path=None, members=None, pwd=None)

#Import list and description of generators for future use.
duid_list = pd.read_csv('Generators.csv')

#Find any remaining csv files
all_csvfiles = glob.glob("*.csv")

#First check that the relevant files are in the folder. Three crucial ones are needed here.
count = 0
for i in all_csvfiles:
    if ('DUDETAILSUMMARY' in i) or ('SCADA' in i) or ('TRADINGPRICE' in i):
        count += 1
if count < 3:
    sys.exit()

#Loop through each array element
for i in all_csvfiles:
    #Collect the full list of generators and the transmission loss factors (TLF)
    if 'DUDETAILSUMMARY' in i:
        #Read the csv file as a pandas dataframe and remove the top row (unnecessary)
        TLF = pd.read_csv(i,skiprows=1)
        #Delete the last row (unnecessary)
        TLF = TLF.iloc[:-1,:]
        #Keep only current values
        TLF = TLF[TLF['END_DATE'].str.contains("2999")==True].reset_index(drop=True)
        #Keep only values of generators
        TLF = TLF[TLF['DISPATCHTYPE'].str.contains("GENERATOR")==True].reset_index(drop=True)
        #qNow keep only relevant columns for calculating the revenue for each generator
        TLF = TLF[['DUID','REGIONID','TRANSMISSIONLOSSFACTOR']]

    #Collect the generator output
    elif 'SCADA' in i:
        #Read the csv file for power data and remove the top row
        scada = pd.read_csv(i,skiprows=1)
        #Now remove the bottom row
        scada = scada.iloc[:-1,:]
        #Check that all generators are dispatched
        scada_dispatch = scada[scada['DISPATCH'].str.contains("DISPATCH")==True].reset_index(drop=True)
        #Check that all are SCADA units
        scada_units = scada_dispatch[scada_dispatch['UNIT_SCADA'].str.contains("UNIT_SCADA")==True].reset_index(drop=True)
        #Now keep the relevant coloumns for calculating the generator revenue at each timestamp, including SETTLEMENTDATE, DUID and SCADAVALUE
        generator_output = scada_units[['SETTLEMENTDATE','DUID','SCADAVALUE']]
        #Convert SETTLEMENTDATE to an actual date/time for easier time difference calculations as part of converting MW to MWh
        generator_output['SETTLEMENTDATE'] = pd.to_datetime(generator_output['SETTLEMENTDATE'],format='%Y/%m/%d %H:%M:%S')

    #Collect the energy trading price for each region
    elif 'TRADINGPRICE' in i:
        #Read the csv file for pricing data and remove the top row
        trade_price = pd.read_csv(i,skiprows=1)
        #Now remove the bottom row
        trade_price = trade_price.iloc[:-1,:]
        #Keep only the relevant columns for calculating the revenue at each timestamp for each region, including SETTLEMENTDATE, REGIONID and RRP
        trade_price = trade_price[['SETTLEMENTDATE','REGIONID','RRP']]
        #Convert SETTLEMENTDATE to an actual date/time for easier time difference calculations as part of converting MW to MWh (same as before)
        trade_price['SETTLEMENTDATE'] = pd.to_datetime(trade_price['SETTLEMENTDATE'],format='%Y/%m/%d %H:%M:%S')

#Now merge all dataframes
TLF_scada = pd.merge(TLF,generator_output,on=['DUID'],how='inner')
full_data = pd.merge(TLF_scada,trade_price,on=['SETTLEMENTDATE','REGIONID'],how='inner')

#Sort values by the date and time
full_data.sort_values('SETTLEMENTDATE',inplace=True)
#Determine time intervals in minutes for each station power reading, completed more accurately by the sorting beforehand
full_data['TimeInterval'] = full_data.groupby(['REGIONID','DUID'])['SETTLEMENTDATE'].diff()
#Convert this time interval into hours (this will help with calculating MWh)
full_data['TimeInterval'] = full_data['TimeInterval']/np.timedelta64(1,'h')

#Add a new column to calculate the revenue, REVENUE = RRP * SCADAVALUE * TRANSMISSIONLOSSFACTOR (N.B. SCADAVALUE is multiplied by TimeInterval scaled to hours to convert MW to MWh)
#RRP is given in AUD/MWh, SCADAVALUE is transformed to MWh and TRANSMISSIONLOSSFACTOR is dimensionless and thus the Revenue is given in AUD
full_data['Revenue (AUD)'] = full_data['TRANSMISSIONLOSSFACTOR'] * full_data['SCADAVALUE'] * full_data['TimeInterval'] * full_data['RRP']
#Update revenue data to be to 2 decimal places for easier readability
full_data['Revenue (AUD)'] = full_data['Revenue (AUD)'].round(2)

#Now collect the total revenue for each generator alongside relevant independent variables (location and generator). This will rename the column to 'sum', which will be altered shortly.
main = full_data.groupby(['DUID','REGIONID'])['Revenue (AUD)'].agg([np.sum]).reset_index()

#Add the names and fuel type of each generator as well by merging in the DUID list
output = pd.merge(main,duid_list,on=['DUID'],how='inner')
#Simply rearrange the data columns for easier readibility
output = output[['DUID','StationName','REGIONID','FuelSource','sum']]

#Format the data (i.e., renaming). E.g., remove the '1' from each REGIONID element and rename the columns for a more formal interpretation.
output['REGIONID'] = output['REGIONID'].str.rstrip('1')
output.rename(columns={'REGIONID':'State/Territory','StationName':'Station Name','FuelSource':'Fuel','sum':'Total Nov 2021 Revenue (AUD)'},inplace=True)

#Print the dataset
print(output)

#Export the resulting data to csv
output.to_csv('output.csv',index=False)
