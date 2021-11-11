#! python3

### This file contains functions related to downloading data from public web sources, minimally transforming that data,
### and saving them as text .csv files


import os, ftplib, pandas as pd, requests, logging, bs4, re, zipfile as zf
from datetime import datetime, timedelta, date
from pandas_datareader import wb

### Get CO2 Concentration Data Collected in Mauna Loa, Hawaii, USA from NOAA
def get_CO2_conc(output_folder):
    logging.info('Downloading CO2 concentration data from NOAA')
    url = 'https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_mm_mlo.csv'
    outfile = os.path.join(output_folder, 'co2_mm_mlo.csv')
    pagey = requests.get(url)
    pagey.raise_for_status()
    open(outfile, 'w+').write(pagey.text)
    logging.info('Saved tidied data to ' + outfile)

### Download HadCRUT data from the UK Met Office
### This url goes to monthly time series data for ensemble medians and uncertainties about global tempeature
### deviation from the 1961-1990 average
def get_global_temp_data(output_folder):
    logging.info('Downloading global temperature data from UK Met')

    ## Step 1: Download data file from UK Met
    url = 'https://www.metoffice.gov.uk/hadobs/hadcrut4/data/current/time_series/HadCRUT.4.6.0.0.monthly_ns_avg.txt'
    raw_file = os.path.join(output_folder, 'HadCRUT.4.6.0.0.monthly_ns_avg.txt')
    pagey = requests.get(url)
    pagey.raise_for_status()
    open(raw_file, 'w+').write(pagey.text)

    ## Step 2: Read in downloaded file as a pandas Data Frame
    HadCRUT = pd.read_csv(raw_file, delimiter='\s+', header=None,
                         names=['DateText', 'Median Global Temp C Deviation',
                                'Bias Uncertainty (Lower 95 CI)', 'Bias Uncertainty (Upper 95 CI)',
                                'Measurement and Sampling Uncertainty (Lower 95 CI)',
                                'Measurement and Sampling Uncertainty (Upper 95 CI)',
                                'Coverage Uncertainty (Lower 95 CI)',
                                'Coverage Uncertainty (Upper 95 CI)',
                                'Measurement, Sampling, and Bias Uncertainty (Lower 95 CI)',
                                'Measurement, Sampling, and Bias Uncertainty (Upper 95 CI)',
                                'Combined Uncertainty (Lower 95 CI)',
                                'Combined Uncertainty (Upper 95 CI)'
                                ])

    a = HadCRUT.loc[:, 'DateText'].apply(lambda x: datetime.strptime(x, '%Y/%m'))
    HadCRUT.insert(0, 'Date', a)
    logging.debug(HadCRUT.head())

    ## Step 3: Save data frame to file
    outfile = os.path.join(output_folder, 'tidied_data_HadCRUT_global_temperature_anomalies.csv')
    HadCRUT.to_csv(outfile, index=False, encoding = 'utf-8')
    logging.info('Saved tidied global temp data to ' + outfile)

### Download summary file of energy production data from the International Energy Agency (IEA)
def get_iea_data(output_folder):
    ## Step 1: Find the correct excel file and download it from the IEA site
    # Get the webpage and scrape it for the correct link to the excel file
    logging.info('Downloading IEA headline energy data')
    title = 'IEA Headline Energy Data - excel file'
    url = 'https://www.iea.org/statistics'
    pagey = requests.get(url)
    pagey.raise_for_status()
    parsey = bs4.BeautifulSoup(pagey.text, 'html5lib')
    elem = ''.join(['a[title="', title, '"]'])
    a = parsey.select(elem)
    # b = parsey.findAll('a', text=link_text)

    # There could be multiple matches. Find the first valid one and download it
    for i in a:
        excel_link1 = i.get('href')
        excel_link2 = ''.join(['https://www.iea.org', i.get('href')])
        try:
            # Try version 1 of link i
            try:
                xl = requests.get(excel_link1)
                xl.raise_for_status()
                excel_link = excel_link1
            # Try version 2 of link i
            except:
                xl = None
                xl = requests.get(excel_link2)
                xl.raise_for_status()
                excel_link = excel_link2

            xlPath = os.path.join(output_folder, os.path.basename(excel_link))

            # Write out file
            xlFile = open(xlPath, 'wb')
            for chunk in xl.iter_content(100000):
                xlFile.write(chunk)
            xlFile.close()

            ## Step 2: Load Excel file into DataFrame
            temp = pd.ExcelFile(xlPath)
            temp2 = list(filter(lambda x: 'TimeSeries' in x, temp.sheet_names))[0]
            iead = temp.parse(temp2, skiprows=1)
            of = xlPath+'.csv'
            iead.loc[:, 'Download Timestamp'] = datetime.now()
            iead.to_csv(of, index=False, encoding = 'utf-8')
            logging.debug('Saved data')

            ## Step 3: Convert from wide to long
            tfn = os.path.join(output_folder,'tidied_data_iea_headline_energy.csv')
            long = pd.melt(iead.reset_index(), id_vars=['index','Country','Product','Flow','NoCountry','NoProduct','NoFlow'
                ,'Download Timestamp'])
            long.to_csv(tfn, index=False, encoding='utf-8')
            return None
        except:
            continue
    raise Exception('Did not download any IEA data!')

### Download a single data set from the World Bank
def get_wb_data(output_folder, indicator_id, indicator, country='WLD'):
    logging.info('Downloading World Bank data')
    logging.debug("Indicator: " + indicator)
    logging.debug("country: " + country)
    dat = wb.download(indicator=indicator_id, country=country, start=1800, end=2100)
    dat.reset_index(inplace=True)
    master = None
    for j in range(0,len(dat),1):
        temp = pd.DataFrame( { 'Indicator ID': [indicator_id],
                               'Indicator' : [indicator],
                               'Country' : dat.loc[j,'country'],
                               'Year' : dat.loc[j,'year'],
                               'Value' : dat.loc[j,indicator_id]
                               }
                             )
        master = temp.append(master, ignore_index=True)
    master.dropna(inplace=True)
    master.reset_index(inplace=True, drop=True)
    f = os.path.join(output_folder,'tidied_data_world_bank_' + indicator_id +'.csv')
    master.to_csv(f,index=False, encoding='utf-8')
    logging.info('Saved tidied World Bank data')

### Download grain data from USDA
def get_grain_data(output_folder
                  ,baseurl='https://apps.fas.usda.gov/psdonline/downloads/'
                  ,filename = 'psd_grains_pulses_csv.zip'):
    logging.info('Getting USDA grain data')
    ag = requests.get(baseurl+filename)
    ag.raise_for_status()

    # Write out file
    agPath = os.path.join(output_folder,filename)
    with open(agPath, 'wb') as agFile:
        for chunk in ag.iter_content(100000):
            agFile.write(chunk)

    # Unzip file
    logging.info('Unzipping ' + agPath)
    with zf.ZipFile(agPath) as myzip:
        myzip.extract(member='psd_grains_pulses.csv',path=output_folder)
    #os.remove(agPath)
    gFile = os.path.join(output_folder, 'psd_grains_pulses.csv')
    gdata = pd.read_csv(gFile)
    logging.info('Removing irrelevant rows, writing back out to ' + gFile)
    gdata = gdata[(gdata['Attribute_Description'] =='Production') | (gdata['Attribute_Description'] =='Total Supply')]
    gdata.to_csv(gFile,index=False, encoding='utf-8')

### Download nClimDiv climate data from NOAA National Climatic Data Center with temperature,
### precipitation, heating degree days, and cooling degree days for the Puget Sound Lowlands
def get_nClimDiv(output_folder, raw_output_folder):
    logging.info('Downloading nClimDiv temperature and precipitation data from NOAA')
    base = 'ftp.ncdc.noaa.gov'
    folder = 'pub/data/cirs/climdiv/'
    ftp = ftplib.FTP(base)
    ftp.login()
    ftp.cwd(folder)
    fList = ftp.nlst()
    mList = ["climdiv-cddcdv", "climdiv-hddcdv", "climdiv-pcpndv", "climdiv-tmpcdv"]
    for thisMet in mList:
        matching = [s for s in fList if thisMet in s]
        sink = matching[0]+'.txt'
        sink = os.path.join(raw_output_folder, sink)
        logging.info('Saving ' + sink)
        ftp.retrbinary('RETR ' + matching[0], open(sink, 'wb').write)
        temp = pd.read_csv(sink, delimiter='\s+', header=None,
                           names=['StateDivisionElementYear', '01', '02'
                                  ,'03','04','05','06','07'
                                  ,'08','09','10','11','12']
                           ,dtype=str)
        temp.loc[:,'STATE-CODE'] = temp.StateDivisionElementYear.str.slice(0,2)
        temp.loc[:,'DIVISION-NUMBER'] = temp.StateDivisionElementYear.str.slice(2,4)
        temp.loc[:,'ELEMENT'] = temp.StateDivisionElementYear.str.slice(4,6)
        temp.loc[:,'YEAR'] = temp.StateDivisionElementYear.str.slice(start=6)
        mTemp = temp.melt(id_vars=['StateDivisionElementYear','STATE-CODE', 'DIVISION-NUMBER'
                                   ,'ELEMENT','YEAR'], var_name='Month')
        outfile = 'tidied_data_n' + thisMet + '.csv'
        outfile = os.path.join(output_folder,outfile)
        logging.info('Saving tidied data ' + outfile)
        mTemp.to_csv(outfile, index=False, encoding='utf-8')
    ftp.quit()

### Download SNOTEL data from USDA about snowpack levels
# Cycle throw SNOTEL station list, download data and write out as tidy
def get_snotel_data(snotel_folder):
    logging.info('Downloading SNOTEL data from USDA')
    snotel_stations = pd.read_csv(os.path.join(snotel_folder, 'WA_SNOTEL_STATION_LIST.csv'))
    for thisSI in snotel_stations.loc[:,'snotel_station_id']:
        logging.debug('Now on SNOTEL station ' + str(thisSI))
        p1='https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/customSingleStationReport/daily/'
        p2=':WA:SNTL|id=%22%22|name/POR_BEGIN,POR_END/WTEQ::value,WTEQ::qcFlag,WTEQ::qaFlag,SNDN::value,SNDN::qcFlag,SNDN::qaFlag?fitToScreen=false'
        url = p1+str(thisSI)+p2
        raw_file = os.path.join(snotel_folder,'snotel_station_id_'+str(thisSI)+'_swe_hx.csv')
        pagey = requests.get(url)
        pagey.raise_for_status()
        logging.info('Writing out ' + raw_file)
        open(raw_file, 'w+').write(pagey.text)

        # Find the last comment line (denoted by #). The line right after is the header
        textfile = open(raw_file, 'r')
        lines = textfile.readlines()
        j = []
        for i in range(0, len(lines), 1):
            if '#' in lines[i]:
                j.append(i)
        j = max(j)
        #print('line j-1: ' + lines[j-1])
        #print('line j: ' + lines[j])
        try:
            logging.debug(lines[j + 1])
        except Exception as e:
            logging.warning('Error during attempted print of header')
            logging.warning('The exception caught:')
            logging.warning(str(e))
            logging.warning(str(e.args))
            logging.warning('Moving on to next station')
            continue

        # Start import using space delimiter
        logging.debug('About to read file back in')
        tidy_snotel = pd.read_csv(raw_file, skiprows=j + 1)
        logging.debug(str(tidy_snotel))
        logging.debug('About to write file back out')
        if len(tidy_snotel.index) == 0:
            logging.debug('length of tidy_snotel is zero, skipping')
            continue
        tidy_snotel.loc[:, 'snotel_station_id'] = thisSI
        tidy_file = os.path.join(snotel_folder, 'tidy_data_snotel_station_id_' + str(thisSI) + '_swe_hx.csv')
        logging.info('Writing out ' + tidy_file)
        tidy_snotel.to_csv(tidy_file,index=False, encoding='utf-8')

    #Cycle through tidy SNOTEL data
    fl = os.listdir(snotel_folder)
    # Set up a regular expression for finding .csv files with the right station_id
    regex = re.compile(r'^tidy_data_snotel_station_id_(.*)[.]csv$')
    fl = [m.group(0) for l in fl for m in [regex.match(l)] if m]
    thisT = fl[0]
    masterT = pd.read_csv(os.path.join(snotel_folder,thisT))
    for thisT in fl:
        tempT = pd.read_csv(os.path.join(snotel_folder,thisT))
        masterT = masterT.append(tempT)
    masterT.drop_duplicates(inplace=True)
    tidy_master_file = os.path.join(snotel_folder, 'tidy_data_master_snotel_swe_hx.csv')
    logging.info('Writing out ' + tidy_master_file)
    masterT.to_csv(tidy_master_file,index=False, encoding='utf-8')

### Download tidal gauge data from NOAA with sea level information
def get_tidal_data(tidal_folder):
    logging.info('Downloading tidal gauge data from NOAA')
    tidal_stations = pd.read_csv(os.path.join(tidal_folder, 'tidal_stations.csv'))
    for thisTS in tidal_stations.loc[:, 'NWLON Station ID']:
        logging.debug('Now on NWLON station ' + str(thisTS))
        tehdate = date.today()
        tehdate = tehdate.replace(day=1)
        tehdate = tehdate - timedelta(days=1)
        last_month_txt = tehdate.strftime("%Y%m%d")
        p1_STND = 'https://tidesandcurrents.noaa.gov/api/datagetter?product=monthly_mean&application=NOS.COOPS.TAC.WL&begin_date=19500101&end_date=' + last_month_txt + '&time_zone=GMT&units=metric&format=csv&datum=STND&format=csv&station='
        p1_NAVD = 'https://tidesandcurrents.noaa.gov/api/datagetter?product=monthly_mean&application=NOS.COOPS.TAC.WL&begin_date=19500101&end_date=' + last_month_txt + '&time_zone=GMT&units=metric&format=csv&datum=NAVD&format=csv&station='
    
        
        try:
            url_NAVD = p1_NAVD + str(thisTS)
            raw_file_NAVD = os.path.join(tidal_folder, 'nwlon_station_id_' + str(thisTS) + '_datum_NAVD.csv')
            pagey_NAVD = requests.get(url_NAVD)
            pagey_NAVD.raise_for_status()
            logging.info('Writing out ' + raw_file_NAVD)
            open(raw_file_NAVD, 'w+').write(pagey_NAVD.text)
        
            logging.debug('About to read NAVD file back in')
            tidy_nwlon_navd = pd.read_csv(raw_file_NAVD)
            tidy_nwlon_navd.columns = tidy_nwlon_navd.columns.str.strip()
            tidy_nwlon_navd.loc[:, 'Datum'] = 'NAVD'
            tidy_nwlon_navd.loc[:, 'nwlon_station_id'] = thisTS
            tidy_file_navd = os.path.join(tidal_folder,
                                          'tidied_data_nwlon_station_id_' + str(thisTS) + '_datum_NAVD.csv')
            tidy_nwlon_navd.to_csv(tidy_file_navd, index=False, encoding='utf-8')
        except Exception as e:
            logging.warning('Error thrown')
            logging.warning('The exception caught:')
            logging.warning(str(e))
            logging.info(str(e.args))
            logging.warning('Moving on to STND file')
    
        try:
            url_STND = p1_STND + str(thisTS)
            raw_file_STND = os.path.join(tidal_folder, 'nwlon_station_id_' + str(thisTS) + '_datum_STND.csv')
            pagey_STND = requests.get(url_STND)
            pagey_STND.raise_for_status()
            logging.info('Writing out ' + raw_file_STND)
            open(raw_file_STND, 'w+').write(pagey_STND.text)
            logging.debug('About to read STND file back in')
            tidy_nwlon_stnd = pd.read_csv(raw_file_STND)
            tidy_nwlon_stnd.columns = tidy_nwlon_stnd.columns.str.strip()
            tidy_nwlon_stnd.loc[:, 'Datum'] = 'STND'
            tidy_nwlon_stnd.loc[:, 'nwlon_station_id'] = thisTS
            tidy_file_stnd = os.path.join(tidal_folder,
                                          'tidied_data_nwlon_station_id_' + str(thisTS) + '_datum_STND.csv')
            tidy_nwlon_stnd.to_csv(tidy_file_stnd, index=False, encoding='utf-8')
        except Exception as e:
            logging.warning('Error thrown')
            logging.warning('The exception caught:')
            logging.warning(str(e))
            logging.info(str(e.args))
            logging.warning('Moving on to next tidal gauge station')
    
    # Cycle through tidy tidal data
    fl = os.listdir(tidal_folder)
    
    # Set up a regular expression for finding .csv files with the right station_id
    regex = re.compile(r'^tidied_data_nwlon_station_id_(.*)[.]csv$')
    fl = [m.group(0) for l in fl for m in [regex.match(l)] if m]
    thisT = fl[0]
    masterT = pd.read_csv(os.path.join(tidal_folder, thisT))
    for thisT in fl:
        tempT = pd.read_csv(os.path.join(tidal_folder, thisT))
        masterT = masterT.append(tempT)
    masterT.drop_duplicates(inplace=True)
    tidy_master_file = os.path.join(tidal_folder, 'tidy_data_master_tidal_data.csv')
    logging.info('Writing out ' + tidy_master_file)
    masterT.to_csv(tidy_master_file, index=False, encoding='utf-8')

### Download USGS streamflow data
def get_usgs_streamflow(sf_folder):
    logging.info('Downloading streamflow data from USGS')
    sf_stations = pd.read_csv(os.path.join(sf_folder, 'streamflow_stations.csv'))
    for i in range(len(sf_stations.index)):
        thisSF = str(sf_stations.loc[i, 'USGS Site Number'])
        logging.info('Now on USGS Site Number ' + thisSF)
        url = sf_stations.loc[i, 'URL']
        raw_file = os.path.join(sf_folder, 'usgs_streamflow_site_no_' + thisSF + '.txt')
        pagey = requests.get(url)
        pagey.raise_for_status()
        logging.info('Writing out ' + raw_file)
        open(raw_file, 'w+').write(pagey.text)

        # Find the last comment line (denoted by #). The line right after is the header
        textfile = open(raw_file, 'r')
        lines = textfile.readlines()
        j = []
        for i in range(0, len(lines), 1):
            if '#' in lines[i]:
                j.append(i)
        j = max(j)

        try:
            logging.debug(lines[j + 1])
        except Exception as e:
            logging.warning('Error during attempted print of header')
            logging.warning('The exception caught:')
            logging.warning(str(e))
            logging.warning(str(e.args))
            logging.warning('Moving on to next station')
            continue

        # Start import using space delimiter
        logging.debug('About to read file back in')
        tidy_sf = pd.read_csv(raw_file, skiprows=j + 1, delimiter='\t')
        tidy_sf = tidy_sf.iloc[1:]
        logging.debug(str(tidy_sf))
        logging.debug('About to write file back out')
        if len(tidy_sf.index) == 0:
            logging.debug('length of tidy_sf is zero, skipping')
            continue
        tidy_file = os.path.join(sf_folder, 'tidy_data_usgs_streamflow_site_id_' + thisSF + '.csv')
        logging.info('Writing out ' + tidy_file)
        tidy_sf.to_csv(tidy_file, index=False, encoding='utf-8')

    # Cycle through tidy SNOTEL data
    fl = os.listdir(sf_folder)
    # Set up a regular expression for finding .csv files with the right station_id
    regex = re.compile(r'^tidy_data_usgs_streamflow_site_id_(.*)[.]csv$')
    fl = [m.group(0) for l in fl for m in [regex.match(l)] if m]
    thisT = fl[0]
    masterT = pd.read_csv(os.path.join(sf_folder, thisT))
    for thisT in fl:
        tempT = pd.read_csv(os.path.join(sf_folder, thisT))
        masterT = masterT.append(tempT)
    masterT = masterT.loc[:, ['site_no', 'year_nu', 'month_nu', 'mean_va']]
    masterT.drop_duplicates(inplace=True)
    tidy_master_file = os.path.join(sf_folder, 'tidy_data_master_usgs_streamflow.csv')
    logging.info('Writing out ' + tidy_master_file)
    masterT.to_csv(tidy_master_file, index=False, encoding='utf-8')
