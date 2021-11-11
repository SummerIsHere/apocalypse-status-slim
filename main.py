#! python3

### Main function to run the entire chain of downloading and extracting data


import logging, os, download_tidy_up as dtu, sys#, pandas as pd

## Set up paths
data_folder = os.path.join(os.getcwd(), 'downloaded_data')
snotel_folder = os.path.join(data_folder, 'snotel')
sf_folder = os.path.join(data_folder, 'streamflow')
tidal_folder = os.path.join(data_folder, 'tide_gauge')
nClim_output_folder = os.path.join(data_folder,'nclimdiv')
nClim_raw_output_folder = os.path.join(nClim_output_folder,'raw_data')

## Set up logging file
logging.basicConfig(level=logging.INFO
                    , filename='main_logging.txt'
                    , format=' %(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.info('Start main.py')

## Set up standard out file
sdpath = os.path.join('main_stdout.txt')
sys.stdout = open(sdpath, 'w')
sys.stderr = open(sdpath, 'a')

## Set up a second handler for output to stdout
root = logging.getLogger()
root.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
root.addHandler(ch)

# logging.info('Before exception')
# raise Exception ('Deliberate exception')

## Download and minimally tidy up data
logging.info('Getting CO2 Data')
dtu.get_CO2_conc(output_folder=data_folder)
logging.info('Getting NOAA US climate data')
dtu.get_nClimDiv(output_folder=nClim_output_folder,raw_output_folder=nClim_raw_output_folder)
#logging.info('Getting Snotel Data')
#dtu.get_snotel_data(snotel_folder)
#logging.info('Getting tidal gauge data')
#dtu.get_tidal_data(tidal_folder)
logging.info('Getting streamflow data')
dtu.get_usgs_streamflow(sf_folder)
logging.info('Getting global temperature data')
dtu.get_global_temp_data(output_folder=data_folder)
## World Bank Data is two years out of date, disable update and update file manually using worldometer.info
#logging.info('Getting population data from World Bank')
#dtu.get_wb_data(output_folder=data_folder, indicator_id='SP.POP.TOTL', indicator='Population, total', country='WLD')
logging.info('Getting grain data from USDA')
dtu.get_grain_data(output_folder=data_folder
                  ,baseurl='https://apps.fas.usda.gov/psdonline/downloads/'
                  ,filename = 'psd_grains_pulses_csv.zip')

logging.info('End main.py')