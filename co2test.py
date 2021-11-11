# -*- coding: utf-8 -*-
"""
Created on Wed Nov 10 15:32:32 2021

@author: kmars
"""
import os, ftplib, pandas as pd, requests, logging, bs4, re, zipfile as zf

output_folder = os.getcwd()
url = 'https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_mm_mlo.csv'
raw_file = os.path.join(output_folder, 'co2_mm_mlo.csv')
pagey = requests.get(url)
pagey.raise_for_status()
open(raw_file, 'w+').write(pagey.text)