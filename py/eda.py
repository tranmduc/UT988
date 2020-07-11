# -*- coding: utf-8 -*-
"""
Created on Thu May 28 16:00:22 2020

@author: Minh Duc
"""

import pandas as pd

df = pd.read_csv('raw_04.csv')

print(df.describe())