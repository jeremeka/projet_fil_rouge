#!/usr/bin/env python
# coding: utf-8

# In[15]:


# imports
import pandas as pd
import matplotlib.pyplot as plt
import datetime
import numpy as np
from sktime import datasets
from sktime.forecasting import model_selection
from sktime.utils.plotting import plot_series
#!pip install sktime

class EnergyDataset:
    
    def __init__(self, dataset_path):
        
        # Load dataset from path
        self.initial_dataset = pd.read_csv(dataset_path)
        self.formatted_dataset = self.initial_dataset
        
    def get_data_by_equipement_id(self, equipment_id, formatted_dataset=True):
        
        # filter dataset by equipment id
        if formatted_dataset:
            dataset = self.formatted_dataset[self.formatted_dataset['equipment_id']== equipment_id]
        else:
            dataset = self.initial_dataset[self.initial_dataset['equipment_id']== equipment_id]
            
        return dataset
    
    def format_energy_to_numeric(self):
        # apply format to dataset
        self.formatted_dataset['energy_input_in_mwh'] = self.formatted_dataset['energy_input_in_mwh'].str.replace(',','.')
        self.formatted_dataset['energy_output_in_mwh'] = self.formatted_dataset['energy_output_in_mwh'].str.replace(',','.')
        self.formatted_dataset['energy_input_in_mwh'] = pd.to_numeric(self.formatted_dataset['energy_input_in_mwh'])
        self.formatted_dataset['energy_output_in_mwh'] = pd.to_numeric(self.formatted_dataset['energy_output_in_mwh'])

    def format_to_timeseries(self):
        self.formatted_dataset['timestamp_local'] = pd.to_datetime(self.formatted_dataset['timestamp_local'])
        self.formatted_dataset.sort_values(by='timestamp_local', inplace = True) 
        self.formatted_dataset.set_index(['timestamp_local'], inplace=True)

