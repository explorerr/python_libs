
import numpy as np
from sklearn import datasets
from sklearn import linear_model
from sklearn import svm
from sklearn import ensemble

from sklearn.utils import shuffle
from sklearn.metrics import mean_squared_error

## error functions:
def cvrmse( pred, actu ):
    return np.sqrt( ((pred-actu)**2).mean() ) / np.mean(actu) 
    # return np.sqrt(mean_squared_error(pred, actu)) / np.mean(actu)

def cvrmse_max( pred, actu, mymax ):
    return np.sqrt( ((pred-actu)**2).mean() / mymax )

def cvrmse_max_cal( pred, actu ):
    return np.sqrt( ( (pred-actu)**2 ).mean() / np.max(actu) )

def mape( pred, actu ):
    return np.abs(pred - actu).mean() / np.mean(actu)

def mape_max( pred, actu, mymax):
    return np.abs(pred - actu).mean() / mymax

def mape_max_no_zero( pred, actu, mymax):
    return np.abs(pred[actu!=0] - actu[actu!=0]).mean() / mymax

def mae( pred, actu ):
    return np.abs(pred - actu).mean()

## data quality functions:
# import numpy as np
# from sklearn.preprocessing import Imputer
# # missing_values is the value of your placeholder, strategy is if you'd like mean, median or mode, and axis=0 means it calculates the imputation based on the other feature values for that sample
# imp = Imputer(missing_values='NaN', strategy='mean', axis=0)
# imp.fit(train)
# Imputer(axis=0, copy=True, missing_values='NaN', strategy='mean', verbose=0)
# train_imp = imp.transform(train)
    
def run_model( tr_df, ts_df, formular, add_params ):
    
    pass

def select_feature( tr_df, ts_df, formular, select_feature, fixed_feature ):
    
    pass

def moving_window_run_model( tr_df, ts_df, formular, seq_start, seq_end ):
    
    pass
    
    
    