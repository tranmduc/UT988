# -*- coding: utf-8 -*-
"""
Created on Thu May 14 16:01:49 2020

@author: Minh Duc
"""

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn import metrics

df = pd.read_csv("everLoan_04.csv")

#print(df.isnull().sum())

#X = df.drop(columns=['ISDN','STATUS','SEX','SUB_AGE','MONTH_ACTIVE'])
#X = df.drop(columns=['ISDN', 'STATUS', 'SEX'])
X = df[['REVENUE_01', 'REVENUE_02', 'REVENUE_03', 'ScratchAllAmt', 'RevenueAllAmt']]
#X = df[['REVENUE_01', 'REVENUE_02', 'REVENUE_03','TOTAL_DURATION_OG_01','TOTAL_CALL_OG_01','AMOUNT','TOTAL_SCRATCH_VALUE_01','TOTAL_DURATION_OG_02','TOTAL_CALL_OG_02']]# Features
#X = df[['REVENUE_CHECK']]
y = df['STATUS'] # Target variable

X_train,X_test,y_train,y_test=train_test_split(X,y,test_size=0.3
                                               ,random_state=42
                                               )


model = RandomForestClassifier(n_estimators=1000, 
                               #bootstrap = True,
                               #max_features = 'sqrt',
                               random_state = 42)
# Fit on training data
model.fit(X_train, y_train)

y_pred_train = model.predict(X_train)
y_pred=model.predict(X_test)

# import the metrics class
cnf_matrix = metrics.confusion_matrix(y_test, y_pred)
print(cnf_matrix)

print("Accuracy on train:",metrics.accuracy_score(y_train, y_pred_train))
print("Accuracy:",metrics.accuracy_score(y_test, y_pred))
print("Precision:",metrics.precision_score(y_test, y_pred))
print("Recall:",metrics.recall_score(y_test, y_pred))
print("F1-Score:",metrics.f1_score(y_test, y_pred))

fi = pd.DataFrame({'feature': list(X.columns),
                   'importance': model.feature_importances_}).\
                    sort_values('importance', ascending = False)

print(fi.head(15))