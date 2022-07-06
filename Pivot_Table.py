# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import datetime
import warnings
warnings.filterwarnings( 'ignore' )

def Pivot_Table( csv_path, column_names, calculated_column_names, fill_value, top_3_cn, nuclear_energy, save_path, single_month=False ): 
    '''
    Arguments from UiPath 
    csv_path : the path of raw data  
    column_names : Chinese column names of raw/output data 
    calculated_column_names : Calculated column names 
    top_3_cn : fixed top three companies of report
    nuclear_energy : Chinese name of nuclear energy in category names
    save_path : the path of output data
    fill_value : value to use to fill holes
    '''
    
    # Chinese column names of raw data 原始資料中的部份欄位名稱
    columns = column_names.split()
    Year = columns[0]
    CategoryName = columns[2]
    ProductName = columns[4]
    CompanyName = columns[6]
    Month = columns[9]
    PolicyPremium = columns[10]
    EndorsementPremium = columns[12]
    PaidLoss = columns[14]
    UnpaidLoss = columns[16]
    
    # Calculated column names 需透過計算所得的欄位名稱:保單筆數、舉績、市佔率、成長率、損失金額、損失率、總計
    calculated_columns = calculated_column_names.split()
    PolicyCount = calculated_columns[0]
    Premium = calculated_columns[1]
    MarketShare = calculated_columns[2]
    GrowthRate = calculated_columns[3]
    Loss = calculated_columns[4]
    LossRatio = calculated_columns[5]
    TotalName = calculated_columns[6]

    # Read csv file
    raw_data = pd.read_csv( csv_path, encoding='big5' )
    
    # Statistic Month 統計月份(單月時)
    if single_month==True:
        now = datetime.datetime.now()
        statistic_month = 11 if now.month==1 else 12 if now.month==2 else now.month-2  
        
        ###### 開發測試用 ######
        #statistic_month = 9 
        ########################
        raw_data = raw_data[ raw_data[Month]==statistic_month ]
        
    # Fill missing value
    for col in columns: 
        raw_data[col].fillna( fill_value, inplace=True )
    
    # Trim all strings of a dataframe
    raw_data = raw_data.applymap( lambda x: x.strip() if isinstance(x,str) else x )

    # Calculate "Premium"(舉績) and "Loss"(損失金額) of each data
    raw_data[Premium] = raw_data.apply( lambda x: x[PolicyPremium]+x[EndorsementPremium], axis=1 )
    raw_data[Loss] = raw_data.apply( lambda x: x[PaidLoss]+x[UnpaidLoss], axis=1 )  

    # Select partial data from a dataframe
    data = raw_data[ [Year,CategoryName,ProductName,CompanyName,PolicyCount,Loss,Premium] ]
    
    # Get Category Name without nuclear energy 所有的險別名稱(不含核能保險)
    category_list = list( set(data[CategoryName]) )
    if nuclear_energy in category_list:
        category_list.remove( nuclear_energy )

    # Create pivot table 各險別樞紐分析表 
    table_list = {}
    for c in category_list:
        df = data[ data[CategoryName]==c ]
 
        # Ststistic year(統計年度) 
        current_year = df[Year].max()
        last_year = df[Year].min()

        # Market Summary 各險別各商品全業界總計
        MarketSummary = pd.pivot_table( df, values=[PolicyCount,Premium,Loss], index=[ProductName,Year], aggfunc=np.sum, dropna=False, fill_value=0 )
        TotalPremium = MarketSummary[ [Premium] ]
        MarketSummary.reset_index( inplace=True )
        MarketSummary[CompanyName] = TotalName
        MarketSummary[MarketShare] = "100.0%"
        MarketSummary[GrowthRate] = MarketSummary[[Premium]].pct_change( fill_method='ffill' )
        MarketSummary[GrowthRate] = MarketSummary.apply( lambda x: '' if x[Year]==last_year else '{:.1%}'.format(round(x[GrowthRate],1)), axis=1 )
        MarketSummary[GrowthRate] = MarketSummary.apply( lambda x: '100.0%' if (x[Year]==current_year and x[GrowthRate]=='inf%') 
                                                         else ('0.0%' if (x[Year]==current_year and x[GrowthRate]=='nan%') 
                                                               else ('' if (x[Year]==current_year and x[GrowthRate]=='-inf%') else x[GrowthRate]) ), axis=1 )
        MarketSummary[LossRatio] = MarketSummary.apply( lambda x: '--' if x[Premium]==0 else '{:.1%}'.format(round(x[Loss]/x[Premium],1)) , axis=1 )
        MarketSummary.set_index( [CompanyName,Year,ProductName], inplace=True )
        MarketSummary = MarketSummary[ [PolicyCount,Premium,MarketShare,GrowthRate,Loss,LossRatio] ]
        MarketSummary[PolicyCount] = MarketSummary.apply( lambda x: '--' if isinstance(x[PolicyCount],str) else '{:,}'.format(int(x[PolicyCount])), axis=1 )
        MarketSummary[Premium] = MarketSummary.apply( lambda x: '--' if isinstance(x[Premium],str) else '{:,}'.format(int(x[Premium])), axis=1 )
        MarketSummary[Loss] = MarketSummary.apply( lambda x: '--' if isinstance(x[Loss],str) else '{:,}'.format(int(x[Loss])), axis=1 )
        
        # Get company order 依各險別各公司當年度的總舉績，計算各公司在各險別中的排名
        # 註: 報表中的公司排序前三間: 國泰、富邦、新光
        performance = pd.pivot_table( df[ df[Year]==current_year ], values=Premium, index=CompanyName, 
                                      aggfunc=np.sum, dropna=False, fill_value='--' )
        performance.sort_values( by=Premium, ascending=False, inplace=True )
        company_rank = performance.index.values.tolist()
        company_list = dict(zip(company_rank,range(1,len(company_rank)+1)))
        for company in top_3_cn.split(): 
            if company in company_list.keys():
                rank = company_list.get( company )
                del company_list[company]
                company_list = {company:rank, **company_list}
        company_order = list(company_list.keys())
  
        # Calculate pivot table
        PivotTable = pd.pivot_table( df, values=[PolicyCount,Premium,Loss], index=[CompanyName,Year,ProductName], 
                                     aggfunc=np.sum, dropna=False, fill_value='--' )
   
        # Calculate "Market Share"(市佔率)
        TotalPremium.columns = ['Total_Premium']
        TotalPremium.reset_index( inplace=True )
        PivotTable.reset_index( inplace=True )
        PivotTable = pd.merge( PivotTable, TotalPremium, on=[ProductName,Year], how='left' )
        PivotTable[MarketShare] = PivotTable.apply( lambda x: '{:.2%}'.format(round(x[Premium]/x['Total_Premium'],4)) if (isinstance(x[Premium],float) and x['Total_Premium']>0) else '--', axis=1 )
        PivotTable.drop( columns=['Total_Premium'], inplace=True )
        PivotTable.set_index( [CompanyName,Year,ProductName], inplace=True )

        # Calculate "Loss Ratio"(損失率)
        PivotTable[LossRatio] = PivotTable.apply( lambda x: '--' if isinstance(x[Premium],str) else ('--' if x[Premium]==0 else '{:.1%}'.format(x[Loss]/x[Premium])), axis=1 )

        # Convert float to int
        PivotTable[Premium] = PivotTable.apply( lambda x: '--' if isinstance(x[Premium],str) else '{:,}'.format(int(x[Premium])), axis=1 )
        PivotTable[Loss] = PivotTable.apply( lambda x: '--' if isinstance(x[Loss],str) else '{:,}'.format(int(x[Loss])), axis=1 )
 
        # Calculate "Growth Rate"(成長率) of "Premium"(舉績)
        ## 各公司各年度各商品的保費總計
        CompanyPremium = pd.pivot_table( df, values=Premium, index=[CompanyName,ProductName,Year], 
                                         aggfunc=np.sum, dropna=False, fill_value=0 )
        CompanyPremium.reset_index( inplace=True )
        CompanyPremium[GrowthRate] = CompanyPremium.groupby([CompanyName])[[Premium]].pct_change( fill_method='ffill' )
        CompanyPremium.set_index( [CompanyName,Year,ProductName], inplace=True )  
        CompanyPremium = CompanyPremium[[GrowthRate]]
        CompanyPremium = CompanyPremium[ CompanyPremium.index.get_level_values(Year)==current_year ]
        CompanyPremium.dropna( inplace=True )
        CompanyPremium[GrowthRate] = CompanyPremium.apply( lambda x: '100.0%' if (x[GrowthRate]==float('inf') or x[GrowthRate]==float('-inf')) 
                                                           else ('' if isinstance(x[GrowthRate],str) 
                                                                 else '{:.2%}'.format(round(x[GrowthRate],4))), axis=1 )
        
        # Update values in "PivotTable" dataframe from "CompanyPremium" dataframe
        PivotTable[GrowthRate] = ''
        PivotTable = PivotTable[ [PolicyCount,Premium,MarketShare,GrowthRate,Loss,LossRatio] ]
        PivotTable.update( CompanyPremium )
                
        # Combine two dataframes "PivotTable" and "MarketSummary"
        FinalResult = PivotTable.append( MarketSummary )
       
        # Reshape a dataframe
        FinalResult = FinalResult.stack().unstack([2,3])
        
        # Reorder the company name 
        FinalResult = FinalResult.reindex( np.append(company_order,TotalName), level=CompanyName ) 
         
        # Add a new item to a dictionary
        table_list[c] = FinalResult   
 
    # Write multiple sheets in a xlsx file
    with pd.ExcelWriter( save_path, engine='openpyxl' ) as writer:    
        for key in table_list:
            table_list[key].to_excel( writer, key )   
        writer.save() 
        
    return category_list