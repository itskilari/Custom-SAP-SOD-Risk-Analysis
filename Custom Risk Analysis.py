import pandas as pd
path = "Enter Ruleset/Report excel path"
df = pd.read_excel(path, sheet_name=0)
cRoles = set(df['Composite roles'].tolist()) # can also index sheet by name or fetch all sheets
sRoles = set(df['Single roles'].tolist())
rep = pd.DataFrame()
"""Function X Actions table Function|Action|Description|Status"""
funcActs = pd.read_excel(path, 'Function Actions', index_col=None)
#Actions and Function Table
actnFuncs = pd.read_excel(path, 'Action Function', index_col=None)
#Functions, Risks and Conflict Functions table
funcRisks = pd.read_excel(path, 'FunctionRisk', index_col=None)
riskLib = pd.read_excel(path, 'Risk Library', index_col=None)
critFuncs = ['BS15',	'BS16', 'BS17', 'BS20', 'BS18', 'BS19', 'CA01',
             'FI10', 'FI11', 'FI12', 'HN03', 'HN04', 'HN05', 'HN02',
             'HN13', 'HN14', 'HN16', 'HN17', 'HN18', 'HR06', 'HR07', 'MM09',
             'MM10', 'PM01', 'PP03', 'PR09', 'PR10', 'PS04', 'PS05', 'SD08', 'SD09']
aList = actnFuncs['Action'].tolist()

for x in sRoles:
    lTxns = df.groupby('Single roles').get_group(x)['T-code'].tolist()
    
    for i in lTxns:
        #Get functions for lookup Tcode from List1
        if i in aList:
            data = actnFuncs.groupby('Action').get_group(i)
            for index, lookFunc in data.iterrows():
                #Get Risks associated with Lookup Function/Tcode
                data2 = funcRisks.groupby('Function').get_group(lookFunc['Function'])
                for index2, riskFunc in data2.iterrows():
                    lRFunc = riskFunc['RFunctions']
                    data3 = funcRisks.groupby('Risk').get_group(riskFunc['Risk'])
                    if lookFunc['Function'] in critFuncs:
                        print(x, lookFunc['Action'], "-", riskFunc['Risk'], lookFunc['Function'], "(CRIT)")
                        rep = rep.append(pd.DataFrame({'0Roles': x,
                                                        '1Tcode':lookFunc['Action'], '2Risks':riskFunc['Risk'],
                                                       '3Risk Description' : riskLib.groupby('Risk').get_group(riskFunc['Risk']).iloc[0]['Risk description'],
                                                       '4Func':lookFunc['Function'],
                                                       '5Function description' : riskLib.groupby('Function').get_group(lookFunc['Function']).iloc[0]['Function description'],
                                                       '9Risk Description' : riskLib.groupby('Risk').get_group(riskFunc['Risk']).iloc[0]['Risk type'],
                                                       '10Risk Description' : riskLib.groupby('Risk').get_group(riskFunc['Risk']).iloc[0]['Priority']}, 
                                                        index=[0]), ignore_index=True, sort=True)
                        break
                    else:
                        for index3, criskFunc in data3.iterrows():
                            if criskFunc['RFunctions'] == lookFunc['Function']:
                                break
                                #get Conflict functoin and conflict tcodes
                            else:
                                data4 = funcActs.groupby('Function').get_group(criskFunc['RFunctions'])
                                ctlist = data4['Action'].tolist()
                                for j in ctlist:
                                    if j in lTxns and lookFunc['Action'] != j:
                                        print(x, lookFunc['Action'], "-", riskFunc['Risk'],
                                              lookFunc['Function'], criskFunc['RFunctions'], j)
                                        rep = rep.append(pd.DataFrame({'0Roles': x,
                                                                        '1Tcode':lookFunc['Action'],
                                                                       '2Risks':riskFunc['Risk'],
                                                                       '3Risk Description' : riskLib.groupby('Risk').get_group(riskFunc['Risk']).iloc[0]['Risk description'],
                                                                       '4Func':lookFunc['Function'],
                                                                       '5Function description' : riskLib.groupby('Function').get_group(lookFunc['Function']).iloc[0]['Function description'],
                                                                       '7CFunc':criskFunc['RFunctions'],
                                                                       '8ConFunction description' : riskLib.groupby('Function').get_group(criskFunc['RFunctions']).iloc[0]['Function description'],
                                                                       '9Risk Description' : riskLib.groupby('Risk').get_group(riskFunc['Risk']).iloc[0]['Risk type'],
                                                                       '10Risk Description' : riskLib.groupby('Risk').get_group(riskFunc['Risk']).iloc[0]['Priority'],
                                                                       '6CTcode': j}, index=[0]), ignore_index=True, sort=True)
                                    else:
                                        continue
        else:
            continue
        
rep.fillna(value="CRITICAL", axis=0, inplace=True)
writer = pd.ExcelWriter("C:\\Users\\<User>\\Desktop\\SOD\\Report.xlsx", engine='xlsxwriter')
rep.to_excel(writer, sheet_name='Sheet1', index=False)
writer.save()
writer.close()

