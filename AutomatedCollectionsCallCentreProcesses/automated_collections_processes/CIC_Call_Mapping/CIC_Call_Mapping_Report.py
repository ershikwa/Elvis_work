# -*- coding: utf-8 -*-
"""
Created on Wed Apr  8 15:35:30 2020

@author: BTuckerAdmin
"""

import datetime
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyodbc

#Getting the previous day's year and month in order to select the correct tables from the database.
now = datetime.datetime.now()
d = (now - datetime.timedelta(days=1)).strftime("%Y%m")

server = 'mpwaps,17001' 
database = 'Prd_ContactInfo' 
username = 'BTuckerAdmin'
#username = 'PNeluheniAdmin'

###############################################################################################################################################################################
def runningMeanFast(x, N):
    return np.convolve(x, np.ones((N,))/N)[(N-1):]
###############################################################################################################################################################################
def CapacityArrayCalc(DialDurationArray, TalkDurationArray, FinalCallArray, InitiatedSecondArray, AgentArray, RPCArray, NumCallsFunc, NumLinesFunc, NumAgentsFunc, NumSecondsFunc):
     Line_Utilization_Array = np.zeros((NumLinesFunc,NumSecondsFunc),dtype=np.int)
     Agent_Utilization_Array = np.zeros((NumAgentsFunc,NumSecondsFunc),dtype=np.int)
     Call_Initiate_Array = np.zeros((NumLinesFunc,NumSecondsFunc),dtype=np.int)
     RPC_Array = np.zeros((NumAgentsFunc,NumSecondsFunc),dtype=np.int)
     Calls_Initiated = 0
     while Calls_Initiated < NumCallsFunc:
         for i in np.arange(NumSecondsFunc):
             Calls_Initiated_This_Second = 0
             Calls_This_Second = np.where(InitiatedSecondArray[:] == i)
             if len(Calls_This_Second[0]) > 0:
                 for j in np.arange(len(Calls_This_Second[0])):
                     if AgentArray[Calls_Initiated] > -1:
                         Agent_Utilization_Array[AgentArray[Calls_Initiated],i + DialDurationArray[Calls_Initiated]:i + DialDurationArray[Calls_Initiated] + TalkDurationArray[Calls_Initiated]] = 1
                         RPC_Array[AgentArray[Calls_Initiated],i + DialDurationArray[Calls_Initiated]:i + DialDurationArray[Calls_Initiated] + TalkDurationArray[Calls_Initiated]] = RPCArray[Calls_Initiated]
                     Line_Utilization_Array[np.where(Line_Utilization_Array[:,i] < 1)[0][0],i:(i + DialDurationArray[Calls_Initiated] + TalkDurationArray[Calls_Initiated])] = 1
                     Call_Initiate_Array[np.where(Line_Utilization_Array[:,i] < 1)[0][0],i] = 1
                     Calls_Initiated_This_Second += 1
                     Calls_Initiated += 1
     return Line_Utilization_Array, Agent_Utilization_Array, Call_Initiate_Array, RPC_Array
###############################################################################################################################################################################
def Post_Process(FinalLineUtilizationArray, FinalAgentUtilizationArray, CallInitiationArray, RPCArray):
    Line_Utilization = np.zeros(NumSeconds,dtype=np.int)
    Agent_Utilization = np.zeros(NumSeconds,dtype=np.int)
    Call_Initiation = np.zeros(NumSeconds,dtype=np.int)
    RPC_Array = np.zeros(NumSeconds,dtype=np.int)
    for i in np.arange(len(FinalLineUtilizationArray[0,:])):
        Line_Utilization[i] = len(np.where(FinalLineUtilizationArray[:,i] > 0)[0])
        Agent_Utilization[i] = len(np.where(FinalAgentUtilizationArray[:,i] > 0)[0])
        Call_Initiation[i] = len(np.where(CallInitiationArray[:,i] > 0)[0])
        RPC_Array[i] = len(np.where(RPCArray[:,i] > 0)[0])
    return Line_Utilization, Agent_Utilization, Call_Initiation, RPC_Array
###############################################################################################################################################################################
def CallPlan_Analytics(FinalCallArray, CallOutcomeArray, DialDurationArray, TalkDurationArray):
    CallsPerCycle = np.zeros(NumCycles,dtype=np.int)
    RPCPerCycle = np.zeros(NumCycles,dtype=np.int)
    TPCPerCycle = np.zeros(NumCycles,dtype=np.int)
    PTPPerCycle = np.zeros(NumCycles,dtype=np.int)
    UnclassifiedContactsPerCycle = np.zeros(NumCycles,dtype=np.int)
    for i in np.arange(NumCycles):
        CallsPerCycle[i] = len(np.where(FinalCallArray[:,i] > 0)[0])
        UnclassifiedContactsPerCycle[i] = len(np.where(CallOutcomeArray[:,i] == 4)[0])
        TPCPerCycle[i] = len(np.where(CallOutcomeArray[:,i] == 5)[0])
        RPCPerCycle[i] = len(np.where(CallOutcomeArray[:,i] > 5)[0])
        PTPPerCycle[i] = len(np.where(CallOutcomeArray[:,i] == 6)[0])
    FinalTotalCalls = np.sum(CallsPerCycle)
    UnclassifiedContactsTotal = np.sum(UnclassifiedContactsPerCycle)
    TPCTotal = np.sum(TPCPerCycle)
    RPCTotal = np.sum(RPCPerCycle)
    PTPTotal = np.sum(PTPPerCycle)
    return CallsPerCycle, FinalTotalCalls, TPCPerCycle, RPCPerCycle, PTPPerCycle, UnclassifiedContactsPerCycle, TPCTotal, RPCTotal, PTPTotal, UnclassifiedContactsTotal
###############################################################################################################################################################################
cnxn = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server='MPWAPS,17001', database='Prd_ContactInfo',trusted_connection='yes')
cursor = cnxn.cursor()
#Intentionally querying the base table - data errors will occur if they are out of sync.
df_AgentList = pd.read_sql('select day, count(distinct localuserid)	Agents, count(distinct clientnumber)	Clients, count(distinct callid_detail)	Calls \
from KM1_CIC_PTP_'+d+'_Reduced_Fields_DeDuped \
where campaign is not null \
group by day \
order by day',cnxn)

Agents_Per_Day = df_AgentList['Agents'].values
Day_Array = df_AgentList['day'].values
Current_Day = len(Agents_Per_Day)

Month = int(datetime.datetime.now().month)
Year = int(datetime.datetime.now().strftime("%Y"))
numsecs_plot = 50000


AvailableAgents = Agents_Per_Day[Current_Day-1]
Start_date = datetime.datetime(Year,Month,Day_Array[Current_Day-1],6,45,0)
date_list = [Start_date + datetime.timedelta(seconds=x) for x in range(numsecs_plot)]
time_list = list(range(len(date_list)))

QueryString = 'Select Call_Num, Duration, Dial_Duration, Total_Duration, hour, minute, clientnumber, finishcode, \
                RPC, PTP_Count, Initiated_Second, Connected_Second, AgentNum from [UNIZA\BTuckerAdmin].BHT_CIC_'+d+'_Call_Mapping_12 where day = ' + str(Day_Array[Current_Day-1]) + ' order by call_num'
df = pd.read_sql(QueryString,cnxn)
Agent_Array = df['AgentNum'].values

NumCycles = 50
NumLines = 1800
NumAgents = Agents_Per_Day[Current_Day-1]
NumSeconds = 60000

Final_Call_Array_1D = np.ones(len(df),dtype=np.int).ravel(order = 'F')
Call_Outcome_Array_1D = np.ones(len(df),dtype=np.int).ravel(order = 'F')
Dial_Duration_Array_1D = df['Dial_Duration'].values.ravel(order = 'F')
Talk_Duration_Array_1D = df['Duration'].values.ravel(order = 'F')
Initiated_Second_Array_1D = df['Initiated_Second'].values.ravel(order = 'F')
RPC_Array_1D = df['RPC'].values.ravel(order = 'F')

NumCalls = len(Final_Call_Array_1D)

Final_Line_Utilization_Array = np.zeros((NumLines,NumSeconds),dtype=np.int)
Final_Agent_Utilization_Array = np.zeros((NumAgents,NumSeconds),dtype=np.int)
Call_Initiation_Vector = np.zeros((NumAgents,NumSeconds),dtype=np.int)
Final_RPC_Array = np.zeros((NumAgents,NumSeconds),dtype=np.int)

Final_Line_Utilization_Array, Final_Agent_Utilization_Array, Call_Initiation_Vector, Final_RPC_Array = CapacityArrayCalc(Dial_Duration_Array_1D, Talk_Duration_Array_1D, Final_Call_Array_1D, Initiated_Second_Array_1D, Agent_Array, RPC_Array_1D, NumCalls, NumLines, NumAgents, NumSeconds)
Line_Utilization_Vector = np.zeros(NumSeconds,dtype=np.int)
Agent_Utilization_Vector = np.zeros(NumSeconds,dtype=np.int)
Line_Utilization_Vector, Agent_Utilization_Vector, Call_Initiation_Vector, RPC_Vector = Post_Process(Final_Line_Utilization_Array, Final_Agent_Utilization_Array, Call_Initiation_Vector, Final_RPC_Array)

Day_Run = Day_Array[Current_Day-1]
numsecs = 60000

Lines_Moving_Avg = runningMeanFast(Line_Utilization_Vector, 120)
Agents_Moving_Avg = runningMeanFast(Agent_Utilization_Vector, 120)
Calls_Initiated_Moving_Avg = runningMeanFast(Call_Initiation_Vector, 120)
RPC_Moving_Avg = runningMeanFast(RPC_Vector, 120)

np.save('PlotFile' + '_' + str(Year) + '_' + str(Month) + '_' + str(Day_Run) + ' ',[Lines_Moving_Avg, Agents_Moving_Avg, Calls_Initiated_Moving_Avg, RPC_Moving_Avg])

for i in np.arange(len(date_list)):
    time_list[i] = date_list[i].time()

Filename = 'PlotFile_' + str(Year) + '_' + str(Month) + '_' + str(Day_Run) + ' .npy'

Plot_Title_Overall = 'Call Mapping: ' + str(Year) + '/' + str(Month) + '/' + str(Day_Run)

Image_File_Name = 'PlotFile_' + str(Year) + '_' + str(Month) + '_' + str(Day_Run) + '_Image'

Extract_1 = np.load(Filename)

################################################################################################################################################################################################
####Original Plots
plt.figure(figsize = (20,8))
for i in np.arange(len(date_list)):
    time_list[i] = date_list[i].time()
plt.title(Plot_Title_Overall)
#plt.legend(Lines_Moving_Avg, Agents_Moving_Avg, Calls_Initiated_Moving_Avg, RPC_Moving_Avg)
Lines_patch = mpatches.Patch(color = 'blue', label = 'Lines Active')
#plt.legend(handles = [Lines_patch])
plt.plot(time_list,Extract_1[0,0:numsecs_plot],linewidth=0.7)
Agents_patch = mpatches.Patch(color = 'orange', label = 'Agents Active')
plt.plot(time_list,Extract_1[1,0:numsecs_plot],linewidth=0.7)
Calls_Initiated_patch = mpatches.Patch(color = 'green', label = 'Calls Initiated')
plt.plot(time_list,Extract_1[2,0:numsecs_plot],linewidth=0.7)
RPC_patch = mpatches.Patch(color = 'red', label = 'RPC Calls Active')
plt.plot(time_list,Extract_1[3,0:numsecs_plot],linewidth=0.7)

AvailableAgents_Array = AvailableAgents * np.ones(numsecs_plot,dtype=np.int)
plt.text(time_list[40000],AvailableAgents + 5,'Maximum Available Agents: ' + str(AvailableAgents))
plt.plot(time_list,AvailableAgents * np.ones(numsecs_plot,dtype=np.int),linewidth=0.7, dashes=[6, 2])
plt.legend(handles = [Lines_patch, Agents_patch, RPC_patch, Calls_Initiated_patch])
plt.savefig(Image_File_Name, dpi=200)
#plt.savefig(Image_File_Name, bbox_inches='tight', format='eps', dpi=1000)
################################################################################################################################################################################################
####Secondary Axis Plots
Image_File_Name_Sec = Image_File_Name + '_SecAxis'

fig, ax1 = plt.subplots(figsize = (20,8))

ax2 = ax1.twinx()
ax1.plot(time_list, Extract_1[1,0:numsecs_plot],linewidth=0.7, color='orange')
ax1.plot(time_list, Extract_1[2,0:numsecs_plot],linewidth=0.7, color='green')
ax1.plot(time_list, Extract_1[3,0:numsecs_plot],linewidth=0.7, color='red')
ax2.plot(time_list, Extract_1[0,0:numsecs_plot],linewidth=0.7, color='blue')
ax1.plot(time_list, AvailableAgents_Array,linewidth=0.7, color='grey', dashes=[6, 2])


Lines_patch = mpatches.Patch(color = 'blue', label = 'Lines Active')
Agents_patch = mpatches.Patch(color = 'orange', label = 'Agents Active')
Calls_Initiated_patch = mpatches.Patch(color = 'green', label = 'Calls Initiated')
RPC_patch = mpatches.Patch(color = 'red', label = 'RPC Calls Active')
Available_patch = mpatches.Patch(color = 'grey', label = 'Agents Available')
plt.legend(handles = [Agents_patch, RPC_patch, Calls_Initiated_patch, Lines_patch])
plt.text(time_list[200],np.max(Extract_1[0]) + 2,'Maximum Available Agents: ' + str(AvailableAgents))
ax1.set_xlabel('Time')
ax1.set_ylabel('Active Agents / RPC Calls / Calls Initiated')
ax2.set_ylabel('Lines Active')
plt.title(Plot_Title_Overall)
plt.savefig(Image_File_Name_Sec, dpi=200)
################################################################################################################################################################################################
cnxn.close()
print("Done: " + str(Day_Array[Current_Day-1]))


import Automation_System_Functions  
Automation_System_Functions.end_program()


