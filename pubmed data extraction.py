#!/usr/bin/env python
# coding: utf-8

'''
Pubmed data extraction using entrez and ranking the given KOL lists from the excel 
'''


#imports:

from Bio import Entrez
import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from geotext import GeoText
import math
import numpy as np


#entrez:

Entrez.email = "ex@gmail.com" 
handle = Entrez.einfo()
rec = Entrez.read(handle)
handle.close()


#extracting data from pubmed database with reference to the "KOL List" provided via email:

id_list2=[]
df = pd.DataFrame()
dict12={}
temp_dic = {}
a = []
d2={}

kol_data= pd.read_excel("KOL list.xlsx")
kol_data.drop_duplicates(inplace=True)
kol_data.reset_index(inplace=True)

last1= kol_data['Last']
first1= kol_data['First']

#for each first name and last name, search details (from e search) and efetch the details with respect to their PMID (from e fetch)
for i in range(0,len(last1)):
    
    handle = Entrez.esearch(db="pubmed", term='({}[Author])'.format(last1[i]+" "+first1[i]),retmax='1000') 
    dict12[i]= Entrez.read(handle)
    handle.close()
    id_list2 = dict12[i]['IdList']
    print(len(id_list2))
    
    a1=math.ceil(len(id_list2)/200)
    abcde=(len(id_list2)%200)
    
    for ab in range (0,a1):
        handle = Entrez.efetch(db='pubmed', id=id_list2, rettype='medline', retmode='xml',retstart=ab*200,retmax='200')
        dict12[i]= Entrez.read(handle)
        handle.close()
        
        d2=dict(dict12[i])
        

        for j in range(len(d2['PubmedArticle'])):
            temp_dic = d2['PubmedArticle'][j]['PubmedData']
            temp_dic2 = d2['PubmedArticle'][j]['MedlineCitation']


            #getting  PMID, year, month and date of pyblication
            temp_dic['PMID'] = temp_dic['ArticleIdList'][0]

            #getting dates

            if (len(d2['PubmedArticle'][j]['MedlineCitation']['Article']['ArticleDate']))==0:
                temp_dic['Article_Date']=""
            else:
                temp_dic['Article_Date']= d2['PubmedArticle'][j]['MedlineCitation']['Article']['ArticleDate'][0]['Year'] + str("-") + d2['PubmedArticle'][j]['MedlineCitation']['Article']['ArticleDate'][0]['Month'] + str("-") + d2['PubmedArticle'][j]['MedlineCitation']['Article']['ArticleDate'][0]['Day']

            for t in range (0, len(temp_dic['History'])):
                if ('received' in str(temp_dic['History'][t])):
                        temp_dic['PubMed_recieved'] = temp_dic['History'][t]['Year'] + str("-") + temp_dic['History'][t]['Month'] + str("-") + temp_dic['History'][t]['Day']
                elif ('revised' in str(temp_dic['History'][t])):
                        temp_dic['PubMed_revised'] = temp_dic['History'][t]['Year'] + str("-") + temp_dic['History'][t]['Month'] + str("-") + temp_dic['History'][t]['Day']
                elif ('accepted' in str(temp_dic['History'][t])):
                        temp_dic['PubMed_accepted'] = temp_dic['History'][t]['Year'] + str("-") + temp_dic['History'][t]['Month'] + str("-") + temp_dic['History'][t]['Day']
                elif ('pubmed' in str(temp_dic['History'][t])):
                        temp_dic['PubMed_published'] = temp_dic['History'][t]['Year'] + str("-") + temp_dic['History'][t]['Month'] + str("-") + temp_dic['History'][t]['Day']
                elif ('medline' in str(temp_dic['History'][t])):
                        temp_dic['PubMed_medline'] = temp_dic['History'][t]['Year'] + str("-") + temp_dic['History'][t]['Month'] + str("-") + temp_dic['History'][t]['Day']
                elif ('entrez' in str(temp_dic['History'][t])):
                        temp_dic['PubMed_entrez'] = temp_dic['History'][t]['Year'] + str("-") + temp_dic['History'][t]['Month'] + str("-") + temp_dic['History'][t]['Day']

            #title of the paper
            temp_dic['Title'] = temp_dic2['Article']['ArticleTitle']


            #getting first name of author 
            temp_dic['First_Name'] = first1[i]


            #co-author list:
            temp_dic['Co-author']=""
            for k in range (0,(len(temp_dic2['Article']['AuthorList']))):
                if ("ForeName" in (temp_dic2['Article']['AuthorList'][k])):
                    if ((first1[i] in temp_dic2['Article']['AuthorList'][k]["ForeName"] ) and (last1[i] in temp_dic2['Article']['AuthorList'][k]["LastName"])):
                        continue
                    else:
                        temp_dic['Co-author'] = str(temp_dic['Co-author']) + str(", ") + str(temp_dic2['Article']['AuthorList'][k]['ForeName']) +str(" ") + str(temp_dic2['Article']['AuthorList'][k]['LastName'])
            
            
            if ("ForeName" in (temp_dic2['Article']['AuthorList'][0])):
                if ((str(first1[i]) in str(temp_dic2['Article']['AuthorList'][0]["ForeName"]))):
                    temp_dic['Main_Author']=1
                else:
                    temp_dic['Main_Author']=0
            else:
                temp_dic['Main_Author']=0
            
            #getiing last name and initial
            temp_dic['Last_Name'] = last1[i]

            #initials of Author
            if ("Initials" in (temp_dic2['Article']['AuthorList'][0])):
                temp_dic['Initials'] = temp_dic2['Article']['AuthorList'][0]['Initials']
            else:
                temp_dic['Initials'] = ""    


            #affiliation info    
            temp_dic['Affiliation'] = ""
            for h in range (0, (len(temp_dic2['Article']['AuthorList']))):
                if ("LastName" in (temp_dic2['Article']['AuthorList'][h])):
                    if (str(temp_dic2['Article']['AuthorList'][h]["LastName"]) == str(last1[i]) and str(temp_dic2['Article']['AuthorList'][h]["ForeName"]) == str(first1[i])):
                        if (len(temp_dic2['Article']['AuthorList'][h]['AffiliationInfo'])==0):
                            temp_dic['Affiliation']= ""
                        else:
                            temp_dic['Affiliation'] = d2['PubmedArticle'][j]['MedlineCitation']['Article']['AuthorList'][h]['AffiliationInfo'][0]['Affiliation']
                            break


            #publication type
            temp_dic['Publication_Type']=""
            for v in range(0,len(temp_dic2['Article']['PublicationTypeList'])):
                temp_dic['Publication_Type'] += temp_dic2['Article']['PublicationTypeList'][v] + str(", ")


            #country
            temp_dic['Country'] = temp_dic2['MedlineJournalInfo']['Country']


            #Number of ciatation and references (if statement is there to rectify null values and was giving error)
            if (len(temp_dic['ReferenceList']))== 0 :
                temp_dic['No of Citation'] = 0
                temp_dic['References'] =""
            else:
                temp_dic['References']= ""
                temp_dic['No of Citation'] = len(temp_dic['ReferenceList'][0]['Reference'])
                for u in range (0, len(temp_dic['ReferenceList'][0]['Reference'])):
                    temp_dic['References'] += temp_dic['ReferenceList'][0]['Reference'][u]['Citation'] + str('; ')   

            #Getting the MeSH headings    
            temp_dic['MeSH_headings']="" 
            if 'MeshHeadingList' in d2['PubmedArticle'][j]['MedlineCitation']:
                if (len(d2['PubmedArticle'][j]['MedlineCitation']['MeshHeadingList'])==0):
                    temp_dic['MeSH_headings']=""
                else:
                    for p in range (0, len(d2['PubmedArticle'][j]['MedlineCitation']['MeshHeadingList'])):
                        temp_dic['MeSH_headings']+= d2['PubmedArticle'][j]['MedlineCitation']['MeshHeadingList'][p]['DescriptorName']+ str(", ")


            a.append(temp_dic)
        df = df.append(pd.DataFrame(a))


########################################
df.drop(["History","ReferenceList","PublicationStatus","ArticleIdList",],axis=1,inplace=True)

#dropping duplicates from df or PubMed extraction

df.drop_duplicates(inplace= True)
df.reset_index(drop=True, inplace=True)
df


###creating full name based on first name and last name
df['Full_Name']= df['First_Name']+ str(" ") + df['Last_Name']
df.dropna(subset=['PMID'],axis='rows',inplace= True)
df.dropna(subset=['Full_Name'],axis='rows',inplace= True)

kol_data['Full_Name']= kol_data['First'] + str(" ") + kol_data['Last']
kol_data.dropna(subset=['Full_Name'], inplace= True)
new=df


##merging KOL data and PubMed data
combined1= pd.merge(kol_data,df,on='Full_Name',how = 'inner')
combined1.drop(['index'],axis=1,inplace=True)

combined1.drop(['Last','MI','First','First_Name','Last_Name','Initials','Publication_Type'],axis=1,inplace=True)
combined1.to_csv('combined.csv')


## finding unique MeSH headings
mesh_list=[]
a=combined1['MeSH_headings'].str.split(',')
for i in range(0,len(a)):
    if str(a[i]) == 'nan':
        continue
    else:
        for j in range(0,len(a[i])):
            if str(a[i][j]) not in str(mesh_list):
                mesh_list.append(a[i][j])
            else:
                continue

## removing unwanted leading 0's
mesh_list= [x.strip() for x in mesh_list]

## Finding the MeSH headings and the respective number it appeared in MeSH 

a=combined1[['MeSH_headings','PMID']]
MeSH_no_data=[]

a.drop_duplicates(inplace=True)
a.reset_index(inplace=True)
a.drop(['index'], axis= 'columns',inplace=True)

for m in range (0,len(mesh_list)):
    mesh_terms = mesh_list[m]
    count = 0
    for j in range(0,len(a)):
        if str(mesh_terms) in str(a['MeSH_headings'][j]):
            count +=1
        else:
            continue
    MeSH_no_data.append([mesh_terms,count]) 
print("done")


## MeSH repeating more than 10 times 
Mesh_DF= pd.DataFrame(MeSH_no_data)
Mesh_DF.sort_values(by=1,ascending=False,inplace=True)
final= Mesh_DF
final2=final[final[1]>10]
final2.reset_index(inplace=True,drop=True)
final2.drop_duplicates(inplace=True)
final2
# final2.to_csv('Mesh name and counts1.csv')


## read the edited mesh_headings (deleted unnecessary headings and kept only heart related mesh headings)
final_mesh_headings= pd.read_csv('Mesh name and counts.csv')


##matching if city of KOL list and Author's city from PubMed is matching
combined1['Match']= 100
for g in range(0, len(combined1['PMID'])):
    loc1= combined1.columns.get_loc('Match')
    for h in range(0, len(final_mesh_headings['Headings'])):
        if (str(final_mesh_headings['Headings'][h]).lower() not in (str(combined1.loc[g]['MeSH_headings'])).lower()):
            if str(final_mesh_headings['Headings'][h]).lower() in str(combined1.loc[g]['Title']).lower():
                if (combined1.loc[g]['City'] in (GeoText(combined1.loc[g]['Affiliation'])).cities) and str(GeoText(combined1.loc[g]['Country']).country_mentions)[15:17] in str(GeoText(combined1.loc[g]['Affiliation']).country_mentions):
                    combined1.iat[g,loc1]= 1
                    break
                elif (str(combined1.loc[g]['Hospital']).lower().strip() in (str(combined1.loc[g]['Affiliation']).lower())):
                    combined1.iat[g,loc1]= 1
                    break
                    break
                else:
                    combined1.iat[g,loc1]= 0
                    continue
            else:
                combined1.iat[g,loc1]= 0
                continue
                    
        else:
            if (combined1.loc[g]['City'] in (GeoText(combined1.loc[g]['Affiliation'])).cities) and str(GeoText(combined1.loc[g]['Country']).country_mentions)[15:17] in str(GeoText(combined1.loc[g]['Affiliation']).country_mentions):
                combined1.iat[g,loc1]= 1
                break
            elif (str(combined1.loc[g]['Hospital']).lower().strip() in (str(combined1.loc[g]['Affiliation'])).lower()):
                combined1.iat[g,loc1]= 1
                break
            else:
                combined1.iat[g,loc1]= 0
        
abc=combined1[combined1['Match']==1]
abc.reset_index(drop= True, inplace=True)
abc
#abc.to_csv('combined1.csv')


###getting the latest and oldest publication dates
from datetime import datetime
combined1= pd.read_csv('Combined.csv')
abcd = combined1.astype({'Article_Date':'string'})
new1= abcd[['Full_Name','Article_Date']]

combined1[['Article_Date']]
type(new1['Article_Date'][0])
newmindate= new1.groupby(['Full_Name']).min()
newmindate.rename(columns={"Article_Date":"Oldest_pub_date"},inplace=True)
newmindate
newmaxdate= new1.groupby(['Full_Name']).max()
newmaxdate.rename(columns={"Article_Date":"Latest_pub_date"},inplace=True)
mixed_date= newmindate.merge(newmaxdate, on=['Full_Name'])
mixed_date

mixed_date['Oldest_pub_date']=  mixed_date['Oldest_pub_date'].map(lambda x: datetime.strptime(x,'%Y-%m-%d'))
mixed_date['Latest_pub_date']=  mixed_date['Latest_pub_date'].map(lambda x: datetime.strptime(x,'%Y-%m-%d'))

mixed_date['exp_in_pulishing']= mixed_date.apply(lambda x: x['Latest_pub_date'] - x['Oldest_pub_date'], axis=1)
mixed_date.sort_values(by = ['exp_in_pulishing'],ascending=False, inplace=True)
mixed_date






####################################################################

#----------------------------------
#KOL based on highest number of publications

pubmed1= abc[['PMID','Full_Name']]
pubmed2= pubmed1.groupby(['Full_Name']).count().sort_values(by='PMID',ascending=False)
pubmed2['Rank no_pub']=pubmed2.PMID.rank(pct=True)*0.35
pubmed2['NumberOfPublications']= pubmed2['PMID']
rank1= pubmed2
rank1

#-----------------------------------
#KOL based on highest number of citation
abc.to_csv('check12222.csv')
pubmed12 = abc[['Full_Name','No of Citation']]
pubmed12.to_csv('check123.csv')

pubmed12.rename(columns={"No of Citation":"No_of_Citation"},inplace=True)
pubmed12= pubmed12.groupby(['Full_Name']).sum().sort_values(by='No_of_Citation',ascending=False) 
pubmed12
pubmed12['Rank no_citation']=pubmed12.No_of_Citation.rank(pct=True)*0.25
rank2=pubmed12
rank2


#-----------------------------------
# #KOL based on number of main author

pubmed1 = abc[['Full_Name','Main_Author']]
pubmed2= pubmed1.groupby(['Full_Name']).sum().sort_values(by='Main_Author',ascending=False) 
pubmed2['Rank Main_Author']=pubmed2.Main_Author.rank(pct=True)*0.2
rank3 = pubmed2
rank3

pubmed1= abc[['Full_Name','Emerging']]
emerging = pubmed1.groupby(['Full_Name']).min().sort_values(by='Emerging',ascending=False) 
emerging


#-----------------------------------
# based on latest publications
rank4= mixed_date[['Oldest_pub_date','Latest_pub_date']]
rank4['Rank LatestPub']=rank4.Latest_pub_date.rank(pct=True)*0.07
rank4


rank5= mixed_date[['exp_in_pulishing']]

rank5['Rank PublExp']=rank5.exp_in_pulishing.rank(pct=True)*0.13
rank5


#-----------------------------------
#based on average rankings

rankmerged= pd.merge(rank1,rank2,on=['Full_Name']).merge(rank3, on= ["Full_Name"]).merge(emerging, on = ["Full_Name"]).merge(rank4, on=["Full_Name"]).merge(rank5, on= ["Full_Name"])
rankmerged['total_ratings']= rankmerged['Rank no_pub'] + rankmerged['Rank no_citation'] +rankmerged['Rank Main_Author']+ rankmerged['Rank LatestPub']+rankmerged['Rank PublExp']
final_merged= rankmerged[['NumberOfPublications','No_of_Citation','Main_Author','Latest_pub_date','exp_in_pulishing','total_ratings','Emerging']]
final_merged.sort_values(by='total_ratings',ascending=False,inplace=True)
final_merged['Final']= final_merged.total_ratings.rank(pct=True)
final_merged['Final']= round(final_merged['Final']*100,2)
final_merged




###############################

# selecting only required columns
df_final= abc[['Full_Name','City','State','Global Region','US Area','Hospital','Position','Segment','Email']]

#remove duplicates
df_final.drop_duplicates(inplace=True)
df_final.reset_index(inplace = True, drop=True)
df_final


# merging ranking and the required columns
final_output= df_final.merge(final_merged, on= ['Full_Name'])
final_output.drop(['total_ratings'],axis=1 , inplace=True)

# sorting and making the final output
final_output.sort_values(by=['Final'], ascending=False, inplace=True)
final_output.reset_index(inplace=True, drop=True)
final_output.index = np.arange(1, len(final_output) + 1)
final_output

final_output.to_csv('final output.csv')