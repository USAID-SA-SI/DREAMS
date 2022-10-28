# Title: AGYW_PREV MER Reporting Script
# Author: C. Trapence
# Purpose: Automating the process of Reporting AGYW_PREV for Inter agency
# Date:2022-10-22
#Updated:2022:10:28
#Load Required libraries

library(tidyverse)
library(readxl)
library(lubridate)
library(readr)
library(excel.link)
library(openxlsx)
library(data.table)
library(sqldf)
library(stringr)


setwd("C:\\Users\\ctrapence\\Documents\\Clement Trapence-South Africa WP\\SCRIPTS\\DREAMS Import")
# Load the master site list, mechanisms as extracted from DATIM

#Site_List <- read_delim("sitebyim.txt")

#site_list2<- Site_List %>%   select (orgunituid: facility) %>% distinct(orgunituid, .keep_all = TRUE)

#write.xlsx(site_list2,"Site_list.xls" )

sitebyim<-read.xlsx("Site_list.xlsx") %>%   rename(district=psnu) %>% filter(facility=="Data reported above Facility level")

DREAMS_PSNU<-sitebyim %>%  select(district,psnuuid,orgunituid,operatingunit,operatingunituid,mech_code) %>% group_by(district) %>%  distinct(district,.keep_all = TRUE) %>%  mutate(mech_code=as.integer(mech_code))

#Mechanims names to get attributecombooption_ID

mechanisms <- read.csv("mechanisms.csv", stringsAsFactors=FALSE) %>% filter(ou=='South Africa') %>%  mutate(attributeOptionCombo="cDGPF739ZZr")

# Process DREAMS Data and correcting the District name from short names to full names

AGYW_PREV_raw<- read_excel("AGYW_PREV_Export.xlsx", sheet = "Pivot_Data")%>% mutate(PSNU=case_when(district=="kz Uthukela District Municipality"~"kz Uthukela District Municipality",district=="gp Ekurhuleni Metropolitan Municipality"~"gp Ekurhuleni Metropolitan Municipality",
district=="kz eThekwini Metropolitan Municipality"~"kz eThekwini Metropolitan Municipality",district=="ec Oliver Tambo District Municipality"~"ec Oliver Tambo District Municipality",
district=="kz Zululand District Municipality"~"kz Zululand District Municipality",district=="kz uMgungundlovu District Municipality"~"kz uMgungundlovu District Municipality",  
district=="nw Dr Kenneth Kaunda District Municipality"~"nw Dr Kenneth Kaunda District Municipality",district=="nw Bojanala Platinum District Municipality"~"nw Bojanala Platinum District Municipality",
district=="nw Ngaka Modiri Molema District Municipality"~"nw Ngaka Modiri Molema District Municipality" ,district=="gp City of Tshwane Metropolitan Municipality"~"gp City of Tshwane Metropolitan Municipality",
district=="mp Gert Sibande District Municipality"~"mp Gert Sibande District Municipality",district=="mp Ehlanzeni District Municipality"~"mp Ehlanzeni District Municipality",
district=="wc City of Cape Town Metropolitan Municipality"~"wc City of Cape Town Metropolitan Municipality",district=="gp City of Johannesburg Metropolitan Municipality"~"gp City of Johannesburg Metropolitan Municipality",
district=="lp Mopani District Municipality"~"lp Mopani District Municipality",district=="mp Nkangala District Municipality"~"mp Nkangala District Municipality",district=="kz King Cetshwayo District Municipality"~"kz King Cetshwayo District Municipality",
district=="kz Ugu District Municipality"~"kz Ugu District Municipality",district=="ec Buffalo City Metropolitan Municipality"~"ec Buffalo City Metropolitan Municipality",district=="gp Sedibeng District Municipality"~"gp Sedibeng District Municipality",
district=="lp Capricorn District Municipality"~"lp Capricorn District Municipality",district=="fs Thabo Mofutsanyane District Municipality"~"fs Thabo Mofutsanyane District Municipality",district=="fs Lejweleputswa District Municipality"~"fs Lejweleputswa District Municipality",
district=="ec Alfred Nzo District Municipality"~"ec Alfred Nzo District Municipality"))

#When CBMIS Uses short names for Districts please use the code below
 # #mutate(PSNU=case_when(district=="Uthukela"~"kz Uthukela District Municipality",district=="Ekurhuleni"~"gp Ekurhuleni Metropolitan Municipality",
 #                          district=="eThekwini"~"kz eThekwini Metropolitan Municipality",district=="O.R.Tambo"~"ec Oliver Tambo District Municipality",
 #                          district=="Zululand"~"kz Zululand District Municipality",district=="Umgungundlovu"~"kz uMgungundlovu District Municipality",  
 #                          district=="Dr Kenneth Kaunda"~"nw Dr Kenneth Kaunda District Municipality",district=="Bojanala"~"nw Bojanala Platinum District Municipality",
 #                          district=="Ngaka Modiri Molema"~"nw Ngaka Modiri Molema District Municipality" ,district=="City of Tshwane"~"gp City of Tshwane Metropolitan Municipality",
 #                          district=="Gert Sibande"~"mp Gert Sibande District Municipality",district=="Ehlanzeni"~"mp Ehlanzeni District Municipality",
 #                          district=="City of Cape Town"~"wc City of Cape Town Metropolitan Municipality",district=="City of Johannesburg"~"gp City of Johannesburg Metropolitan Municipality",
 #                          district=="Mopani"~"lp Mopani District Municipality",district=="Nkangala"~"mp Nkangala District Municipality",district=="King Cetshwayo"~"kz King Cetshwayo District Municipality",
 #                          district=="Ugu"~"kz Ugu District Municipality",district=="Buffalo City"~"ec Buffalo City Metropolitan Municipality",district=="Sedibeng"~"gp Sedibeng District Municipality",
 #                          district=="Capricorn"~"lp Capricorn District Municipality",district=="Thabo Mofutsanyane"~"fs Thabo Mofutsanyane District Municipality",district=="Lejweleputswa"~"fs Lejweleputswa District Municipality",
 #                          district=="Alfred Nzo"~"ec Alfred Nzo District Municipality"))


#Removes all the Inactive beneficiaries from the dataset

AGYW_PREV1.0<-sqldf('select * from AGYW_PREV_raw where status  Not like "%Inactive%"')
#Remove the numbering from status column to create a matching category option combo
AGYW_PREV1.0 <- AGYW_PREV1.0 %>%  select(province,PSNU, sex,district, `first partner`,agecat,status,disaggregation,unique_count) %>% filter(unique_count!=0) %>% 
  mutate(status=gsub("[1-9.*]", "", status))  %>% mutate(status=gsub("[1-9*]", "", status))  %>% rename (mech_name=`first partner`,Value=unique_count) 
  
#Adding in Mechanism ID's using Implementing Partners names
AGYW_PREV1.1<-AGYW_PREV1.0%>% mutate(disaggregation=gsub("[a-z.*]","",disaggregation))  %>% mutate(time="Months in DREAMS")%>%  select (-district) %>% rename(district=PSNU) %>% 
mutate(mech_code=as.integer(case_when(mech_name=="Shout It Now"~"81891",mech_name=="TB/HIV Care"~"83013",mech_name=="NACOSA - GBV (80008)"~"80008",mech_name=="NACOSA - OVC (80002)"~"80002",
        mech_name=="FHI 360 DREAMS"~"82199",mech_name=="Wits Reproductive Health& HIV Institute"~"80007",mech_name=="FHI 360 OVC"~"14295",mech_name=="HIVSA"~"70307",mech_name=="Mothers to Mothers (M2M)"~"80004",
        mech_name=="Centre for Communication Impact (CCI)"~ "17537",mech_name=="Education Development Center (EDC)"~"160611",mech_name=="Pact"~"14631") )) 

AGYW_PREV1.2<-AGYW_PREV1.1 %>% mutate(disaggregation=paste(disaggregation ,time,sep=" ")) %>% select(-time)

tempfile1 <- left_join(DREAMS_PSNU, AGYW_PREV1.2, by=c("district"))

tempfile1.1<-tempfile1  %>%  filter(Value!=0) %>% 
  mutate(categoryoptioncombo=case_when(disaggregation!="/ Months in DREAMS"~paste(agecat,sex,disaggregation,status,sep=", "))) %>% 
  
  mutate(categoryoptioncombo2=case_when(disaggregation=="/ Months in DREAMS"~paste(agecat,sex,status,sep=", "))) %>% 
  mutate(categoryoptioncombo_final=if_else(disaggregation=="/ Months in DREAMS",categoryoptioncombo2,categoryoptioncombo)) %>% 
  select(-categoryoptioncombo2,-categoryoptioncombo ) %>%  
  rename (categoryoptioncombo=categoryoptioncombo_final)

#House keeping on AGYW Category option coding
tempfile1.2<-tempfile1.1 %>%  mutate(categoryoptioncombo=gsub("pa","Pa",categoryoptioncombo)) %>%  mutate(categoryoptioncombo=gsub("com","Com",categoryoptioncombo)) %>% 
  mutate(categoryoptioncombo=gsub("sec","Sec",categoryoptioncombo)) %>%  mutate(categoryoptioncombo=gsub("*8*","",categoryoptioncombo)) %>% mutate(catecombo=categoryoptioncombo)  %>%  mutate(categoryoptioncombo=tolower(gsub(" ","",categoryoptioncombo))) %>% 
  mutate(categoryoptioncombo=gsub("0-6","<6",categoryoptioncombo)) %>%  mutate(categoryoptioncombo=gsub(",*nosecondary","",categoryoptioncombo)) %>% mutate(categoryoptioncombo=gsub(":",",",categoryoptioncombo)) 
  
Host_results<-read.csv("Host Country Results DREAMS (USG).csv")   %>%  mutate(categoryoptioncombo=tolower(gsub(" ","",categoryoptioncombo))) %>%  select(dataset,dataelement,dataelementdesc,dataelementuid,categoryoptioncombocode,categoryoptioncombo)

tempfile1.3<-left_join(tempfile1.2,Host_results,by="categoryoptioncombo") %>%  rename (categoryOptionCombo=categoryoptioncombocode) %>%  mutate(attributeOptionCombo="cDGPF739ZZr")  %>% 
  mutate(dataelementuid=if_else(status==" Received (completed) an evidence-based intervention focused on preventing violence within the reporting period","e9eMQs1jUCB",if_else(
                                   status==" Received educational support to remain in, advance, and/or rematriculate in school within the reporting period","KqAes2sA33z",dataelementuid)))
tempfile1.3<-tempfile1.3 %>% mutate (categoryOptionCombo=if_else(is.na(categoryOptionCombo),"HllvX50cXC0",categoryOptionCombo)) 
tempfile1.3<-tempfile1.3 %>% mutate (dataelementdesc=if_else((categoryOptionCombo)=="HllvX50cXC0","Number of individual AGYW that have completed at least the DREAMS primary package of services/interventions at the time of reporting",dataelementdesc)) 
tempfile1.3<-tempfile1.3 %>% mutate (dataelementdesc=if_else((categoryOptionCombo)=="HllvX50cXC0","Number of individual AGYW that have completed at least the DREAMS primary package of services/interventions at the time of reporting",dataelementdesc)) 
tempfile1.3<-tempfile1.3 %>% mutate (dataelement=if_else((dataelementuid)=="e9eMQs1jUCB","AGYW_PREV (D, NoApp, ViolencePrevention): DREAMS): DREAMS",dataelement)) 
tempfile1.3<-tempfile1.3 %>% mutate (dataelement=if_else((dataelementuid)=="KqAes2sA33z","AGYW_PREV (D, NoApp, EducationSupport): DREAMS",dataelement)) 
tempfile1.3<-tempfile1.3 %>% mutate (dataset=if_else(is.na(dataset),"Host Country Results: DREAMS (USG)",dataset)) 

AGYW_Import_File<-tempfile1.3 %>%  select(district,catecombo,dataelementuid,dataelement,psnuuid,categoryOptionCombo,categoryoptioncombo,attributeOptionCombo,Value) %>%  mutate(period="2022Q4") %>%
  group_by(district,catecombo,dataelementuid,attributeOptionCombo,dataelement,psnuuid,categoryOptionCombo,categoryoptioncombo,period) %>%  summarise_at(vars(Value), sum, na.rm = TRUE) 

AGYW_DREAMS<-AGYW_Import_File%>% data.frame() %>% select(dataelementuid,period,psnuuid,categoryOptionCombo,attributeOptionCombo,Value)   %>% rename( Orgunit=psnuuid, dataElement = dataelementuid)

shell("taskkill /im EXCEL.exe /f /t")

write.xlsx(AGYW_Import_File,"AGYW_Prev_Review.xlsx")

write_csv(AGYW_DREAMS,"AGY_PREV.csv")
