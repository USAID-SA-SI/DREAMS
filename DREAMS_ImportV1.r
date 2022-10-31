# Title: AGYW_PREV MER Reporting Script
# Author: C. Trapence
# Purpose: Automating the process of Reporting AGYW_PREV for Inter-agency
# Date:2022-10-22
# Updated:2022:10:31 @ 11:18pm
#Load Required libraries
# Red text symbolizes comments

#######################################################################################################################
#  sources files used in the code include:                                                                            #
#              1) Data Export from CBMIS                                                                              #
#              2) Host Country Results DREAMS (USG) from DATIM Support                                                #
#              3) Data Exchange Organisation Units from DATIM Support                                                 #
#              4) Mechanisms from DATIM support                                                                       #
#######################################################################################################################

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

#'[Load Mechanisms,host country results as extracted from DATIM] 

DREAMS_Orgunits<-read.csv("Data Exchange Organisation Units.csv") %>%  rename(sub_district=orgunit_name,sub_districtuid=orgunit_internal_id)

#'[Mechanims names to get attributecombooption_id]

mechanisms <- read.csv("mechanisms.csv", stringsAsFactors=FALSE) %>% filter(ou=='South Africa') %>%  mutate(attributeOptionCombo="cDGPF739ZZr")

#'[This block Process DREAMS Data and correcting the District name from short names to full names]

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
district=="ec Alfred Nzo District Municipality"~"ec Alfred Nzo District Municipality"))  %>% mutate(old_sub_district=`sub-district`)

#'[DREAMS Data is entered at Level 6 of the DATIM org unit hierarchy,this code clean P CBMIS names to match DATIM Org unit hierarchy]

AGYW_PREV_raw<-AGYW_PREV_raw %>%  mutate(`sub-district`=case_when  (`sub-district`=="kz The Msunduzi Local Municipality"	~"kz Msunduzi Local Municipality",
           `sub-district`=="ec King Sabata Dalindyebo Health sub-District"	~"ec King Sabata Dalindyebo Local Municipality",
           `sub-district`=="gp Ekurhuleni East 1 Health sub-District"	~"gp Ekurhuleni East 1 Local Municipality",
           `sub-district`=="kz eThekwini Metropolitan Municipality Sub"	~"kz eThekwini Metropolitan Municipality Sub - DM EHS",
           `sub-district`=="lp Polokwane Local Municipality"	~"lp Polokwane Local Municipality EHP",
           `sub-district`=="kz Mfolozi Local Municipality"	~"kz uMfolozi Local Municipality",
           `sub-district`=="ec Ntabankulu Health sub-District"	~"ec Ntabankulu Local Municipality",
           `sub-district`=="ec Ingquza Hill Health sub-District"	~"ec Ingquza Hill Local Municipality",
           `sub-district`=="mp Albert Luthuli Local Municipality"	~"mp Chief Albert Luthuli Local Municipality",
           `sub-district`=="mp Pixley Ka Seme Local Municipality"	~"nc Pixley ka Seme District Municipality",
           `sub-district`=="ec Matatiele Health sub-District"	~"ec Matatiele Local Municipality",
           `sub-district`=="fs Maluti a Phofung Local Municipality"	~"fs Maluti-a-Phofung Local Municipality",
           `sub-district`=="gp Ekurhuleni East 2 Health sub-District"	~"gp Ekurhuleni East 2 Local Municipality",
           `sub-district`=="gp Tshwane 4 Health sub-District"	~"gp Tshwane 4 Local Municipality",
           `sub-district`=="ec Winnie Madikizela-Mandela Health Sub-District"	~"ec Mbizana Local Municipality",
           `sub-district`=="gp Tshwane 3 Health sub-District"	~"gp Tshwane 3 Local Municipality",
           `sub-district`=="gp Tshwane 6 Health sub-District"	~"gp Tshwane 6 Local Municipality",
           `sub-district`=="ec Umzimvubu Health sub-District"	~"ec Umzimvubu Local Municipality",
           `sub-district`=="gp Ekurhuleni South 1 Health sub-District"	~"gp Ekurhuleni South 1 Local Municipality",
           `sub-district`=="gp Ekurhuleni North 1 Health sub-District"	~"gp Ekurhuleni North 1 Local Municipality",
           `sub-district`=="gp Ekurhuleni South 2 Health sub-District"	~"gp Ekurhuleni South 2 Local Municipality",
           `sub-district`=="kz Mooi Mpofana Local Municipality"	~"kz Mpofana Local Municipality",
           `sub-district`=="gp Tshwane 2 Health sub-District"	~"gp Tshwane 2 Local Municipality",
           `sub-district`=="gp Tshwane 1 Health sub-District"	~"gp Tshwane 1 Local Municipality",
           `sub-district`=="gp Tshwane 5 Health sub-District"	~"gp Tshwane 5 Local Municipality",
           `sub-district`=="gp Tshwane 7 Health sub-District"	~"gp Tshwane 7 Local Municipality",
           `sub-district`=="kz uMhlathuze Local Municipality"	~"kz City of uMhlathuze Local Municipality",
           `sub-district`=="ec Port St Johns Health sub-District"	~"ec Port St Johns Local Municipality",
           `sub-district`	=="mp Mbombela Local Municipality"	~"mp City of Mbombela Local Municipality")) %>%  mutate(`sub-district`=if_else(is.na(`sub-district`),old_sub_district,`sub-district`)) %>% 
  select(-old_sub_district) %>%  rename (sub_district=`sub-district`)
#'[This block transform  short names to align with DATIM and NDOH District Names;When CBMIS Uses short names for Districts please un-comment and use the code block below]

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

#'[This block Removes all the Inactive beneficiaries from the CBMIS data set]

AGYW_PREV1.0<-sqldf('select * from AGYW_PREV_raw where status  Not like "%Inactive%"')

#'[Remove the numbering from status column to create a matching category option combo]
AGYW_PREV1.0 <- AGYW_PREV1.0 %>%  select(province,PSNU, sex,district,sub_district ,`first partner`,agecat,status,disaggregation,unique_count) %>% filter(unique_count!=0) %>% 
  mutate(status=gsub("[1-9.*]", "", status))  %>% mutate(status=gsub("[1-9*]", "", status))  %>% rename (mech_name=`first partner`,Value=unique_count) 
  
#'[Adding in Mechanism ID's using Implementing Partners names]
AGYW_PREV1.1<-AGYW_PREV1.0%>% mutate(disaggregation=gsub("[a-z.*]","",disaggregation))  %>% mutate(time="Months in DREAMS")%>%  select (-district) %>% rename(district=PSNU) %>% 
mutate(mech_code=as.integer(case_when(mech_name=="Shout It Now"~"81891",mech_name=="TB/HIV Care"~"83013",mech_name=="NACOSA - GBV (80008)"~"80008",mech_name=="NACOSA - OVC (80002)"~"80002",
        mech_name=="FHI 360 DREAMS"~"82199",mech_name=="Wits Reproductive Health& HIV Institute"~"80007",mech_name=="FHI 360 OVC"~"14295",mech_name=="HIVSA"~"70307",mech_name=="Mothers to Mothers (M2M)"~"80004",
        mech_name=="Centre for Communication Impact (CCI)"~ "17537",mech_name=="Education Development Center (EDC)"~"160611",mech_name=="Pact"~"14631") )) 

AGYW_PREV1.2<-AGYW_PREV1.1 %>% mutate(disaggregation=paste(disaggregation ,time,sep=" ")) %>% select(-time)

tempfile1 <- left_join( AGYW_PREV1.2,DREAMS_Orgunits, by=c("sub_district"))

tempfile1.1<-tempfile1  %>%  filter(Value!=0) %>% 
  mutate(categoryoptioncombo=case_when(disaggregation!="/ Months in DREAMS"~paste(agecat,sex,disaggregation,status,sep=", "))) %>% 
  
  mutate(categoryoptioncombo2=case_when(disaggregation=="/ Months in DREAMS"~paste(agecat,sex,status,sep=", "))) %>% 
  mutate(categoryoptioncombo_final=if_else(disaggregation=="/ Months in DREAMS",categoryoptioncombo2,categoryoptioncombo)) %>% 
  select(-categoryoptioncombo2,-categoryoptioncombo ) %>%  
  rename (categoryoptioncombo=categoryoptioncombo_final)

#'[House keeping on AGYW Category option coding]
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

AGYW_Import_File<-tempfile1.3 %>%  select(district,sub_district,sub_districtuid,catecombo,dataelementuid,dataelement,categoryOptionCombo,categoryoptioncombo,attributeOptionCombo,Value) %>%  mutate(period="2022Q4") %>%
  group_by(district,sub_district,sub_districtuid,catecombo,dataelementuid,attributeOptionCombo,dataelement,categoryOptionCombo,categoryoptioncombo,period) %>%  summarise_at(vars(Value), sum, na.rm = TRUE) 

AGYW_DREAMS<-AGYW_Import_File%>% data.frame() %>% select(dataelementuid,sub_districtuid,period,categoryOptionCombo,attributeOptionCombo,Value)   %>% rename( Orgunit=sub_districtuid, dataElement = dataelementuid)

#'[Warning!] *The code below will close any opened excel document on your computer and make sure you have saved all your excel documents before running it*

shell("taskkill /im EXCEL.exe /f /t")

#'[This output "AGYW_Prev_Review.xlsx" is to assist in checking the numbers against CBMIS results]
write.xlsx(AGYW_Import_File,"AGYW_Prev_Review.xlsx")

#'[Final import output below]

write_csv(AGYW_DREAMS,"AGYW_PREV.csv")
