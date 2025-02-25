# Healthcare-DataPipleine

This is a Health and Genomics Data Pipeline that gives out personalized healthcare recommendations based on data sourced from the interent. I primarily used AWS Services like S3, Glue, Redshift Serverless, Cloudwatch, IAM and billing & Cost Management. I worked on this project to primarily build my portfolio as an Analyst. 

The data in this project is divided in two parts: Lifestyle data and Genomics Data. Lifestyle data comprises of several health realted factors like nutrition(Calcium intake/day, fibre intake/day, protein intake/day, etc.), alchohol and tobacco consumption. I used several sources to scrap data for these factors(Please refer sources at the end). For Genomcis data, I only used the 1000 genomes project website which contains information about genomic predisposition for every country. The aim of this project is to combine these two factors and analyze them to see if there's any pattern revelead. 

I used Python to load and clean the data initially on Google Colab and then moved on to using AWS S3 for storing my data securely. I then used AWS Glue to write ETL Scripts to that merged these files together. I had to troubleshoot my script mutliple times, for which i primarily used Cloudwatch logs. Since the volume of the data was huge and the genomcis data had a seperate data type(variant call format- .vcf file), I had to run the scripts seperately for Lifestyle data first and then merge the genomics data later on. Once that was done, I loaded the data onto Redshift serverless. 


https://drive.google.com/drive/folders/1BknntyIu3Upo52jfStgmjO7-uhGkde4u?usp=sharing

![image](https://github.com/user-attachments/assets/bbb74039-bf1b-4abd-ae84-53609cebaf44)
