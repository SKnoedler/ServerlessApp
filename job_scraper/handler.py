import boto3
import json
import requests
from bs4 import BeautifulSoup
import datetime
import uuid

def scrape(event, context):
    table = 'SteffenJobs'
    job_scrape(table)

def job_scrape(table):
    page = requests.get("https://www.stepstone.de/5/ergebnisliste.html?stf=freeText&ns=1&companyid=0&sourceofthesearchfield=resultlistpage%3Ageneral&qs=%5B%5D&ke=data&ra=30&suid=f1446ddf-231b-41da-a9b5-eec3f105ba1d&fci=423362&ag=age_1&li=100&ob=date&action=sort_publish")
    soup = BeautifulSoup(page.text, "html.parser")
    content = soup.select(".col-lg-9")

    for posting in content:
        title = posting.find_all("h2")
        company = posting.find_all("div", attrs={"class":"styled__CompanyName-iq4jvn-0 gakwWs"})

    # creating new empy lists to store information
    ID = []
    job_title = []
    company_name = []
    date1 = []

    # we only want the text being stored in our lists
    for x in range(len(title)):
        ID.append(str(uuid.uuid4()))
        date1.append(str(datetime.datetime.now()))
        job_title.append(title[x].text)

    for x in range(len(company)):
        company_name.append(company[x].text)

    data = [{'ID': ID, 'date': date1, 'Job': job_title, 'Company': company_name} for ID,date1,job_title,company_name in zip(ID,date1,job_title,company_name)]



    client = boto3.resource('dynamodb')

    # this will search for dynamoDB table
    # your table name may be different
    table = client.Table(table)

    with table.batch_writer() as batch:
        for r in data:
            batch.put_item(Item=r)
