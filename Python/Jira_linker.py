from jira import JIRA
from datetime import datetime

now = datetime.now()
 
optionsJit = {'server': 'https://jit.o3.ru', 'proxies': 'proxy_dict'}
optionsJira = {'server': 'https://jira.o3.ru', 'proxies': 'proxy_dict'}
jit = JIRA(options=optionsJit, token_auth='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')           #токен для JIT
jira = JIRA(options=optionsJira, token_auth='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')         #токен для JIRA
jql = 'resolved >= startOfday() AND resolved <= endOfday() AND assignee = currentuser()'                
jira_search = jira.search_issues(jql, maxResults = 37, fields ='key')
        
ticket1 = {"project":{"key":"MWORK"},
         "summary":f'Профилактические работы в выходной, Дмитриев Е.Г., {now.strftime("%d.%m.%Y")}',                                                       
         "issuetype":{"name":"Task"},
         "description":"Работа в выходные / Разбор бэклога",
         "components": [{"name":"Логистика"}],
         "customfield_10905":"Дмитриев Евгений Геннадьевич",                        #ваше ФИО
         "customfield_11800":f'{now.strftime("%Y-%m-%d")}T10:00:00.00+0300',
         "customfield_12708":f'{now.strftime("%Y-%m-%d")}T18:00:00.00+0300',
         "assignee":{
         "name":"evgdmitriev",                                                    #ваш логин
         "key":"evgdmitriev",                                                     #ваш логин
         "emailAddress":"evgdmitriev@ozon.ru"}                                   #ваш e-mail
         }
 
 
t1 = jit.create_issue(fields=ticket1)
 
for issue in jira_search:  

    source_issue = jira.issue(f'{issue}')

    target_issue_url = f'https://jit.o3.ru/browse/{t1}'

    remote_link_jira = {
            "url": target_issue_url,
            "title": f'{t1.key}',
            "summary": f'{t1.fields.summary}',
            "icon": {                                         
            "url16x16":"https://jit.o3.ru//secure/viewavatar?size=xsmall&avatarId=16509&avatarType=issuetype",    
            "title":"Incident Ticket"     
        },
            "status": {
                "resolved": True,
                "icon": {
                    "url16x16": "https://jira.o3.ru/images/icons/statuses/closed.png",
                    "title": "Обработано",
                }
            }
            
    }

    jira.add_remote_link(issue=source_issue, destination=remote_link_jira, relationship='is mentioned by')

    target_issue = jit.issue(f'{t1}')
    source_issue_url = f'https://jira.o3.ru/browse/{issue}'

    remote_link_jit = {

            "url": source_issue_url,
            "title": f'{source_issue.key}',
            "summary": f'{source_issue.fields.summary}',
            "icon": {                                         
            "url16x16":"https://jira.o3.ru/secure/viewavatar?size=xsmall&avatarId=16500&avatarType=issuetype",    
            "title":"Task Ticket"     
            },
            "status": {
                "resolved": True,
                "icon": {
                    "url16x16": "https://jira.o3.ru/images/icons/statuses/closed.png",
                    "title": "Обработано",
                }
                    }

    }

    jit.add_remote_link(issue=target_issue, destination=remote_link_jit, relationship='mentions')