import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import json
import re
import pymongo
import random

headers = {
  'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
  'accept-language': 'en-US,en;q=0.9,hi;q=0.8',
  'priority': 'u=0, i',
  'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Windows"',
  'sec-fetch-dest': 'document',
  'sec-fetch-mode': 'navigate',
  'sec-fetch-site': 'none',
  'sec-fetch-user': '?1',
  'upgrade-insecure-requests': '1',
  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
  'Cookie': 'aaid=259f600b-cc82-4765-9eac-e519eecb5e51; _ga=GA1.1.166192967.1743509735; _dox_track%3Ainitial_url=eyJfcmFpbHMiOnsibWVzc2FnZSI6Iklsd2lhSFIwY0hNNkx5OTNkM2N1Wkc5NGFXMXBkSGt1WTI5dEwzQjFZaTloYm01bExXdHBjbXN0YldRdFlXSmhNbUV3WWpSY0lpST0iLCJleHAiOiIyMDI1LTA1LTA3VDExOjQ4OjA3LjYxOFoiLCJwdXIiOiJjb29raWUuX2RveF90cmFjazppbml0aWFsX3VybCJ9fQ%3D%3D--f23319859a700870c9c8504e49d7b79b75831f8e; _dox_track%3Areferrer=eyJfcmFpbHMiOnsibWVzc2FnZSI6IkltNTFiR3dpIiwiZXhwIjoiMjAyNS0wNS0wN1QxMTo0ODowNy42MThaIiwicHVyIjoiY29va2llLl9kb3hfdHJhY2s6cmVmZXJyZXIifX0%3D--ed6561142c9999ce4be222166537a7457b2ebd91; _dox_track%3Auser_agent=eyJfcmFpbHMiOnsibWVzc2FnZSI6Iklsd2lUVzk2YVd4c1lTODFMakFnS0ZkcGJtUnZkM01nVGxRZ01UQXVNRHNnVjJsdU5qUTdJSGcyTkNrZ1FYQndiR1ZYWldKTGFYUXZOVE0zTGpNMklDaExTRlJOVEN3Z2JHbHJaU0JIWldOcmJ5a2dRMmh5YjIxbEx6RXpOUzR3TGpBdU1DQlRZV1poY21rdk5UTTNMak0yWENJaSIsImV4cCI6IjIwMjUtMDUtMDdUMTE6NDg6MDcuNjE4WiIsInB1ciI6ImNvb2tpZS5fZG94X3RyYWNrOnVzZXJfYWdlbnQifX0%3D--cbe1f8286fef6b65839343b3b784ac1fab0bd8ca; _dox_track%3Astarted_at=eyJfcmFpbHMiOnsibWVzc2FnZSI6Iklsd2lNakF5TlMwd05TMHdOVlF4TURvd01Ub3pNaTQwTWpGYVhDSWkiLCJleHAiOiIyMDI1LTA1LTA3VDExOjQ4OjA3LjYxOFoiLCJwdXIiOiJjb29raWUuX2RveF90cmFjazpzdGFydGVkX2F0In19--64c19fc3658484462a6a49ff323596e9f65e8159; _dox_track%3Acampaign=eyJfcmFpbHMiOnsibWVzc2FnZSI6IkltNTFiR3dpIiwiZXhwIjoiMjAyNS0wNS0wN1QxMTo0ODowNy42MThaIiwicHVyIjoiY29va2llLl9kb3hfdHJhY2s6Y2FtcGFpZ24ifX0%3D--4fd8b20e4747ccad12ae20fd2b84b075293243f9; _dox_track%3Aemail=eyJfcmFpbHMiOnsibWVzc2FnZSI6IkltNTFiR3dpIiwiZXhwIjoiMjAyNS0wNS0wN1QxMTo0ODowNy42MThaIiwicHVyIjoiY29va2llLl9kb3hfdHJhY2s6ZW1haWwifX0%3D--c604d814b3e76d891f5835d04d99012d09af6b7b; _dox_track%3Aplatform=eyJfcmFpbHMiOnsibWVzc2FnZSI6Iklsd2lkMlZpWENJaSIsImV4cCI6IjIwMjUtMDUtMDdUMTE6NDg6MDcuNjE4WiIsInB1ciI6ImNvb2tpZS5fZG94X3RyYWNrOnBsYXRmb3JtIn19--19e3ba739f81adcf79b4601360d2d1be4af09322; _dox_track%3Aapp_group=eyJfcmFpbHMiOnsibWVzc2FnZSI6Iklsd2laRzk0YVcxcGRIbGNJaUk9IiwiZXhwIjoiMjAyNS0wNS0wN1QxMTo0ODowNy42MThaIiwicHVyIjoiY29va2llLl9kb3hfdHJhY2s6YXBwX2dyb3VwIn19--5911e6c73362abc6a91c13105db644d32225636c; _dox_track%3Alock_campaign=eyJfcmFpbHMiOnsibWVzc2FnZSI6IkltWmhiSE5sSWc9PSIsImV4cCI6IjIwMjUtMDUtMDdUMTE6NDg6MDcuNjE4WiIsInB1ciI6ImNvb2tpZS5fZG94X3RyYWNrOmxvY2tfY2FtcGFpZ24ifX0%3D--c022382b04c10aadacc058157f0f79d03cd4f6e3; _dox_track%3Aemail_uuid=eyJfcmFpbHMiOnsibWVzc2FnZSI6IkltNTFiR3dpIiwiZXhwIjoiMjAyNS0wNS0wN1QxMTo0ODowNy42MThaIiwicHVyIjoiY29va2llLl9kb3hfdHJhY2s6ZW1haWxfdXVpZCJ9fQ%3D%3D--f2dacbb037896cb9e956566c388dd5b3e3405830; ajs_anonymous_id=259f600b-cc82-4765-9eac-e519eecb5e51; _ga_MQZ23K5YFB=GS1.1.1746443294.9.1.1746445958.0.0.0; dox_analytics_campaign=eyJtZWRpdW0iOiJkaXJlY3QifQ==; _dox_track%3Aapp_group=eyJfcmFpbHMiOnsibWVzc2FnZSI6Iklsd2laRzk0YVcxcGRIbGNJaUk9IiwiZXhwIjoiMjAyNS0wNS0wN1QxNToxNzoxOS45MDdaIiwicHVyIjoiY29va2llLl9kb3hfdHJhY2s6YXBwX2dyb3VwIn19--04fbd68b74bd0a5a1544c8a683fa8c9245769f3b; _dox_track%3Acampaign=eyJfcmFpbHMiOnsibWVzc2FnZSI6IkltNTFiR3dpIiwiZXhwIjoiMjAyNS0wNS0wN1QxNToxNzoxOS45MDdaIiwicHVyIjoiY29va2llLl9kb3hfdHJhY2s6Y2FtcGFpZ24ifX0%3D--403f1fa465921bd942abfd1b3570b6eec6c6668c; _dox_track%3Aemail=eyJfcmFpbHMiOnsibWVzc2FnZSI6IkltNTFiR3dpIiwiZXhwIjoiMjAyNS0wNS0wN1QxNToxNzoxOS45MDdaIiwicHVyIjoiY29va2llLl9kb3hfdHJhY2s6ZW1haWwifX0%3D--46393835796f389273eaac18ffda078553e888df; _dox_track%3Aemail_uuid=eyJfcmFpbHMiOnsibWVzc2FnZSI6IkltNTFiR3dpIiwiZXhwIjoiMjAyNS0wNS0wN1QxNToxNzoxOS45MDdaIiwicHVyIjoiY29va2llLl9kb3hfdHJhY2s6ZW1haWxfdXVpZCJ9fQ%3D%3D--3f24a89ca0c878eff7903fa23a961cb55b53e1fd; _dox_track%3Ainitial_url=eyJfcmFpbHMiOnsibWVzc2FnZSI6Iklsd2lhSFIwY0hNNkx5OTNkM2N1Wkc5NGFXMXBkSGt1WTI5dEwzQjFZaTloYm01bExXdHBjbXN0YldRdFlXSmhNbUV3WWpSY0lpST0iLCJleHAiOiIyMDI1LTA1LTA3VDE1OjE3OjE5LjkwN1oiLCJwdXIiOiJjb29raWUuX2RveF90cmFjazppbml0aWFsX3VybCJ9fQ%3D%3D--404fe8262a5112086f589b2060a58e48c07f4dcb; _dox_track%3Alock_campaign=eyJfcmFpbHMiOnsibWVzc2FnZSI6IkltWmhiSE5sSWc9PSIsImV4cCI6IjIwMjUtMDUtMDdUMTU6MTc6MTkuOTA3WiIsInB1ciI6ImNvb2tpZS5fZG94X3RyYWNrOmxvY2tfY2FtcGFpZ24ifX0%3D--0d8dc89116e5dc5c794759e31e806f7996240048; _dox_track%3Aplatform=eyJfcmFpbHMiOnsibWVzc2FnZSI6Iklsd2lkMlZpWENJaSIsImV4cCI6IjIwMjUtMDUtMDdUMTU6MTc6MTkuOTA3WiIsInB1ciI6ImNvb2tpZS5fZG94X3RyYWNrOnBsYXRmb3JtIn19--1d536754625c034f3a0015141b1db90173955b8d; _dox_track%3Areferrer=eyJfcmFpbHMiOnsibWVzc2FnZSI6IkltNTFiR3dpIiwiZXhwIjoiMjAyNS0wNS0wN1QxNToxNzoxOS45MDdaIiwicHVyIjoiY29va2llLl9kb3hfdHJhY2s6cmVmZXJyZXIifX0%3D--a846e3c6d34a667e8d74bb088f6220dfcacb92b4; _dox_track%3Astarted_at=eyJfcmFpbHMiOnsibWVzc2FnZSI6Iklsd2lNakF5TlMwd05TMHdOVlF4TURvd01Ub3pNaTQwTWpGYVhDSWkiLCJleHAiOiIyMDI1LTA1LTA3VDE1OjE3OjE5LjkwN1oiLCJwdXIiOiJjb29raWUuX2RveF90cmFjazpzdGFydGVkX2F0In19--05f29ff387585b3169625856510658974e22d01d; _dox_track%3Auser_agent=eyJfcmFpbHMiOnsibWVzc2FnZSI6Iklsd2lUVzk2YVd4c1lTODFMakFnS0ZkcGJtUnZkM01nVGxRZ01UQXVNRHNnVjJsdU5qUTdJSGcyTkNrZ1FYQndiR1ZYWldKTGFYUXZOVE0zTGpNMklDaExTRlJOVEN3Z2JHbHJaU0JIWldOcmJ5a2dRMmh5YjIxbEx6RXpOUzR3TGpBdU1DQlRZV1poY21rdk5UTTNMak0yWENJaSIsImV4cCI6IjIwMjUtMDUtMDdUMTU6MTc6MTkuOTA3WiIsInB1ciI6ImNvb2tpZS5fZG94X3RyYWNrOnVzZXJfYWdlbnQifX0%3D--5540162313ad4dce9b0a50efdf9ee504fbfd7bd2'
}

proxies = {
    "http": "http://202.5.54.70:4145",
}

client = pymongo.MongoClient("mongodb+srv://")
db = client["npi_directory"]
collection = db["doximity"]

    
def get_text_or_blank(tag):
    return tag.get_text(strip=True) if tag else ''

def get_attr_or_blank(tag, attr):
    return tag[attr] if tag and tag.has_attr(attr) else ''
    
def crawler(npi, source_url):
    print(f"Crawling url: {source_url}")
    
    # response = requests.request("GET", source_url, headers=headers, proxies=proxies, timeout=10)
    # time.sleep(5)
    try:
        response = requests.get(source_url, headers=headers, proxies=proxies, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return
    time.sleep(2)
    html = response.text 
    soup = BeautifulSoup(html, 'html.parser')

    doctor_name = ' '.join([get_text_or_blank(span) for span in soup.select('h1.profile-overview-user-name span')])
    speciality_tag = soup.find('a', class_='profile-overview-subheading-link')
    speciality = get_text_or_blank(speciality_tag)
    speciality_url = get_attr_or_blank(speciality_tag, 'href')
    profile_img_tag = soup.select_one('div.profile-overview-photo-container img.profile-overview-photo')
    profile_img_url = get_attr_or_blank(profile_img_tag, 'src')

    street = get_attr_or_blank(soup.find('meta', itemprop='streetAddress'), 'content')
    city_tag = soup.find('span', itemprop='addressLocality')
    city_link = city_tag.find('a') if city_tag else None
    city = get_text_or_blank(city_link)
    city_url = get_attr_or_blank(city_link, 'href')

    state_tag = soup.find('span', itemprop='addressRegion')
    state_link = state_tag.find('a') if state_tag else None
    state = get_text_or_blank(state_link)
    state_url = get_attr_or_blank(state_link, 'href')

    address = f"{street}, {city}, {state}".strip(', ')
    position = get_text_or_blank(soup.select_one('p.profile-overview-info-line'))
    job_title = get_text_or_blank(soup.find('p', itemprop='jobTitle'))

    profile_card_heading = get_text_or_blank(soup.find('h2', class_='profile-card-heading'))

    contact_address = soup.select_one('li[data-sel-address] .profile-overview-contact-list-item-text-container')
    full_address = ', '.join([line.get_text(strip=True) for line in contact_address.find_all('span')]) if contact_address else ''

    phone = soup.select_one('li[itemprop="telephone"]')
    dr_phone = [line.get_text(strip=True) for line in phone.find_all('span')][-1] if phone and phone.find_all('span') else ''

    fax = soup.select_one('li[itemprop="faxNumber"]')
    dr_fax = [line.get_text(strip=True) for line in fax.find_all('span')][-1] if fax and fax.find_all('span') else ''

    summary_section = soup.select_one('section.profile-section.summary-info .profile-summary-content')
    summary_text = summary_section.get_text(strip=True) if summary_section else ""

    experties_section = soup.select_one('section.profile-section.skills-info .profile-sectioned-list')
    clinical_experties = experties_section.get_text(strip=True) if experties_section else ""

    work_experience = []
    work_section = soup.select('ul.employments > li')
    for item in work_section:
        org_tag = item.select_one('[itemprop="name"]')
        title_tag = item.select_one('[itemprop="title"]')
        period_tag = item.select_one('[itemprop="employmentPeriod"]')

        exp_entry = {
            "organization": org_tag.get_text(strip=True) if org_tag else "",
            "title": title_tag.get_text(strip=True) if title_tag else "",
            "employment_period": period_tag.get_text(strip=True) if period_tag else ""
        }
        work_experience.append(exp_entry)


    education_info = []
    for li in soup.select('section.education-info ul.training li'):
        institute = get_text_or_blank(li.find('span', itemprop='name'))
        degree = get_text_or_blank(li.find('span', class_='br'))
        img = get_attr_or_blank(li.find('img'), 'src')
        education_info.append({'institute': institute, 'degree': degree, 'image_url': img})

    certifications = []
    for li in soup.select('section.certification-info ul li'):
        name = get_text_or_blank(li.find('span', class_='black'))
        detail = get_text_or_blank(li.find('span', class_='br'))
        img = get_attr_or_blank(li.find('img'), 'src')
        certifications.append({'name': name, 'detail': detail, 'image_url': img})

    awards = []
    for li in soup.select('section.award-info ul li.show_more_hidden'):
        name = get_text_or_blank(li.find('span', class_='black'))
        year = get_text_or_blank(li.find('span', class_='br'))
        awards.append({'name': name, 'description': year})

    journal_articles = []
    for li in soup.select('ul.sec_journal_articles li.show_more_hidden'):
        title = get_text_or_blank(li.find('span', class_='black'))
        authors = get_text_or_blank(li.find('span', class_='br'))
        journal_articles.append({'title': title, 'description': authors})

    abstracts_posters = []
    for li in soup.select('ul.sec_abstracts li.show_more_hidden'):
        title = get_text_or_blank(li.find('span', class_='black'))
        authors = get_text_or_blank(li.find('span', class_='br'))
        abstracts_posters.append({'title': title, 'description': authors})

    lectures = []
    for li in soup.select('ul.sec_lectures li.show_more_hidden'):
        title = get_text_or_blank(li.find('span', class_='black'))
        authors = get_text_or_blank(li.find('span', class_='br'))
        lectures.append({'title': title, 'description': authors})

    memberships = []
    for li in soup.select('section.membership-info ul.profile-sectioned-list li'):
        org = get_text_or_blank(li.find('span', class_='black'))
        type_ = get_text_or_blank(li.find('span', class_='br'))
        memberships.append({'organization': org, 'membership_type': type_})

    insurances = []
    for div in soup.select('section.insurers div.col-1-2'):
        insurances += [x.strip() for x in div.decode_contents().split('<br/>') if x.strip()]

    clinical_trials = []
    for li in soup.select('section.trials-info ul.profile-sectioned-list li.show_more_hidden'):
        title = get_text_or_blank(li.find('a', class_='black'))
        link = get_attr_or_blank(li.find('a', class_='black'), 'href')
        start_date = get_text_or_blank(li.find('span', class_='br')).replace('Start of enrollment:', '').strip()

        status = ''
        phases = []
        for p in li.select('div.tag-list p.tag-list-item'):
            text = get_text_or_blank(p)
            if 'Phase' in text or 'PHASE' in text:
                phases.append(text)
            elif not status:
                status = text

        roles = ''
        roles_block = li.select_one('p strong.profile-clinical-trial-roles')
        if roles_block and roles_block.next_sibling:
            roles = roles_block.next_sibling.strip()

        clinical_trials.append({
            'title': title,
            'link': link,
            'start_of_enrollment': start_date,
            'status': status,
            'phase': ', '.join(phases) if phases else '',
            'roles': roles
        })

    pubmed_articles = []

    for li in soup.select('ul.sec_pubmed_articles li.show_more_hidden'):
        title = get_text_or_blank(li.find('div', class_='list-section-publication-title'))
        link = get_attr_or_blank(li.find('a', class_='black'), 'href')

        span = li.find('span', class_='br')
        if span:
            # Extract raw HTML content so we can split by <br/> or <br>
            parts = re.split(r'<br\s*/?>', span.decode_contents())
            authors = parts[0].strip() if len(parts) > 0 else ""
            institute_line = parts[1].strip() if len(parts) > 1 else ""

            # Extract the date from the institute line
            match = re.search(r'(\d{4}-\d{2}-\d{2})$', institute_line)
            date = match.group(1) if match else ""

            # Remove the date from the institute string
            institute = institute_line.replace(date, '').rstrip('.').strip() if date else institute_line
        else:
            authors = ""
            institute = ""
            date = ""

        pubmed_articles.append({
            'title': title,
            'link': link,
            'authors': authors,
            'institute': institute,
            'date': date
        })
        
    press_mentions = []
    for li in soup.select('section.press-info ul.profile-sectioned-list:nth-of-type(1) li'):
        title = get_text_or_blank(li.find('span', class_='black'))
        date = get_text_or_blank(li.find('span', class_='br'))
        image_url = get_attr_or_blank(li.find('img'), 'src')
        press_mentions.append({
            'title': title,
            'description': date,
            'image_url': image_url
        })
    
    news_attachments = []
    for item in soup.select('div.attachments-inner'):
        title = get_text_or_blank(item.select_one('.attachment_text .profile-attachment-title'))
        description = get_text_or_blank(item.select_one('.attachment_text .br'))
        image_url = get_attr_or_blank(item.select_one('.attachment_thumbnail img'), 'src')
        link_tag = item.select_one('.attachment_thumbnail a')
        href = get_attr_or_blank(link_tag, 'href') if link_tag else ''
        iframe = item.select_one('iframe.embedly-embed')
        news_attachments.append({
            'title': title,
            'description': description,
            'image_url': image_url,
            'link': href
        })

    grant_support = []
    grant_items = soup.select('section.grant-info ul.profile-sectioned-list li')
    for item in grant_items:
        grant_title = get_text_or_blank(item.select_one('span.black'))
        grant_org = get_text_or_blank(item.select('span.br')[0]) if len(item.select('span.br')) > 0 else ''
        grant_year = get_text_or_blank(item.select('span.br')[1]) if len(item.select('span.br')) > 1 else ''
        
        grant_support.append({
            'title': grant_title,
            'organization': grant_org,
            'years': grant_year
        })

    committees = []
    committee_items = soup.select('section.committee-info ul.profile-sectioned-list li')
    for item in committee_items:
        role = get_text_or_blank(item.select_one('span.black'))
        years = get_text_or_blank(item.select_one('span.br'))
        committees.append({
            'role': role,
            'years': years
        })

    research_history = []
    research_items = soup.select('section.research-info ul.profile-sectioned-list li')
    for item in research_items:
        spans = item.select('span')
        entry_research = {
            'title': get_text_or_blank(spans[0]) if len(spans) > 0 else '',
            'link': get_text_or_blank(spans[1]) if len(spans) > 1 and spans[1].text.startswith('http') else '',
            'years': get_text_or_blank(spans[-1]) if len(spans) > 2 else ''
        }
        research_history.append(entry_research)

    external_links = []
    link_items = soup.select('section.link-info ul.profile-sectioned-list li')
    for item in link_items:
        link_tag = item.select_one('a')
        span = item.select_one('span.br')
        if link_tag and span:
            external_links.append({
                'title': link_tag.get_text(strip=True),
                'url': link_tag['href']
            })
    
    authored_content = []
    authored_items = soup.select('section.non-journal-media ul.profile-sectioned-list li')

    for item in authored_items:
        if 'showmore' in item.get('class', []):
            continue
        title_tag = item.select_one('span.black')
        date_tag = item.select_one('span.br')
        if title_tag and date_tag:
            authored_content.append({
                'title': title_tag.get_text(strip=True),
                'date': date_tag.get_text(strip=True)
            })

    languages = None
    language_items = soup.select('section.language-info ul.profile-sectioned-list li')
    for item in language_items:
        languages = item.get_text(strip=True)

    books_chapters = []

    book_section = soup.select('ul.sec_book_chapters > li.show_more_hidden')
    for book in book_section:
        entry = {
            "title": "",
            "chapter_name": "",
            "author": "",
            "editor": "",
            "edition": "",
            "year": ""
        }

        spans = book.select('.list-section-authors span.black, .list-section-authors.br')

        for span in spans:
            text = span.get_text(strip=True)

            if not text:
                continue

            if not entry["title"]:
                entry["title"] = text

            elif text.startswith("Chapter:"):
                entry["chapter_name"] = text.replace("Chapter:", "").strip()

            elif "Editors:" in text:
                entry["editor"] = text.replace("Editors:", "").strip()

            elif "Edition" in text:
                if "," in text:
                    edition_parts = text.split(",", 1)
                    entry["edition"] = edition_parts[0].replace("Edition", "").strip()
                    entry["year"] = edition_parts[1].strip()
                else:
                    entry["edition"] = text.replace("Edition", "").strip()

            elif text.isdigit():
                entry["year"] = text

            elif not entry["author"]:
                entry["author"] = text

        books_chapters.append(entry)
        
    other_publications = []
    other_section = soup.select('ul.sec_other > li.show_more_hidden')
    for item in other_section:
        title_tag = item.select_one('.list-section-title-strong span.black')
        metadata_tag = item.select_one('span.br')

        entry = {
            "title": title_tag.get_text(strip=True) if title_tag else "",
            "description": metadata_tag.get_text(strip=True) if metadata_tag else ""
        }
        other_publications.append(entry)

    authored_content = []
    authored_items = soup.select('section.non-journal-media ul.profile-sectioned-list > li')
    for item in authored_items:
        title_tag = item.select_one('span.black')
        date_tag = item.select_one('span.br')

        if title_tag and date_tag:
            entry = {
                "title": title_tag.get_text(strip=True),
                "date": date_tag.get_text(strip=True)
            }
            authored_content.append(entry)

    industry_relationships = []
    industry_section = soup.select('section.industry-info ul.profile-sectioned-list > li')
    for item in industry_section:
        spans = item.select('span')
        industry_entry = {
            "title": spans[0].get_text(strip=True) if len(spans) > 0 else "",
            "description": spans[1].get_text(strip=True) if len(spans) > 1 else "",
            "period": spans[3].get_text(strip=True) if len(spans) > 3 else ""
        }
        industry_relationships.append(industry_entry)
        
    meta_tag = soup.find('meta', attrs={'property': 'og:url'})
    doximity_url = meta_tag['content'] if meta_tag else ""



    doctor_info = {
        "npi": str(npi),
        "source_url": source_url,
        "doximity_url": doximity_url,
        "data": {
            "name": doctor_name,
            "speciality": speciality,
            "street": street,
            "city": city,
            "state": state,
            "full_address": full_address,
            "address_url": f"https://www.doximity.com{city_url}" if city_url else '',
            "profile_img_url": profile_img_url,
            "phone": dr_phone,
            "fax": dr_fax,
            "speciality_info": position,
            "about": job_title.replace('\n', ' '),
            "languages": languages,
            "profile_card_heading": profile_card_heading.replace('\n', ' '),
            "summary": summary_text,
            "clinical_experties": clinical_experties,
            "work_experience": work_experience,
            "education_info": education_info,
            "certifications": certifications,
            "awards": awards,
            "pubmed_articles": pubmed_articles,
            "journal_articles": journal_articles,
            "others": other_publications,
            "press_mentions": press_mentions,
            "abstracts_posters": abstracts_posters,
            "lectures": lectures,
            "memberships": memberships,
            "insurances": insurances,
            "clinical_trials": clinical_trials,
            "news_attachments": news_attachments,
            "grant_support": grant_support,
            "committees": committees,
            "research_history": research_history,
            "external_links": external_links,
            "authored_content": authored_content,
            "books_chapters": books_chapters,
            "authored_content": authored_content,
            "industry_relationships": industry_relationships
        }
    }

    print(f"Store npi in mongo: {npi}")
    # print(doctor_info)
    collection.insert_one(doctor_info)


def main():
    df = pd.read_csv('chunk_1.csv')
    for _, row in df.iterrows():
        npi = row['npi']
        source_url = row['doximityurl']
        is_exist = collection.find_one({"npi": str(npi)})
        if is_exist is None:
            crawler(npi, source_url)
            rand = random.randint(4, 6)
            time.sleep(rand)

main()
