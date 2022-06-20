import asyncio
import random
import re
import time
import urllib.parse
from dataclasses import dataclass
from typing import Optional
import json

import bs4
import httpx
import requests_html
import requests
from asgiref.sync import sync_to_async
from requests.exceptions import ConnectionError
import config

   
def print_args(func):
    def wrapper(*args, **kwargs):
        print(f'{args} {kwargs}')
        return_value = func(*args, **kwargs)
        return return_value
    return wrapper

def is_website_exist_in_db(website: str):
    return json.loads(requests.post(f'{config.PROTOCOL}://{config.IP}:{config.PORT}/parser/is-website-exist/', data=json.dumps({'website': website})).text)['status']

def get_any_query():
    return json.loads(requests.get(f'{config.PROTOCOL}://{config.IP}:{config.PORT}/parser/get-any-query/').text)['result']

def write_to_site_db(database: list):
    return json.loads(requests.post(f'{config.PROTOCOL}://{config.IP}:{config.PORT}/parser/add-to-db/', data=json.dumps(database)).text)['status']

async def a_req_get(session, url):
    try:
        return await session.get(url)
    except ConnectionError:
        print('Error Connection')
        asyncio.sleep(5)
        return await a_req_get(session, url)

@print_args
def req_get(session, url):
    try:
        return session.get(url)
    except ConnectionError | requests.ReadTimeout:
        print('Error Connection')
        time.sleep(5)
        return req_get(session, url)


def compare_url(url):
    base = 'https://www.google.com/'
    return urllib.parse.urljoin(base, url)


@dataclass
class GoogleLink:
    '''Link to google search'''
    link: str


@dataclass
class SomeLink:
    '''Some link'''
    link: str
    session: requests_html.AsyncHTMLSession

    def __init__(self, link: str, session) -> None:
        self.link = link
        self.session = session

    async def get_website_data(self):
        url = self.link
        '''
        Returns the website URL and email address found in HTML
        code got from the URL.

        Parameters:
                url (string): URL to send the request to
        '''
        try:
            if url is not None:
                response = await a_req_get(session=self.session, url=url)

                # Get the url
                url_retrieved = response.url
                content = response.content.decode("utf-8")
                soup = bs4.BeautifulSoup(content, 'html.parser')

                # Get emails recursively
                emails = []
                if url_retrieved is not None:
                    q = ["contact", "about"]
                    emails = await self.find_emails(content, soup, 0, q, [])
                    emails = list(dict.fromkeys(emails))

                return url_retrieved, emails
            else:
                return None, None
        except Exception as ex:
            return None, None

    async def find_emails(self, content, base_soup, i, queries=[], found=[]):
        if i < len(queries) and content is not None:
            # Get the emails with regex
            soup = bs4.BeautifulSoup(content, 'html.parser')
            body = soup.find('body')
            html_text_only = body.get_text()
            match = re.findall(r"""[\w\.-]+@[\w\.-]+\.\w+""", html_text_only)

            # Removes duplicate values
            if match is not None:
                found = found + match

            # Advance to next page
            links = base_soup.find_all('a')
            next_page_url = None
            for link in links:
                curr_url = link.get("href")
                if curr_url is not None and queries[i] in curr_url:
                    next_page_url = curr_url
                    break

            cont = None
            if next_page_url is not None:
                try:
                    response = await a_req_get(session=self.session, url=next_page_url)
                    cont = response.content.decode("utf-8")
                except:
                    cont = None

            return await self.find_emails(cont, base_soup, i + 1, queries, found)
        else:
            return found


@dataclass
class QueryString:
    '''Query to search in google maps'''
    query: str


@dataclass
class PhoneNumber():
    '''USA phone number object'''
    number: str


class Card():
    soup: bs4.BeautifulSoup

    name: str | None
    phone: PhoneNumber | None
    website: SomeLink | None
    email: str | None
    address: str | None
    thematic: str | None

    def __init__(self, soup: bs4.BeautifulSoup, client) -> None:
        self.soup = soup
        self.client = client

    def get_phone(self) -> str | None:
        '''Працює більше 5 років · Irvine, CA, Сполучені Штати · +1 949-885-0063'''
        phone_regex = re.compile(
            r'(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})')
        details = self.soup.find('div', {'class': 'rllt__details'})
        phone_object = details.find(name='div', text=phone_regex)
        if phone_object:
            return phone_regex.findall(phone_object.text)[0]
        else:
            return None

    def get_address(self) -> str | None:
        address_regex = re.compile(
            '\d{1,4} [\w\s]{1,20}(?:street|st|avenue|ave|road|rd|highway|hwy|square|sq|trail|trl|drive|dr|court|ct|parkway|pkwy|circle|cir|boulevard|blvd)\W?(?=\s|$)',
            re.IGNORECASE)
        details = self.soup.find('div', {'class': 'rllt__details'})
        address_object = details.find(name='div', text=address_regex)
        if address_object:
            return address_regex.findall(address_object.text)[0]
        else:
            return None

    def get_thematic(self) -> str | None:
        div = self.soup.find("div", {'role': "heading"})
        try:
            div = div.find_next('div')
            return re.split(' · ', div.text)[1]
        except:
            None

    async def get_email(self) -> str | None:
        link = SomeLink(self.website, self.client)
        _, email = await link.get_website_data()
        if email is not None and len(email) > 0:
            self.email = email[0]
            return self.email
        self.email = None
        return None

    def get_name(self) -> str:
        span = self.soup.find("span", {'class': "OSrXXb"})
        return span.text

    def get_website(self) -> SomeLink:
        div = self.soup.find("a", {"class": "yYlJEf Q7PwXb L48Cpd"})
        try:
            return compare_url(div['href'])
        except TypeError:
            return None

    async def colect_data(self) -> bool:
        self.name = self.get_name()
        self.phone = self.get_phone()
        self.address = self.get_address()
        self.thematic = self.get_thematic()
        self.website = self.get_website()
        if self.website:
            if not is_website_exist_in_db(self.website):
                self.email = await self.get_email()
            else:
                self.email = None
        else:
            self.email = None
        return True


class Page():
    _link: str
    _html: str
    _error_requests: float = 0

    def __init__(self, query: str | Optional[QueryString] = None, link: str | Optional[GoogleLink] = None) -> None:
        if link:
            self._link = link
        else:
            self._link = f"https://www.google.com/search?sa=X&rlz=1C1CHBF_enIN844IN844&sz=0&biw=1036&bih=529&tbs=lrf:!1m4!1u3!2m2!3m1!1e1!1m4!1u2!2m2!2m1!1e1!2m1!1e2!2m1!1e3,lf:1&tbm=lcl&q={query}&rflfq=1&num=10&ved=2ahUKEwinh4ngttHwAhV_7XMBHTlIBVAQjGp6BAgiEGc&cshid=1621278964391616&rlst=f#rlfi=hd:;si:;mv:[[53.4004863,154.607647],[-13.0377864,16.6385598]];tbs:lrf:!1m4!1u3!2m2!3m1!1e1!1m4!1u2!2m2!2m1!1e1!1m4!1u1!2m2!1m1!1e1!1m4!1u1!2m2!1m1!1e2!2m1!1e2!2m1!1e1!2m1!1e3,lf:1"

    def get_html(self) -> int:
        session = requests_html.HTMLSession()
        r = req_get(session=session, url=self._link)
        if r.status_code > 190 and r.status_code < 300:
            self._error_requests = 0
            self._html = r.text
            with open('file.html', 'w+', encoding='utf-8') as f:
                f.write(self._html)
        else:
            self._error_requests += 0.5
            print(f'Прилетел бан, ждём {(60 * self._error_requests) * 60} часов')
            time.sleep((60 * self._error_requests) * 60)
            return self.get_html()
        return r.status_code

    def get_cards(self) -> list[bs4.BeautifulSoup]:
        soup = bs4.BeautifulSoup(self._html, 'html.parser')
        return soup.find_all("div", {"jsname": "GZq3Ke"})

    def get_next_page(self) -> GoogleLink:
        soup = bs4.BeautifulSoup(self._html, 'html.parser')
        b = soup.find("a", {"id": "pnnext"})
        if b is not None:
            return compare_url(b['href'])
        else:
            return None


# p = Page()

async def get_cards_info(cards):
    page = []
    async with httpx.AsyncClient() as client:
        for card in cards:
            c = Card(card, client=client)
            if await c.colect_data():
                page.append(
                    {
                        'name': c.name,
                        'phone': c.phone,
                        'website': c.website,
                        'email': c.email,
                        'address': c.address,
                        'thematic': c.thematic
                    }
                )
    return page


def get_page_info(p: Page):
    p.get_html()
    page = []
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        cards = p.get_cards()
    except Exception:
        cards = []
    page = loop.run_until_complete(get_cards_info(cards))
    return page


def go_to_next_page(link, database):
    p = Page(link=link)
    database.append(get_page_info(p))
    return p.get_next_page()


def unpack_lists(array) -> list:
    """
    Recursive algorithm
    Looks like recursive_flatten_iterator, but with extend/append

    """
    lst = []
    for i in array:
        if isinstance(i, list):
            lst.extend(unpack_lists(i))
        else:
            lst.append(i)
    return lst


def parse_query(query: str):
    database = []
    p = Page(query=query)
    p.get_html()
    database.append(get_page_info(p))
    link = p.get_next_page()
    while link:
        time.sleep(random.randint(15, 35))
        link = go_to_next_page(link, database)
    return database


def clean_database(arr: list) -> list[dict]:
    arr = unpack_lists(arr)
    emails = []
    filtered = []
    for card in arr:
        if card['email']:
            if card['email'] not in emails:
                filtered.append(card)
                emails.append(card['email'])
    return filtered


def add_data_to_dicts_in_list(db: list, name: str, data):
    arr = unpack_lists(db)
    for el in arr:
        el[name] = data
    return arr


def main():
    """ Main task function, create parser"""
    while True:
        query = get_any_query()
        print(query)
        if query:
            query_data = parse_query(query['query'])
            query_data = add_data_to_dicts_in_list(query_data, 'city', query['city'])
            query_data = add_data_to_dicts_in_list(query_data, 'state', query['state'])
            query_data = add_data_to_dicts_in_list(query_data, 'query', query['query'])
            query_data = clean_database(query_data)
            write_to_site_db(database=query_data)
        time.sleep(random.randint(60, 90))
    

