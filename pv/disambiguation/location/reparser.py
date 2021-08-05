import re
import unicodedata
import string

from absl import logging

ignore_city_parts_kwds = {'san', 'new', 'los'}
unknown_strings = {'', 'un', 'unknown', 'null'}


def rule_based_remapping(location):
    splt = location.split('|')
    city = splt[0].strip().replace('-', ' ')
    region = splt[1].strip()
    country = splt[2].strip()

    if city == '' and region != '':
        city = region
        region = ''

    ########
    end_abbrvs = [(' twp', 'township'), (' twp.', 'township'),
                  (' pk', 'park'), (' nsw', 'new south wales'), (' on', 'ontario'),
                  (' hsien', 'county')]

    def end_abbrv(city, abbr, expanded):
        if city.endswith(abbr):
            city = city[:-len(abbr)] + ' ' + expanded
        return city

    for ea in end_abbrvs:
        city = end_abbrv(city, ea[0], ea[1])

    ########
    def start_abbrv(city, abbr, expanded):
        if city.startswith(abbr):
            city = expanded + ' ' +city[len(abbr):]
        return city.strip()

    start_abbrvs = [('e ', 'east'), ('e. ', 'east'),
                    ('n ', 'north'), ('n. ', 'north'),
                    ('w ', 'west'), ('w. ', 'west'),
                    ('s ', 'south'), ('s. ', 'south'),
                    ('ft ', 'fort'), ('ft. ', 'fort'), ('st. ', 'saint '), ('st ', 'saint '),
                    ('mt. ', 'mount'), ('mt ', 'mount'),
                    ('late of', ''), ('d-', ''), ('l-', ''), ('i-', ''),
                    ]
    for sa in start_abbrvs:
        city = start_abbrv(city, sa[0], sa[1])

    ########

    ########
    def fix_region(city, region, fix):
        if fix in city and region is '':
            region = fix.strip()
            city = city.replace(fix, '').strip()
        return city, region

    fix_regions = [' new south wales', ' ontario']
    for fr in fix_regions:
        city, region = fix_region(city, region, fr)
    ########

    ########
    fix_countries = [(' cax', 'ca'), (' nlx', 'nl'), (' gb2', 'gb'), (' atx', 'at'), (' ja', 'jp'),
                     (' twx', 'tw'), (' dt', 'de'), (' ja', 'jp'), (' dex', 'de'), (' frx', 'fr'),
                     (' jpx', 'jp'), (' dex', 'de'), (' ilx', 'il'), (' en', 'gb')]

    def fix_country_code(city, region, country, mistake, fix):
        if city.endswith(mistake) and country == '':
            city = city[0:-len(mistake)].strip()
            country = fix
        if region == mistake.strip() and country == '':
            region = ''
            country = fix
        if country.strip() == mistake.strip():
            country = fix
        if city == mistake.strip() and country == '':
            city = ''
            country = fix
        if city == '' and region:
            city = region
            region = ''
        return city, region, country

    for fcc1,fcc2 in fix_countries:
        city, region, country = fix_country_code(city, region, country, mistake=fcc1, fix=fcc2)
    ########

    location = city + '|' + region + '|' + country

    rules = {
        'alexandria||nz': 'alexandra||nz',
        'alexandria||va': 'alexandria|va|us',
        'coplex|oh|us': 'copley|oh|us',
        'daejeon-shi||kr': 'daejeon||kr',
        'daejeon-si||kr': 'daejeon||kr',
        'daejon-si||kr': 'daejeon||kr',
        'daejun-shi||kr': 'daejeon||kr',
        'hong kong||cn': 'hong kong||cn',
        'hong kong||hk': 'hong kong||cn',
        'hong kong hong kong||cn':  'hong kong||cn',
        'lexington massachusetts|ma|us': 'lexington|ma|us',
        'lexington nova scotia||ca': 'lexington|nova scotia|ca',
        'exington park||md': 'lexington park|md|us',
        'los angles|ca|us': 'los angeles|ca|us',
        'lexington||ky': 'lexington|ky|us',
        'los alto hills|ca|us': 'los altos hills|ca|us',
        '|maharashtra|in':'|maharashtra|in',
         'maharastra||in': '|maharashtra|in',
        'menlo|ca|us': 'menlo park|ca|us',
        'motte servolex frx||la': 'motte servolex|la|fr',
        'moutainview|ca|us': 'moutain view|ca|us',
        'westchester|oh|us': 'west chester|oh|us',
        'wheatridge|co|us': 'wheat ridge|co|us',
        'toronto||ca': 'toronto|ontario|ca',
        'toyota||jp': 'toyota||jp',
        'toyota-shi|aichi|jp':  'toyota||jp',
        'toyota-shi aichi-ken||jp':  'toyota||jp',
        'tao yuan||tw': 'taoyuan||tw',
        'taipei hsien||tw': 'taipei county||tw',
        'taipei city||tw': 'taipei||tw',
        'rosh ha&#8242;ayin||il':  'rosh haayin||il',
        'petach tikva||il': 'petah tikva||il',
        'petach tikve||il': 'petah tikva||il',
        'oakbrook|il|us': 'oak brook|il|us',
        """notre dame i'{circumflex over (l)}le perrot|de|ca""": "notre dame de l'{circumflex over (l)}le perrot||ca",
        "north|bay|ca": "north bay|ontario|ca",
        "north vancouver british columbia||ca": "north vancouver|british columbia|ca",
        "north vancouver||ca": 'north vancouver|british columbia|ca',
        'neihu dist. taipei||tw': 'neihu locality taipei||tw',
        'saint gilles lex bruxelles||us': 'saint gilles lex brussels||be',
        'saint paul varces|de|fr': 'saint paul de varces||fr',
        'roundrock|tx|us': 'round rock|tx|us',
        'on|bolton|ca': 'bolton|ontario|ca',
        'offenbach||de': 'offenbach am main||de',
        'minneapolis|mi|us': 'minneapolis|mn|us'

    }
    if location in rules:
        location = rules[location]
        splt = location.split('|')
        city = splt[0]
        region = splt[1]
        country = splt[2]

    rules_city = {
        'lexignton': 'lexington',
        'lexiington': 'lexington',
        'lexingotn': 'lexington',
        'lexingron': 'lexington',
        'lexingtion': 'lexington',
        'lexingtong': 'lexington',
        'lexingtown': 'lexington',
        'lexintgon': 'lexington',
        'lexinggton': 'lexington',
        'lexinton': 'lexington',
        'lexngton': 'lexington',
        'lextington': 'lexington',
        'lexnigton': 'lexington',
        'lexongton': 'lexongton',
        'los angelex': 'los angeles',
        'middlexex': 'middlesex',
        'middlexsex': 'middlesex',
        'munchen': 'munich',
        'schwabisch gmund': 'schwaebisch gmuend',
        'taoyuan': 'tao yuan',
        'vaerlose': 'vaerloese',
        'wheatridge': 'wheat ridge'
    }
    if city in rules_city:
        city = rules_city[city]

    city = city.lstrip(string.digits)
    city = city.rstrip(string.digits)
    region = region.lstrip(string.digits)
    region = region.rstrip(string.digits)

    return [city,region, country]



def city_parts(city_name):
    city_name = normalize_name(city_name, lower=False)
    splt = re.split("[ ,\\|]", city_name)
    res = []
    for s in splt:
        if len(s) != 2:
            res.append(s.lower())
        elif len(s) == 2 and not re.match('[A-Z][A-Z]', s) and not s in unknown_strings:
            res.append(s.lower())
    return res


def normalize_name(name, lower=True):
    if lower:
        return unicodedata.normalize('NFD', name.replace('\"', '')).encode('ascii', 'ignore').decode(
            "utf-8").lower().replace('\n', '').replace('-', '')
    else:
        return unicodedata.normalize('NFD', name.replace('\"', '')).encode('ascii', 'ignore').decode("utf-8").replace(
            '\n', '').replace('-', '')


class LocationDatabase:
    def __init__(self):
        self.abbr2country = dict()
        self.country2abbr = dict()
        self.abbr2state = dict()
        self.state2abbr = dict()
        self.regions = set()
        self.cityNameCountryName2Records = dict()
        self.loaded = False

    def is_country(self, string):
        string = normalize_name(string)
        return string in self.abbr2country or string in self.country2abbr

    def is_state_or_region(self, string):
        string = normalize_name(string)
        return string in self.abbr2state or string in self.state2abbr or string in self.regions

    def reparse(self, string):
        LOCATIONS.load()
        string = normalize_name(string)
        splt = [x.strip() for x in re.split("[ ,\\|]", string) if x]
        country = ''
        state = ''
        city = ''
        i = len(splt) - 1
        while i >= 0:
            s = splt[i]
            if s not in unknown_strings:
                # no country
                if country == '' and self.is_country(s):
                    country = s
                elif state == '' and self.is_state_or_region(s):
                    state = s
                elif country == '' and len(s) == 2:
                    country = s
                elif len(country) > 0 and country == '' and len(s) == 2:
                    state = s
                else:
                    city = s + ' ' + city
            i -= 1
        return rule_based_remapping(city.strip() +'|' + state + '|' + country)

    def __contains__(self, item):
        return self.contains(item._canonical_city, item._canonical_state, item._canonical_country)

    def contains(self, city, state, country):
        """

        :param city:
        :param state:
        :param country:
        :return:
        """
        city = normalize_name(city)
        state = normalize_name(state)
        country = normalize_name(country)
        if country in self.country2abbr:
            country = self.country2abbr[country]
        if state in self.state2abbr:
            state = self.state2abbr[state]
        if (city, country) in self.cityNameCountryName2Records:
            recs = self.cityNameCountryName2Records[(city, country)]
            for r in recs:
                if r[1] == state:
                    return True
        return False

    def load(self):
        if not self.loaded:
            logging.info('Loading location tables!')
            self.load_countries()
            self.load_states()
            self.load_cities()
            self.loaded = True

    def load_countries(self, filename="resources/location/iso3166.csv"):
        """

        
        :param filename: 
        :return: 
        """

        with open(filename, 'r') as fin:
            for line in fin:
                splt = [normalize_name(x) for x in re.split(""",(?=[^"]*([^"]*"[^"]*")*[^"]*$)""", line.strip()) if x]
                self.country2abbr[splt[0]] = splt[1]
                self.abbr2country[splt[1]] = splt[0]

    def load_states(self, filename="resources/location/state_abbreviations.csv"):
        """
        :param filename:
        :return:
        """
        with open(filename, 'r') as fin:
            for line in fin:
                splt = [normalize_name(x) for x in re.split(""",(?=[^"]*([^"]*"[^"]*")*[^"]*$)""", line.strip()) if x]
                self.state2abbr[splt[0]] = splt[1]
                self.abbr2state[splt[1]] = splt[0]

    def load_cities(self, filename="resources/location/all_cities.csv"):
        with open(filename, 'r') as fin:
            for line in fin:
                splt = [normalize_name(x) for x in re.split(""",(?=[^"]*([^"]*"[^"]*")*[^"]*$)""", line.strip()) if x]
                city = splt[0]
                state = splt[1]
                country = splt[2]
                self.regions.add(state)
                if state in self.state2abbr:
                    state = self.state2abbr[state]
                self.cityNameCountryName2Records[(city, country)] = (city, state, country)


LOCATIONS = LocationDatabase()