import re
import unicodedata

from absl import logging

ignore_city_parts_kwds = {'san', 'new', 'los'}
unknown_strings = {'', 'un', 'unknown', 'null'}


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
            "utf-8").lower().replace('\n', '')
    else:
        return unicodedata.normalize('NFD', name.replace('\"', '')).encode('ascii', 'ignore').decode("utf-8").replace(
            '\n', '')


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
        return city.strip(), state, country

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

if __name__ == '__main__':
    ld = LocationDatabase()
    ld.load()
    print(ld.reparse("scottsdale az||us"))
