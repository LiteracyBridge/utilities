import re


# Create an object with the Javascript like property of dot access to dynamic members.
# foo = JsObject()
# foo.someprop = 'a string'
#
# Taken from http://www.adequatelygood.com/JavaScript-Style-Objects-in-Python.html
#
class JsObject(object):
    def __init__(self, *args, **kwargs):
        for arg in args:
            self.__dict__.update(arg)

        self.__dict__.update(kwargs)

    def __getitem__(self, name):
        return self.__dict__.get(name, None)

    def __setitem__(self, name, val):
        return self.__dict__.__setitem__(name, val)

    def __delitem__(self, name):
        if name in self.__dict__:
            del self.__dict__[name]

    def __getattr__(self, name):
        return self.__getitem__(name)

    def __setattr__(self, name, val):
        return self.__setitem__(name, val)

    def __delattr__(self, name):
        return self.__delitem__(name)

    def __iter__(self):
        return self.__dict__.__iter__()

    def __repr__(self):
        return self.__dict__.__repr__()

    def __str__(self):
        return self.__dict__.__str__()


# Class to parse lists of keywords. Initialize with one or more strings of keywords, each string
# may contain multiple keywords separated by ',', ';', or ':'. A dictionary of synonyms may be
# provided {synonym:keyword, ...}.
# To use, call parse with one or more of strings of words; each string may contain multiple
# words as with keywords. The return is a tuple of sets: (matches, unrecognized, ambiguous). It
# is an error if [1] or [2] are non-empty. The set of matches is the full keywords, with synonyms
# resolved.
class KeyWords:
    def __init__(self, *args, allow_abbrevs=True, synonyms=None):
        if synonyms is None:
            synonyms = {}
        self._allow_abbrevs = allow_abbrevs
        self._keywords = set()
        self._synonyms = {}
        self._delims = '[,;: \t]'

        # Get the list of given keywords
        for keywords in args:
            for keyword in re.split(self._delims, keywords):
                keyword = keyword.strip()
                if not keyword:
                    continue
                if keyword in self._keywords:
                    raise ValueError('Keyword already defined: "{}"'.format(keyword))
                self._keywords.add(keyword)
        # Add the list of synonyms
        for synonym, keyword in synonyms.items():
            if synonym in self._keywords:
                raise ValueError('Can\'t define "{}" as synonym for "{}"; already a keyword.'.format(synonym, keyword))
            if synonym in self._synonyms:
                raise ValueError('Can\'t define "{}" as synonym for "{}"; already a synonym for "{}".'
                                 .format(synonym, keyword, self._synonyms[synonym]))
            self._synonyms[synonym] = keyword

    def __repr__(self):
        words = []
        for kw in self._keywords:
            word = kw
            synonyms = [k for k, v in self._synonyms.items() if v == kw]
            if len(synonyms):
                word += '(' + ', '.join(synonyms) + ')'
            words.append(word)
        return '[' + ', '.join(words) + ']'

    # Given a word, determine if it is a keyword or synonym, or an unambiguous abbreviation of a keyword or synonym.
    def _lookup(self, word):
        # Whole keyword or synonym?
        if word in self._keywords:
            return word
        if word in self._synonyms:
            return self._synonyms[word]
        if not self._allow_abbrevs:
            return None
        # See if there are keywords and/or synonyms that this is a prefix to.
        prefix_to = set()
        for kw in self._keywords:
            if kw.startswith(word):
                prefix_to.add(kw)
        for syn, kw in self._synonyms.items():
            if syn.startswith(word):
                prefix_to.add(kw)
        # If this word is a prefix to exactly one keyword or synonym, accept it as an abbreviation.
        if len(prefix_to) == 1:
            return next(iter(prefix_to))
        # Else if not a prefix to any, there's no match.
        elif len(prefix_to) == 0:
            return None
        # Matches multiples; return the list. That's an error.
        return prefix_to

    def parse(self, *args):
        result = (set(), set(), set())
        for word_string in args:
            for word in re.split(self._delims, word_string):
                word = word.strip()
                if not word:
                    continue
                kws = self._lookup(word)
                if type(kws) == str:  # Unambiguous match
                    result[0].add(kws)
                elif kws is None:  # No match
                    result[1].add(word)
                else:  # Multiple match; ambiguous
                    result[2].add(word)
        return result

#
