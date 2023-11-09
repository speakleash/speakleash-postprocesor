import re
import warnings

import numpy
import textstat
import fasttext
from ftlangdetect import detect
from postprocessor.utils import log
from postprocessor.quality import sanity_check, get_doc_quality

fasttext.FastText.eprint = lambda x: None   # Suppress warnings from 'fasttext' library
warnings.filterwarnings('ignore')           # Disable warnings from 'textstat' library


class Analyzer(object):

    AVG_METRICS_DEF = ['avg_word_length', 'avg_sentence_length', 'noun_ratio', 'verb_ratio', 'adj_ratio', 'lexical_density', 'gunning_fog']
    MAX_TEXT_PART = 1024 * 1024     # Max text chunk part 
    CAMEL_CASE_PATTERN = re.compile(r"\b[a-ząęćłńóśżź]+[A-ZĄĘĆŁŃÓŚŻŹ]+[a-ząęćłńóśżź]+[a-ząęćłńóśżźA-ZĄĘĆŁŃÓŚŻŹ]*\b")
    OBSOLETE_KEYS = ['length']      # A list of obsolete keys to remove from new meta

    def __init__(self, txt, meta, nlp, index, metrics=True, quality_metrics=True, lang_detect = True):
        textstat.set_lang('pl')
        self.txt = txt
        self.meta = meta
        self.nlp = nlp
        self.nlp.max_length = len(txt) + 100
        self.index = index
        self.metrics = metrics
        self.quality_metrics = quality_metrics
        self.lang_detect = lang_detect

    def _split_text(self):
        start = 0
        end = self.MAX_TEXT_PART
        parts = []

        while start < len(self.txt):
            if end >= len(self.txt):
                end = len(self.txt)
            
            # Check if the split point is not a space
            if not self.txt[end-1].isspace() and end < len(self.txt):
                while not self.txt[end-1].isspace() and end < len(self.txt):
                    end += 1
            
            parts.append(self.txt[start:end])
            start = end
            end += self.MAX_TEXT_PART

        return parts

    def _count_metrics(self):
        new_meta = self.meta
        words = 0
        verbs = 0
        nouns = 0
        punctuations = 0
        symbols = 0
        stopwords = 0
        oovs = 0
        adjectives = 0
        adverbs = 0
        avg_word_length = 0
        avg_sentence_length = 0
        sentences = 0
        noun_ratio = 0
        verb_ratio = 0
        adj_ratio = 0
        lexical_density = 0
        gunning_fog = 0
        uniq_words = set()
        camel_case = 0
        pos_x = 0
        pos_num = 0
        capitalized_words = 0

        text_parts = self._split_text()

        for part in text_parts:
        
            doc = self.nlp(part)

            for token in doc:
                if not token.is_punct and not token.is_space:
                    if token.is_oov and not token.pos_ == "SYM":
                        oovs += 1
                    
                    # Update stats based on token's part-of-speech
                    if token.pos_ == "X":
                        pos_x += 1
                    elif token.pos_ =="NUM":
                        pos_num += 1
                    elif token.pos_ == "NOUN":
                        nouns += 1
                    elif token.pos_ == "VERB":
                        verbs += 1
                    elif token.pos_ == "ADJ":
                        adjectives += 1
                    elif token.pos_ == "ADV":
                        adverbs += 1

                    # Add token's lemma to unique words
                    uniq_words.add(token.lemma_)

                    avg_word_length += len(token.text)

                # Update stats for symbols, stopwords, punctuations, and words
                if token.pos_ == "SYM":
                    symbols += 1
                if token.is_stop:
                    stopwords += 1
                if token.is_punct:
                    punctuations += 1
                elif not token.is_space and not token.pos_ == "SYM":
                    words += 1
                    if re.match(self.CAMEL_CASE_PATTERN, token.text):
                        camel_case += 1
                    if token.text.isupper():
                        capitalized_words +=1         
                                        

            for sentence in doc.sents:
                avg_sentence_length += len(sentence)
                sentences += 1

        if sentences > 0:
            avg_sentence_length = avg_sentence_length / sentences
        else:
            avg_sentence_length = 0

        if words > 0:
            avg_word_length = avg_word_length / words
        else:
            avg_word_length = 0

        if words > 0:
            noun_ratio = nouns / words
        else:
            noun_ratio = 0

        if words > 0:
            verb_ratio = verbs / words
        else:
            verb_ratio = 0   

        if words > 0:
            adj_ratio = adjectives / words
        else:
            adj_ratio = 0

        if words > 0:
            lexical_density = len(uniq_words) / words
        else:
            lexical_density = 0

        if words > 0:
            gunning_fog = textstat.gunning_fog(self.txt)

        new_meta["characters"] = len(self.txt)
        new_meta["sentences"] = sentences
        new_meta["avg_sentence_length"] = round(avg_sentence_length,4)
        new_meta["words"] = words
        new_meta["verbs"] = verbs
        new_meta["nouns"] = nouns
        new_meta["adverbs"] = adverbs
        new_meta["adjectives"] = adjectives
        new_meta["punctuations"] = punctuations
        new_meta["symbols"] = symbols
        new_meta["stopwords"] = stopwords
        new_meta["oovs"] = oovs
        new_meta["pos_x"] = pos_x
        new_meta["pos_num"] = pos_num
        new_meta["avg_word_length"] = round(avg_word_length,4)
        new_meta["noun_ratio"] = round(noun_ratio,4)
        new_meta["verb_ratio"] = round(verb_ratio,4)
        new_meta["adj_ratio"] = round(adj_ratio,4)
        new_meta["lexical_density"] = round(lexical_density,4)
        new_meta["gunning_fog"] = round(gunning_fog,4)
        new_meta["camel_case"] = camel_case
        new_meta["capitalized_words"] = capitalized_words

        # Remove obsolete keys from new_meta 
        for key in self.OBSOLETE_KEYS:
            new_meta.pop(key,None)

        return new_meta

    def go(self):
        new_meta = self.meta

        if self.metrics:
            new_meta = self._count_metrics()

        if self.quality_metrics:
            if sanity_check(new_meta):
                get_doc_quality(new_meta)
            else:
                name = self.meta.get("name", self.meta.get("url", ""))
                log("Required metrics for quality check not found in meta: " + name, "WARNING")

        if self.lang_detect:
            new_meta["language"] = detect(self.txt.replace('\n',' '))
            new_meta["language"]["score"] = numpy.round(new_meta["language"]["score"], 3)

        return new_meta
