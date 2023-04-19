from common.functions import log
from datetime import datetime
import textstat

class Analyzer(object):

    AVG_METRICS_DEF = ['avg_word_length', 'avg_sentence_length', 'noun_ratio', 'verb_ratio', 'adj_ratio', 'lexical_density', 'gunning_fog']

    def __init__(self, txt, meta, nlp, index):
        textstat.set_lang('pl')
        self.txt = txt
        self.meta = meta
        self.nlp = nlp
        self.nlp.max_length = len(txt) + 100
        self.index = index

    def go(self):

        t1 = datetime.now() 

        name = self.meta.get("name", "")
        if name == "":
            name = self.meta.get("url", "")[:80]

        new_meta = self.meta

        doc = self.nlp(self.txt)
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
        

        for token in doc:
            if not token.is_punct and not token.is_stop and not token.is_space:
                if token.is_oov and not token.pos_ == "SYM":
                    oovs +=1
                if token.pos_ == "NOUN":
                    nouns += 1
                    uniq_words.add(token.lemma_)
                elif token.pos_ == "VERB":
                    verbs += 1
                    uniq_words.add(token.lemma_)
                elif token.pos_ == "ADJ":
                    adjectives += 1
                    uniq_words.add(token.lemma_)
                elif token.pos_ == "ADV":
                    adverbs += 1
                    uniq_words.add(token.lemma_)
                avg_word_length += len(token.text)
            if token.pos_ == "SYM":
                symbols += 1
            if token.is_stop:
                stopwords += 1
            if token.is_punct:
                punctuations += 1
            elif not token.is_space and not token.pos_ == "SYM":
                words += 1
                

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


        new_meta["length"] = len(self.txt)
        new_meta["sentences"] = sentences
        new_meta["avg_sentence_length"] = avg_sentence_length
        new_meta["words"] = words
        new_meta["verbs"] = verbs
        new_meta["nouns"] = nouns
        new_meta["adverbs"] = adverbs
        new_meta["adjectives"] = adjectives
        new_meta["punctuations"] = punctuations
        new_meta["symbols"] = symbols
        new_meta["stopwords"] = stopwords
        new_meta["oovs"] = oovs
        new_meta["avg_word_length"] = avg_word_length
        new_meta["noun_ratio"] = noun_ratio
        new_meta["verb_ratio"] = verb_ratio
        new_meta["adj_ratio"] = adj_ratio
        new_meta["lexical_density"] = lexical_density
        new_meta["gunning_fog"] = gunning_fog


        d = datetime.now() - t1
        elapsed = round(d.microseconds / 1000)
        log("Processing document (" + str(elapsed) + " ms): " + str(self.index+1) + " " + name, "INFO")

        return new_meta

