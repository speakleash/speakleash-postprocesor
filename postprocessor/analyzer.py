from common.functions import log
from datetime import datetime
import textstat

class Analyzer(object):

    AVG_METRICS_DEF = ['avg_word_length', 'avg_sentence_length', 'noun_ratio', 'verb_ratio', 'adj_ratio', 'lexical_density', 'gunning_fog']
    MAX_TEXT_PART = 1024 * 1024 # Max text chunk part 

    def __init__(self, txt, meta, nlp, index):
        textstat.set_lang('pl')
        self.txt = txt
        self.meta = meta
        self.nlp = nlp
        self.nlp.max_length = len(txt) + 100
        self.index = index

    def _split_text(self):
        parts = []
        start = 0
        end = self.MAX_TEXT_PART

        while start < len(self.txt):
            if end >= len(self.txt):
                end = len(self.txt)
            
            # Check if the split point is not a space
            if not self.txt[end-1].isspace() and end < len(self.txt):
                while not self.txt[end].isspace() and end < len(self.txt):
                    end += 1
            
            parts.append(self.txt[start:end])
            start = end
            end += self.MAX_TEXT_PART

        return parts

    def go(self):

        t1 = datetime.now() 

        name = self.meta.get("name", "")
        if name == "":
            name = self.meta.get("url", "")[:80]

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

        text_parts = self._split_text()

        for part in text_parts:
        
            doc = self.nlp(part)

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
        new_meta["avg_word_length"] = round(avg_word_length,4)
        new_meta["noun_ratio"] = round(noun_ratio,4)
        new_meta["verb_ratio"] = round(verb_ratio,4)
        new_meta["adj_ratio"] = round(adj_ratio,4)
        new_meta["lexical_density"] = round(lexical_density,4)
        new_meta["gunning_fog"] = gunning_fog


        d = datetime.now() - t1
        elapsed = d.total_seconds()
        log("Processing document (" + str(elapsed) + " s): " + str(self.index+1) + " " + name, "INFO")

        return new_meta

