import os
import csv
import codecs
import re
import statistics
from bs4 import BeautifulSoup
from nltk.tokenize import sent_tokenize
from nltk import word_tokenize
import logging


_logger = logging.getLogger(__name__)


class CorpusReader:
    """A common base class for other corpus readers"""

    def __init__(self, root=None):
        self.root = root

    def ids(self, count=None, categories=None):
        raise NotImplementedError

    def categories(self, count=None):
        raise NotImplementedError

    def stat(self, ids=None, categories=None):
        raise NotImplementedError

    def abspath(self, ids):
        raise NotImplementedError

    def docs(self, ids=None, categories=None):
        raise NotImplementedError

    def sizes(self, ids=None, categories=None):
        raise NotImplementedError


class FileCorpusReader(CorpusReader):

    def __init__(self, root=None):
        super().__init__(root)

    def categories(self, count=None):
        """
        Returns a list of categories
        :param count: maximum list size
        :return: list of strings or empty list
        """
        counter = 0
        cats = []
        for root, dirs, files in os.walk(self.root):
            if root == self.root:
                continue
            if count is not None and counter == count:
                break
            cats.append(os.path.basename(root))
            counter += 1

        return cats

    def category(self, doc_id):
        """
        Returns document's category
        :param doc_id:
        :return: string: document's category
        """
        cat, _ = os.path.split(doc_id)
        return cat

    def ids(self, count=None, categories=None):
        counter = 0
        ids = []
        for root, dirs, files in os.walk(self.root):
            if root == self.root:
                continue

            if count is not None and counter == count:
                break

            cat_str = os.path.basename(root)
            if categories is not None and cat_str not in categories:
                continue

            # Hidden folder
            if cat_str:
                if cat_str[0] == '.':
                    continue

            for file_name in files:
                # Hidden file
                if file_name[0] == '.':
                    continue
                if count is not None and counter == count:
                    return ids

                ids.append(os.path.join(cat_str, file_name))
                counter += 1

        return ids

    def resolve(self, ids, categories=None):
        """
        Returns a list of ids or categories depending on what is passed
        to each internal corpus reader function.
        """
        if ids is not None and categories is not None:
            raise ValueError("Specify ids or categories, not both")
        if categories is not None:
            return self.ids(categories=categories)
        if ids is not None:
            if isinstance(ids, list):
                return ids
            else:
                return [ids]

        return self.ids()

    def id_to_abspath(self, doc_id):
        """
        Returns document's absolute path in file system
        :param doc_id: document id, string like 'category/filename'
        :return: absolute path
        """
        proc_folder = doc_id.split(os.path.sep)[0]
        file, ext = os.path.splitext(os.path.basename(doc_id))
        src_folder = os.path.join(self.root, proc_folder)

        abs_path = os.path.join(src_folder, file) + ext

        return abs_path if os.path.isfile(abs_path) else ''

    def abspath(self, ids):
        """
        For a given document list returns document's absolute path in file system
        :param ids: list of doc ids
        :return: yield absolute path to doc file
        """
        for fid in ids:
            cat_name, file_name = os.path.split(fid)
            abs_path = os.path.join(self.root, cat_name, file_name)
            if os.path.isfile(abs_path):
                yield abs_path

    def docs(self, ids=None, categories=None):
        """
        Returns just absolute file path of doc
        :param ids:
        :param categories:
        :return:
        """
        # Resolve the ids and the categories
        fileids = self.resolve(ids, categories)

        for path in self.abspath(fileids):
            yield path

    def stat(self, ids=None, categories=None):
        """
        Calc corpus statistics
        :param ids: only for documents in ids list
        :param categories: only for documents in categories list
        :return: dictionary with metrics
        """

        fileids = self.resolve(ids, categories)

        # Categories count
        if categories is None:
            cat = []
            for fid in fileids:
                cat.append(self.category(fid))
            cat_count = len(set(cat))
        else:
            cat_count = len(set(categories))

        # Extensions
        ext = {}
        for path in self.abspath(fileids):
            file_name, file_ext = os.path.splitext(path)
            file_ext = file_ext.lower()[1:]
            ext[file_ext] = ext[file_ext] + 1 if file_ext in ext else 1

        file_sizes = list([x for x in self.sizes(fileids)])

        return {
            'files': len(fileids),
            'categories': cat_count,
            'ext': ext,
            'file_size': sum(file_sizes),
            'max_file_size': max(file_sizes),
            'min_file_size': min(file_sizes),
            'mean_file_size': int(statistics.mean(file_sizes))
        }

    def sizes(self, ids=None, categories=None):
        """
        Returns list of tuples (the document id, document file size on disk)
        :param ids: text ids
        :param categories: text categories
        :return: file sizes
        """
        fileids = self.resolve(ids, categories)

        for path in self.abspath(fileids):
            yield os.path.getsize(path)

    def files_to_csv(self, path=None):
        """
        Dump list of files in csv file.
        csv columns: category, file, ext, size, abs full path
        :param path: write file to specific path, rather than in corpus root
        :return:
        """
        corpus_folder = self.root
        csv_file_name = os.path.join(corpus_folder, 'files.csv') if path is None else path

        with open(csv_file_name, mode='w') as file:
            csv_fields = ['category', 'file', 'ext', 'size', 'path']
            writer = csv.DictWriter(file, fieldnames=csv_fields, delimiter=',')

            writer.writeheader()

            for root, dirs, files in os.walk(corpus_folder):

                if root == corpus_folder:
                    continue

                base_folder = os.path.split(root)[1]

                # Hidden folder
                if base_folder:
                    if base_folder[0] == '.':
                        continue

                for f in files:

                    file_path = os.path.join(root, f)
                    file_size = os.path.getsize(file_path)

                    file_name, file_ext = os.path.splitext(f)

                    # Hidden files
                    if file_name[0] == '.':
                        continue

                    file_ext = file_ext[1:].lower().strip()

                    writer.writerow({
                        'category': base_folder,
                        'file': f,
                        'ext': file_ext,
                        'size': file_size,
                        'path': file_path
                    })

            file.close()


class TxtCorpusReader(FileCorpusReader):

    def __init__(self, root=None, stemmer=None, clean_text=False, language='english'):
        super(TxtCorpusReader, self).__init__(root=root)
        self.stop_words = ['.', ',', '”', '„', ',', '-', '(', ')', ':', '«', '»', ';', '–', '{', '}', '™']
        self.stemmer = stemmer
        self.clean_text = clean_text
        self.language = language

    def docs(self, ids=None, categories=None):
        """
        Returns content of an text document
        """
        # Resolve the fileids and the categories
        fileids = self.resolve(ids, categories)

        # Create a generator, loading one document into memory at a time
        for path in self.abspath(fileids):
            if os.path.isfile(path):
                with codecs.open(path, 'r') as f:
                    yield f.read()

    def paras(self, ids=None, categories=None):

        clean_text = self.clean_text

        for txt in self.docs(ids, categories):
            for s in txt.splitlines():
                raw_text = s.strip()
                text = clean_paragraph(raw_text) if clean_text else raw_text
                if text:
                    yield text

    def sents(self, ids=None, categories=None):
        """
        Extract sentences from the paragraphs
        # Todo: train sent_tokenizer
        """
        clean_text = self.clean_text
        language = self.language

        for paragraph in self.paras(ids, categories):
            for raw_sentence in sent_tokenize(paragraph, language):
                for sentence in raw_sentence.strip().split('; '):
                    text = clean_sentence(sentence) if clean_text else sentence
                    if text:
                        yield text

    def words(self, ids=None, categories=None):
        """
        Extract words from the sentences
        """
        for sentence in self.sents(ids, categories):
            for word in self.text_to_words(sentence):
                yield word

    def text_to_words(self, text):

        clean_text = self.clean_text
        language = self.language

        for token in word_tokenize(text, language):
            if clean_text:
                word = clean_word(token, self.stop_words)
                if self.stemmer is not None:
                    word = self.stemmer.stem_word(word)
            else:
                word = token

            if word:
                yield word

    def stat(self, ids=None, categories=None, count_words=False):
        """
        Calc corpus statistics
        :param count_words:
        :param ids: only for documents in ids list
        :param categories: only for documents in categories list
        :return: dictionary with metrics
        """
        stat_dict = super(TxtCorpusReader, self).stat(ids, categories)

        stat_dict.update({
            'paras': len(list(self.paras(ids, categories))),
            'sents': len(list(self.sents(ids, categories))),
            'words': len(list(self.words(ids, categories))) if count_words else 0,
        })

        return stat_dict

    def sents_to_csv(self, path=None, ids=None):
        """
        Dump list of sentences to csv file.
        """
        corpus_folder = self.root
        csv_file_name = os.path.join(corpus_folder, 'sents.csv') if path is None else path

        with open(csv_file_name, mode='w') as file:
            csv_fields = ['sent', 'id', 'doc', 'category']
            writer = csv.DictWriter(file, fieldnames=csv_fields, delimiter=',')

            writer.writeheader()

            for doc_id in ids:
                sent_id = 0
                for sent in self.sents(ids=[doc_id]):
                    writer.writerow({
                            'sent': sent,
                            'id': sent_id,
                            'doc': doc_id,
                            'category': self.category(doc_id),
                        })
                    sent_id += 1

            file.close()

    def paras_to_csv(self, path=None, ids=None):
        """
        Dump list of paragraphs to csv file.
        """
        corpus_folder = self.root
        csv_file_name = os.path.join(corpus_folder, 'paras.csv') if path is None else path

        with open(csv_file_name, mode='w') as file:
            csv_fields = ['para', 'id', 'doc', 'category']
            writer = csv.DictWriter(file, fieldnames=csv_fields, delimiter=',')

            writer.writeheader()

            for doc_id in ids:
                para_id = 0
                for para in self.paras(ids=[doc_id]):
                    writer.writerow({
                            'para': para,
                            'id': para_id,
                            'doc': doc_id,
                            'category': self.category(doc_id),
                        })
                    para_id += 1

            file.close()


class HTMLCorpusReader(TxtCorpusReader):

    def __init__(self, root=None, stemmer=None, clean_text=False, language='english'):
        super(HTMLCorpusReader, self).__init__(root=root, stemmer=stemmer, clean_text=clean_text, language=language)

    def paras(self, ids=None, categories=None):
        """
        Uses BeautifulSoup to parse the paragraphs from the HTML.
        """
        # Tags to extract as paragraphs from the HTML text
        tags = [
            'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'h7', 'p', 'li', 'dd', 'dt'
        ]

        clean_text = self.clean_text

        for html in self.docs(ids, categories):
            soup = BeautifulSoup(html, 'html.parser')
            for element in soup.find_all(tags):
                raw_text = element.text
                text = clean_paragraph(raw_text) if clean_text else raw_text
                if text:
                    yield text
            soup.decompose()


def clean_paragraph(text):
    clean_text = text.strip()
    # Delete quotes and etc
    clean_text = re.sub(r"""['’"`�]""", '', clean_text)
    clean_text = re.sub(r"""([0-9])([\u0400-\u04FF]|[A-z])""", r"\1 \2", clean_text)
    clean_text = re.sub(r"""([\u0400-\u04FF]|[A-z])([0-9])""", r"\1 \2", clean_text)
    # multiply ;;;; between alphanumerics
    clean_text = re.sub(r'(\w+|\d+);+(\w+|\d+)', r'\1 \2', clean_text)
    # some strange artifacts
    clean_text = re.sub(r'\s+№\s+', ' №', clean_text)
    clean_text = re.sub('…+', '…', clean_text)
    # underscores
    clean_text = re.sub('_+', '', clean_text)
    # multi points
    clean_text = re.sub('\.+', '.', clean_text)
    # slash
    clean_text = re.sub('/', ' ', clean_text)
    clean_text = re.sub('\\\\', ' ', clean_text)
    # id=3303
    clean_text = re.sub('\s*=\s*', r' = ', clean_text)
    # All characters are non alfa
    if re.match('^[0-9  .,-:+*_;\\/]+$', clean_text):
        clean_text = ''
    # Too small for paragraph
    if len(clean_text) <= 3:
        clean_text = ''

    return clean_text


def clean_sentence(text):
    clean_text = text.strip()
    # All characters are non alfa
    if re.match("^[0-9  .,-:+*_;\\/]+$", clean_text):
        clean_text = ''

    return clean_text


def clean_word(text, stop_words=None):
    clean_text = text.strip()
    clean_text = clean_text.lower()
    if stop_words and clean_text in stop_words:
        clean_text = ''
    return clean_text
